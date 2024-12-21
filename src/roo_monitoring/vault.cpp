
#include "vault.h"

#include <map>

#include "common.h"
#include "log.h"
#include "resolution.h"
#include "roo_io/data/multipass_input_stream_reader.h"
#include "roo_logging.h"
#include "roo_monitoring.h"

#ifndef MLOG_roo_monitoring_vault_reader
#define MLOG_roo_monitoring_vault_reader 0
#endif

namespace roo_monitoring {

namespace {

bool read_header(roo_io::MultipassInputStreamReader& is) {
  uint8_t major = is.readU8();
  uint8_t minor = is.readU8();
  if (!is.ok()) {
    LOG(ERROR) << "Failed to read vault file header: "
               << roo_io::StatusAsString(is.status());
    return false;
  }
  if (major != 1 || minor != 1) {
    LOG(ERROR) << "Invalid content of vault file header: " << major << ", "
               << minor;
    return false;
  }
  return true;
}

roo_io::Status read_data(roo_io::MultipassInputStreamReader& is,
                         std::vector<Sample>* data, bool ignore_fill) {
  data->clear();
  uint64_t sample_count = roo_io::ReadVarU64(is);
  if (!is.ok()) {
    if (is.status() != roo_io::kEndOfStream) {
      LOG(ERROR) << "Failed to read data from the vault file: "
                 << roo_io::StatusAsString(is.status());
    }
    return is.status();
  }
  for (uint64_t i = 0; i < sample_count; ++i) {
    uint64_t stream_id = is.readVarU64();
    uint16_t avg = is.readBeU16();
    uint16_t min = is.readBeU16();
    uint16_t max = is.readBeU16();
    uint16_t fill = is.readBeU16();
    if (ignore_fill) {
      fill = 0x2000;
    }
    if (!is.ok()) {
      LOG(ERROR) << "Failed to read a sample from the vault file: "
                 << roo_io::StatusAsString(is.status());
      return is.status();
    }
    data->emplace_back(stream_id, avg, min, max, fill);
  }
  return roo_io::kOk;
}

}  // namespace

VaultFileReader::VaultFileReader(const Collection* collection)
    : collection_(collection),
      ref_(),
      fs_(),
      reader_(),
      index_(0),
      position_(0) {}

bool VaultFileReader::open(const VaultFileRef& vault_ref, int index,
                           int64_t offset) {
  String path;
  ref_ = vault_ref;
  collection_->getVaultFilePath(vault_ref, &path);
  fs_ = collection_->fs().mount();
  if (!fs_.ok()) {
    return false;
  }
  reader_.reset(fs_.fopen(path.c_str()));
  index_ = index;
  position_ = 0;
  if (!reader_.isOpen()) {
    if (reader_.status() == roo_io::kNotFound) {
      MLOG(roo_monitoring_vault_reader)
          << "Vault file " << path.c_str()
          << " doesn't exist; treating as-if empty";
    } else {
      LOG(ERROR) << "Failed to open vault file for read: " << path.c_str()
                 << ": " << roo_io::StatusAsString(reader_.status());
    }
    return false;
  }
  if (offset == 0) {
    if (!read_header(reader_)) {
      reader_.close();
      return false;
    }
    position_ = reader_.position();
  } else if (offset < 0) {
    LOG(ERROR) << "Invalid offset: " << offset;
    return false;
  } else {
    reader_.seek(offset);
    if (reader_.status() != roo_io::kOk) {
      LOG(ERROR) << "Error seeking in the vault file " << path.c_str() << ": "
                 << roo_io::StatusAsString(reader_.status());
      return false;
    }
    position_ = offset;
  }
  MLOG(roo_monitoring_vault_reader)
      << "Vault file " << path.c_str() << " opened for read at index " << index_
      << " and position " << offset;
  return reader_.status() == roo_io::kOk;
}

VaultFileReader::~VaultFileReader() { reader_.close(); }

LogCursor VaultFileReader::tell() {
  if (index_ == 0) {
    // In this case, the file might have not existed, but that's OK;
    // we will just return that we're at the beginning of it.
    return LogCursor(ref_.timestamp(), 0);
  }
  if (past_eof()) {
    LOG(FATAL) << "Attempt to read a position in a file that has been fully "
                  "read and is now closed.";
  } else if (reader_.ok()) {
    position_ = reader_.position();
  } else if (reader_.status() == roo_io::kClosed) {
    LOG(FATAL) << "Attempt to read a position in a file that has been "
                  "unexpectedly closed at index "
               << index_;
  }
  return LogCursor(ref_.timestamp(), position_);
}

bool VaultFileReader::next(std::vector<Sample>* sample) {
  sample->clear();
  if (past_eof()) {
    return false;
  }
  if (!reader_.ok()) {
    ++index_;
    return false;
  }
  // TODO: make this configurable.
  bool ignore_fill = (ref_.resolution() <= kResolution_65536_ms);
  if (read_data(reader_, sample, ignore_fill) == roo_io::kOk) {
    ++index_;
    if (past_eof()) {
      MLOG(roo_monitoring_vault_reader)
          << "End of file reached after successfully scanning the entire "
             "vault file ";
      position_ = reader_.position();
      reader_.close();
    }
    return true;
  }
  if (reader_.status() == roo_io::kEndOfStream) {
    MLOG(roo_monitoring_vault_reader)
        << "End of file reached prematurely, while reading data at index "
        << index_;
    position_ = 0;
  } else {
    position_ = reader_.position();
    LOG(ERROR) << "Error reading data at index " << index_;
  }
  ++index_;
  reader_.close();
  return false;
}

void VaultFileReader::seekForward(int64_t timestamp) {
  int skip = (timestamp - ref_.timestamp()) >> (ref_.resolution() << 1);
  if (skip <= 0) return;
  DCHECK_LE(skip + index_, kRangeElementCount);
  MLOG(roo_monitoring_vault_reader) << "Skipping " << skip << " steps";
  if (skip + index_ >= kRangeElementCount) {
    index_ = kRangeElementCount;
    reader_.close();
    return;
  }
  if (reader_.ok()) {
    std::vector<Sample> ignored;
    for (; !past_eof() && skip >= 0; --skip) {
      next(&ignored);
    }
  } else {
    index_ += skip;
  }
}

bool VaultFileReader::past_eof() const { return index_ >= kRangeElementCount; }

roo_logging::Stream& operator<<(roo_logging::Stream& os,
                                const VaultFileRef& file_ref) {
  os << "[" << file_ref.resolution() << ", " << roo_logging::hex
     << file_ref.timestamp() << ", " << file_ref.time_step() << ", "
     << (file_ref.timestamp() + file_ref.time_span()) << "]";
  return os;
}

}  // namespace roo_monitoring