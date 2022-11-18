
#include "vault.h"

#include <map>

#include "roo_monitoring.h"
#include "common.h"
#include "datastream.h"
#include "log.h"
#include "roo_glog/logging.h"

namespace roo_monitoring {

namespace {

bool read_header(DataInputStream& is) {
  int major = is.read_uint8();
  int minor = is.read_uint8();
  if (!is.good()) {
    LOG(ERROR) << "Failed to read vault file header: " << strerror(errno);
    return false;
  }
  if (major != 1 || minor != 1) {
    LOG(ERROR) << "Invalid content of vault file header: " << major << ", "
               << minor;
    return false;
  }
  return true;
}

bool read_data(DataInputStream& is, std::vector<Sample>* data,
               bool ignore_fill) {
  data->clear();
  uint64_t sample_count = is.read_varint();
  if (!is.good()) {
    if (!is.eof()) {
      LOG(ERROR) << "Failed to read data from the vault file: "
                 << strerror(is.my_errno());
    }
    return false;
  }
  for (uint64_t i = 0; i < sample_count; ++i) {
    uint64_t stream_id = is.read_varint();
    uint16_t avg = is.read_uint16();
    uint16_t min = is.read_uint16();
    uint16_t max = is.read_uint16();
    uint16_t fill = is.read_uint16();
    if (ignore_fill) {
      fill = 0x2000;
    }
    if (!is.good()) {
      LOG(ERROR) << "Failed to read a sample from the vault file: "
                 << is.status();
      return false;
    }
    data->emplace_back(stream_id, avg, min, max, fill);
  }
  return true;
}

}  // namespace

VaultFileReader::VaultFileReader(const Collection* collection)
    : collection_(collection), ref_(), index_(0), position_(0) {}

bool VaultFileReader::open(const VaultFileRef& vault_ref, int index,
                           int64_t offset) {
  String path;
  ref_ = vault_ref;
  collection_->getVaultFilePath(vault_ref, &path);
  file_.open(path.c_str(), std::ios_base::in);
  index_ = index;
  position_ = 0;
  if (!file_.is_open()) {
    if (errno == ENOENT) {
      LOG(INFO) << "Vault file " << path.c_str()
                << " doesn't exist; treating as-if empty";
      file_.clear_error();
      errno = 0;
    } else {
      LOG(ERROR) << "Failed to open vault file for read: " << path.c_str()
                 << ": " << strerror(file_.my_errno());
    }
    return false;
  }
  if (offset == 0) {
    if (!read_header(file_)) {
      file_.close();
      return false;
    }
    position_ = file_.tellg();
  } else if (offset < 0) {
    LOG(ERROR) << "Invalid offset: " << offset;
    return false;
  } else {
    file_.seekg(offset);
    if (file_.bad()) {
      LOG(ERROR) << "Error seeking in the vault file " << path.c_str() << ": "
                 << strerror(file_.my_errno());
      return false;
    }
    position_ = offset;
  }
  LOG(INFO) << "Vault file " << path.c_str() << " opened for read at index "
            << index_ << " and position " << offset;
  return file_.good();
}

VaultFileReader::~VaultFileReader() { file_.close(); }

LogCursor VaultFileReader::tell() {
  if (index_ == 0) {
    // In this case, the file might have not existed, but that's OK;
    // we will just return that we're at the beginning of it.
    return LogCursor(ref_.timestamp(), 0);
  }
  if (past_eof()) {
    LOG(FATAL) << "Attempt to read a position in a file that has been fully "
                  "read and is now closed.";
  } else if (file_.is_open()) {
    position_ = file_.tellg();
  } else if (file_.bad()) {
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
  if (!file_.is_open()) {
    ++index_;
    return false;
  }
  bool ignore_fill = (ref_.resolution() <= kInterpolationResolution);
  if (read_data(file_, sample, ignore_fill)) {
    ++index_;
    if (past_eof()) {
      LOG(INFO) << "End of file reached after successfully scanning the entire "
                   "vault file ";
      position_ = file_.tellg();
      file_.close();
    }
    return true;
  }
  if (file_.eof()) {
    LOG(INFO) << "End of file reached prematurely, while reading data at index "
              << index_;
  } else {
    LOG(ERROR) << "Error reading data at index " << index_;
  }
  ++index_;
  position_ = file_.tellg();
  file_.close();
  return false;
}

void VaultFileReader::seekForward(int64_t timestamp) {
  int skip = (timestamp - ref_.timestamp()) >> (ref_.resolution() << 1);
  if (skip <= 0) return;
  DCHECK_LE(skip + index_, kRangeElementCount);
  LOG(INFO) << "Skipping " << skip << " steps";
  if (skip + index_ >= kRangeElementCount) {
    index_ = kRangeElementCount;
    file_.close();
    return;
  }
  if (file_.is_open()) {
    std::vector<Sample> ignored;
    for (; !past_eof() && skip >= 0; --skip) {
      next(&ignored);
    }
  } else {
    index_ += skip;
  }
}

bool VaultFileReader::past_eof() const { return index_ >= kRangeElementCount; }

std::ostream& operator<<(std::ostream& os, const VaultFileRef& file_ref) {
  os << "[" << file_ref.resolution() << ", " << std::hex << file_ref.timestamp()
     << ", " << file_ref.time_step() << ", "
     << (file_ref.timestamp() + file_ref.time_span()) << "]";
  return os;
}

}  // namespace roo_monitoring