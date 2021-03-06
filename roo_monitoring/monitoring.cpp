#include "roo_monitoring.h"
#include "common.h"
#include "compaction.h"
#include "datastream.h"
#include "log.h"

#include "glog/logging.h"

#include <map>

namespace roo_monitoring {

Transform Transform::Linear(float multiplier, float offset) {
  return Transform(multiplier, offset);
}

Transform Transform::LinearRange(float min_value, float max_value) {
  return Transform(65535.0 / (max_value - min_value), -min_value);
}

uint16_t Transform::apply(float value) const {
  float transformed = multiplier_ * value + offset_;
  if (transformed < 0) {
    return 0;
  }
  if (transformed >= 65535) {
    return 65535;
  }
  return (uint16_t)(transformed + 0.5);
}

float Transform::unapply(uint16_t value) const {
  return (value - offset_) / multiplier_;
}

Collection::Collection(String name)
    : name_(name), transform_(Transform::Linear(256, 0x8000)) {
  base_dir_ = kMonitoringBasePath;
  base_dir_ += "/";
  base_dir_ += name;
}

Writer::Writer(Collection* collection)
    : collection_(collection),
      log_dir_(subdir(collection->base_dir_, kLogSubPath)),
      writer_(log_dir_.c_str()) {}

WriteTransaction::WriteTransaction(Writer* writer)
    : transform_(&writer->collection_->transform()),
      writer_(&writer->writer_) {}

WriteTransaction::~WriteTransaction() { writer_->close(); }

void WriteTransaction::write(int64_t timestamp_ms, uint64_t stream_id,
                             float datum) {
  int64_t ts_rounded = timestamp_ms_floor(timestamp_ms, kTargetResolution);
  if (writer_->can_skip_write(ts_rounded, stream_id)) {
    // Fast path: already written data for this bucket.
    return;
  }
  uint16_t transformed = transform_->apply(datum);
  writer_->write(ts_rounded, stream_id, transformed);
}

class LogCompactionCursor {
 public:
  LogCompactionCursor() : log_cursor_(), target_datum_index_(0) {}
  LogCompactionCursor(LogCursor log_cursor, int16_t target_datum_index)
      : log_cursor_(log_cursor), target_datum_index_(target_datum_index) {
    CHECK_GE(target_datum_index, 0);
    CHECK_LE(target_datum_index, 255);
  }

  const LogCursor& log_cursor() const { return log_cursor_; }
  uint8_t target_datum_index() const { return target_datum_index_; }

 private:
  LogCursor log_cursor_;
  uint8_t target_datum_index_;
};

namespace {

String getLogCompactionCursorPath(const Collection* collection,
                                  const VaultFileRef& ref) {
  String cursor_file_path;
  collection->getVaultFilePath(ref, &cursor_file_path);
  cursor_file_path += ".cursor";
  return cursor_file_path;
}

bool tryReadLogCompactionCursor(const char* cursor_path,
                                LogCompactionCursor* result) {
  DataInputStream cursor_file(cursor_path, std::ios_base::in);
  if (!cursor_file.is_open()) {
    if (errno == ENOENT) {
      errno = 0;
    } else {
      LOG(ERROR) << "Failed to open cursor file " << cursor_path << ": "
                 << strerror(errno);
    }
    return false;
  }
  // Maybe can append.
  uint8_t target_datum_index = cursor_file.read_uint8();
  uint64_t source_file = cursor_file.read_varint();
  uint64_t source_checkpoint = cursor_file.read_varint();
  if (!cursor_file.good()) {
    LOG(ERROR) << "Error reading data from the cursor file: "
               << strerror(errno);
    return false;
  }
  *result = LogCompactionCursor(LogCursor(source_file, source_checkpoint),
                                target_datum_index);
  LOG(INFO) << "Successfully read the cursor content " << cursor_path << ": "
            << std::hex << source_file << std::dec << ", " << source_checkpoint
            << ", " << (int)target_datum_index;
  return true;
}

bool writeCursor(const char* cursor_path, const LogCompactionCursor cursor) {
  DataOutputStream cursor_file(cursor_path, std::ios_base::out);
  if (cursor_file.is_open()) {
    LOG(INFO) << "Writing cursor content " << cursor_path << ": " << std::hex
              << cursor.log_cursor().file() << std::dec << ", "
              << cursor.log_cursor().position() << ", "
              << (int)cursor.target_datum_index();

    cursor_file.write_uint8(cursor.target_datum_index());
    cursor_file.write_varint(cursor.log_cursor().file());
    CHECK_GE(cursor.log_cursor().position(), 0);
    cursor_file.write_varint(cursor.log_cursor().position());
    cursor_file.close();
  }
  if (!cursor_file.good()) {
    LOG(ERROR) << "Error writing to the cursor file " << cursor_path << ": "
               << strerror(errno);
  }
  return cursor_file.good();
}

}  // namespace

bool Writer::Compact() {
  LogReader reader(log_dir_.c_str(), writer_.first_timestamp());
  while (reader.nextRange()) {
    VaultWriter writer(collection_, VaultFileRef::Lookup(reader.range_floor(),
                                                         kTargetResolution));

    // See if we can use cursor.
    String cursor_path =
        getLogCompactionCursorPath(collection_, writer.vault_ref());
    LogCompactionCursor cursor;
    if (tryReadLogCompactionCursor(cursor_path.c_str(), &cursor) &&
        reader.seek(cursor.log_cursor())) {
      writer.openExisting(cursor.target_datum_index());
      if (!writer.good()) return false;
    } else {
      if (errno != 0) return false;
      writer.openNew();
      if (!writer.good()) return false;
    }
    remove(cursor_path.c_str());

    // In any case, now just iterate and compact.
    int64_t increment = timestamp_increment(1, kTargetResolution);
    int64_t current =
        writer.vault_ref().timestamp() +
        timestamp_increment(writer.write_index(), kTargetResolution);
    int16_t compaction_index_begin = writer.write_index();
    int64_t timestamp;
    std::vector<LogSample> data;
    while (reader.nextSample(&timestamp, &data)) {
      if (timestamp < current) {
        // Ignoring out-of-order log entries.
        continue;
      }
      while (current < timestamp) {
        writer.writeEmptyData();
        current += increment;
      }
      CHECK_EQ(current, timestamp);
      writer.writeLogData(data);
      current += increment;
    }
    if (!writer.good()) return false;

    if (reader.isHotRange()) {
      if (!writeCursor(
              cursor_path.c_str(),
              LogCompactionCursor(reader.tell(), writer.write_index()))) {
        return false;
      }
    } else {
      while (writer.write_index() < kRangeElementCount) {
        writer.writeEmptyData();
        current += increment;
      }
      reader.deleteRange();
    }
    writer.close();
    CompactVault(writer.vault_ref(), compaction_index_begin,
                 writer.write_index(), reader.isHotRange());
  }
  return true;
}

bool Writer::CompactVault(VaultFileRef ref, int16_t index_begin,
                          int16_t index_end, bool hot) {
  LOG(INFO) << "Starting vault compaction.";
  while (true) {
    VaultFileRef parent = ref.parent();
    int parent_index_begin = 64 * ref.sibling_index() + (index_begin >> 2);
    int parent_index_end = 64 * ref.sibling_index() + (index_end >> 2);
    ref = parent;
    index_begin = parent_index_begin;
    index_end = parent_index_end;
    if (index_end <= index_begin || ref.resolution() > kMaxResolution) {
      LOG(INFO) << "Vault compacton finished.";
      return true;
    }
    CHECK_LE(index_end, 256);
    CHECK_GT(index_end, 0);
    if (!CompactVaultOneLevel(ref, index_begin, index_end,
                              hot || ref.sibling_index() < 3)) {
      LOG(ERROR) << "Vault compaction failed at resolution "
                 << ref.resolution();
      return false;
    }
  }
}

bool Writer::CompactVaultOneLevel(VaultFileRef ref, int16_t index_begin,
                                  int16_t index_end, bool hot) {
  VaultWriter writer(collection_, ref);
  VaultFileReader reader(collection_);
  LOG(INFO) << "Compacting " << std::hex
            << writer.vault_ref() << ", with end index " << std::dec
            << index_end;

  // See if we can use a cursor file.
  String cursor_path = getLogCompactionCursorPath(collection_, ref);
  LogCompactionCursor cursor;
  if (tryReadLogCompactionCursor(cursor_path.c_str(), &cursor) &&
      reader.open(ref.child(cursor.target_datum_index() / 64),
                  (cursor.target_datum_index() % 64) << 2,
                  cursor.log_cursor().position())) {
    writer.openExisting(cursor.target_datum_index());
  } else {
    reader.open(ref.child(0), 0, 0);
    writer.openNew();
  }
  remove(cursor_path.c_str());

  // Now iterate and compact.
  std::vector<Sample> sample_group;
  Aggregator aggregator;

  while (writer.write_index() < index_end) {
    CHECK_LE(reader.index(), 252);
    for (int i = 0; i < 4; ++i) {
      reader.next(&sample_group);
      for (const Sample& sample : sample_group) {
        if (sample.fill() > 0) {
          aggregator.add(sample);
        }
      }
    }
    writer.writeAggregatedData(aggregator);
    aggregator.clear();
    if (reader.past_eof()) {
      reader.open(reader.vault_ref().next(), 0, 0);
    }
  }
  if (writer.write_index() > 0 && writer.write_index() < 256) {
    writeCursor(cursor_path.c_str(),
                LogCompactionCursor(reader.tell(), writer.write_index()));
  }
  reader.close();
  writer.close();
  if (!reader.good()) {
    LOG(ERROR) << "Failed to process the input vault file: "
               << strerror(reader.my_errno());
    return false;
  }
  if (!writer.good()) {
    LOG(ERROR) << "Failed to process the output vault file: "
               << strerror(writer.my_errno());
    return false;
  }
  LOG(INFO) << "Finished compacting " << std::hex
            << writer.vault_ref() << ", with end index " << std::dec
            << writer.write_index();
  return true;
}

VaultFileRef VaultFileRef::Lookup(int64_t timestamp, int resolution) {
  int ms_per_range_exp = resolution + kRangeLength;
  int64_t range_floor = timestamp_ms_floor(timestamp, ms_per_range_exp);
  return VaultFileRef(range_floor, resolution);
}

void Collection::getVaultFilePath(const VaultFileRef& ref, String* path) const {
  // Introduce a 2nd level directory structure with max 256 (4^4) files.
  // Each file covers 256 (4 ^ range length) time steps, and each time step
  // covers 4^resolution milliseconds.
  int ms_per_group_range_exp = ref.resolution() + kRangeLength + 4;
  Filename filename = Filename::forTimestamp(ref.timestamp());
  Filename dirname = Filename::forTimestamp(
      timestamp_ms_floor(ref.timestamp(), ms_per_group_range_exp));
  *path = base_dir_;
  *path += "/";
  *path += "vault-";
  *path += toHexDigit((ref.resolution() >> 4) & 0xF);
  *path += toHexDigit((ref.resolution() >> 0) & 0xF);
  *path += "/";
  *path += dirname.filename();
  *path += "/";
  *path += filename.filename();
}

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
                 << strerror(is.my_errno());
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
  return true;
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

VaultIterator::VaultIterator(const Collection* collection, int64_t start,
                             int resolution)
    : collection_(collection),
      current_ref_(VaultFileRef::Lookup(start, resolution)),
      current_(collection) {
  current_.open(current_ref_, 0, 0);
  current_.seekForward(start);
}

void VaultIterator::next(std::vector<Sample>* sample) {
  if (current_.past_eof()) {
    current_ref_ = current_ref_.next();
    LOG(INFO) << "Advancing to next file: " << std::hex
              << current_ref_.timestamp();
    current_.open(current_ref_, 0, 0);
  }
  current_.next(sample);
}

int64_t VaultIterator::cursor() const {
  return current_ref_.timestamp_at(current_.index());
}

std::ostream& operator<<(std::ostream& os, const VaultFileRef& file_ref) {
  os << "[" << file_ref.resolution() << ", " << std::hex << file_ref.timestamp()
     << ", " << file_ref.time_step() << ", "
     << (file_ref.timestamp() + file_ref.time_span()) << "]";
  return os;
}

}  // namespace roo_monitoring