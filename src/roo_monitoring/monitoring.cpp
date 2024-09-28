
#include <map>

#include "common.h"
#include "compaction.h"
#include "datastream.h"
#include "log.h"
#include "roo_logging.h"
#include "roo_monitoring.h"

#ifdef ROO_TESTING
const char* GetVfsRoot();
#endif

namespace roo_monitoring {

Collection::Collection(String name, Resolution resolution)
    : name_(name),
      resolution_(resolution),
      transform_(Transform::Linear(256, 0x8000)) {
  base_dir_ = "";
#ifdef ROO_TESTING
  base_dir_ += GetVfsRoot();
#endif
  base_dir_ += kMonitoringBasePath;
  base_dir_ += "/";
  base_dir_ += name;
}

Writer::Writer(Collection* collection)
    : collection_(collection),
      log_dir_(subdir(collection->base_dir_, kLogSubPath)),
      cache_(log_dir_.c_str()),
      writer_(log_dir_.c_str(), cache_, collection->resolution()),
      io_state_(Writer::IOSTATE_OK) {}

WriteTransaction::WriteTransaction(Writer* writer)
    : transform_(&writer->collection_->transform()),
      writer_(&writer->writer_) {}

WriteTransaction::~WriteTransaction() { writer_->close(); }

void WriteTransaction::write(int64_t timestamp_ms, uint64_t stream_id,
                             float datum) {
  int64_t ts_rounded = timestamp_ms_floor(timestamp_ms, writer_->resolution());
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
               << cursor_file.status() << ". Will ignore the cursor.";
    return false;
  }
  *result = LogCompactionCursor(LogCursor(source_file, source_checkpoint),
                                target_datum_index);
  LOG(INFO) << "Successfully read the cursor content " << cursor_path << ": "
            << roo_logging::hex << source_file << roo_logging::dec << ", "
            << source_checkpoint << ", " << (int)target_datum_index;
  return true;
}

bool writeCursor(const char* cursor_path, const LogCompactionCursor cursor) {
  DataOutputStream cursor_file(cursor_path, std::ios_base::out);
  if (cursor_file.is_open()) {
    LOG(INFO) << "Writing cursor content " << cursor_path << ": "
              << roo_logging::hex << cursor.log_cursor().file()
              << roo_logging::dec << ", " << cursor.log_cursor().position()
              << ", " << (int)cursor.target_datum_index();

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

// Vault files form a hierarchy. Four vault files from a lower level cover the
// same time span as a single vault file of a higher level, but with 4x time
// resolution.
//
// Vault files are progressively compacted. Naively, when 4 lower-level vault
// files are finished, they can be compacted to a single new higher-level vault
// file.

// In order to support more incremental compaction, we use a notion of 'hot'
// vault files, which are only partially filled. Every time 4 new entries are
// added to the lower-level 'hot' vault file, these new entries can be compacted
// into one new entry in the higher level 'hot' vault file. In order to support
// that, hot files are accompanied by 'compaction cursor' files. A compaction
// cursor file has the following format:
//
// * target datum index (uint8): the current count of entries in the
//   higher-level vault file. Always within [0 - 255].
// * source file (varint): the start_timestamp (thus filename) of the
//   lower-level hot file that is being compacted.
// * source checkpoint (varint): byte offset in the lower level file up
//   to which the data has already been compacted.
//
// The compaction algorithm tries to pick up where it left off, by looking for
// the cursor file and seeking in both the source and the destination files. If
// the cursor file is missing or malformed, the compaction is simply done from
// scratch (i.e. the destination file is rebuild rather than appended to). After
// the compaction, if the destination file is still hot (i.e. has less than 256
// entries), a new cursor file is created to be used for the next compaction
// run.

void Writer::flushAll() {
  LogReader reader(log_dir_.c_str(), cache_, collection_->resolution(),
                   writer_.first_timestamp());
  while (reader.nextRange()) {
    VaultFileRef ref =
        VaultFileRef::Lookup(reader.range_floor(), collection_->resolution());
    int16_t compaction_index_end;
    writeToVault(reader, ref, compaction_index_end);
    if (io_state() != IOSTATE_OK) return;
    CompactVault(ref, compaction_index_end, reader.isHotRange());
    if (io_state() != IOSTATE_OK) return;
  }
}

// Writer::Status Writer::flushSome() {
//   LogReader reader(log_dir_.c_str(), collection_->resolution(),
//                    writer_.first_timestamp());
//   while (reader.nextRange()) {
//     VaultFileRef ref =
//         VaultFileRef::Lookup(reader.range_floor(),
//         collection_->resolution());
//     int16_t compaction_index_end;
//     if (!writeToVault(reader, ref, compaction_index_end)) return FAILED;
//     if (!CompactVault(ref, compaction_index_end, reader.isHotRange()))
//       return FAILED;
//   }
//   return OK;
// }

void Writer::writeToVault(LogReader& reader, VaultFileRef ref,
                          int16_t& compaction_index_end) {
  VaultWriter writer(collection_, ref);

  // See if we can use cursor.
  String cursor_path = getLogCompactionCursorPath(collection_, ref);
  LogCompactionCursor cursor;
  if (tryReadLogCompactionCursor(cursor_path.c_str(), &cursor) &&
      reader.seek(cursor.log_cursor())) {
    writer.openExisting(cursor.target_datum_index());
    if (!writer.good()) {
      io_state_ = IOSTATE_ERROR;
      return;
    }
  } else {
    if (errno != 0) {
      io_state_ = IOSTATE_ERROR;
      return;
    }
    writer.openNew();
    if (!writer.good()) {
      io_state_ = IOSTATE_ERROR;
      return;
    }
  }
  remove(cursor_path.c_str());

  // In any case, now just iterate and compact.
  int64_t increment = timestamp_increment(1, collection_->resolution());
  int64_t current =
      writer.vault_ref().timestamp() +
      timestamp_increment(writer.write_index(), collection_->resolution());
  // int16_t compaction_index_begin = writer.write_index();
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
  if (!writer.good()) {
    io_state_ = IOSTATE_ERROR;
    return;
  }

  if (reader.isHotRange()) {
    if (!writeCursor(
            cursor_path.c_str(),
            LogCompactionCursor(reader.tell(), writer.write_index()))) {
      io_state_ = IOSTATE_ERROR;
      return;
    }
  } else {
    while (writer.write_index() < kRangeElementCount) {
      writer.writeEmptyData();
      current += increment;
    }
    reader.deleteRange();
  }
  // compaction_range.index_begin = compaction_index_begin;
  compaction_index_end = writer.write_index();
  writer.close();
}

void Writer::CompactVault(VaultFileRef& ref, int16_t compaction_index_end,
                          bool hot) {
  LOG(INFO) << "Starting vault compaction.";
  while (true) {
    VaultFileRef parent = ref.parent();
    compaction_index_end =
        64 * ref.sibling_index() + (compaction_index_end >> 2);
    ref = parent;
    if (ref.resolution() > kMaxResolution) {
      LOG(INFO) << "Vault compacton finished.";
      return;
    }
    if (compaction_index_end == 0) {
      LOG(INFO) << "Compaction index = 0";
      // We're definitely done compacting.
      return;
    }
    CHECK_LE(compaction_index_end, 256);
    CHECK_GT(compaction_index_end, 0);
    Status status = CompactVaultOneLevel(ref, compaction_index_end,
                                         hot || ref.sibling_index() < 3);
    if (status == Writer::OK) {
      // We're done compacting.
      return;
    } else if (status == Writer::FAILED) {
      LOG(ERROR) << "Vault compaction failed at resolution "
                 << ref.resolution();
      io_state_ = IOSTATE_ERROR;
      return;
    }
  }
}

Writer::Status Writer::CompactVaultOneLevel(VaultFileRef ref,
                                            int16_t compaction_index_end,
                                            bool hot) {
  VaultWriter writer(collection_, ref);
  VaultFileReader reader(collection_);
  LOG(INFO) << "Compacting " << roo_logging::hex << writer.vault_ref()
            << ", with end index " << roo_logging::dec << compaction_index_end;

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

  if (writer.write_index() >= compaction_index_end) {
    // We're done!
    return Writer::OK;
  }
  do {
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
  } while (writer.write_index() < compaction_index_end);
  if (writer.write_index() > 0 && writer.write_index() < 256) {
    writeCursor(cursor_path.c_str(),
                LogCompactionCursor(reader.tell(), writer.write_index()));
  }
  reader.close();
  writer.close();
  if (!reader.good()) {
    LOG(ERROR) << "Failed to process the input vault file: " << reader.status();
    return Writer::FAILED;
  }
  if (!writer.good()) {
    LOG(ERROR) << "Failed to process the output vault file: "
               << writer.status();
    return Writer::FAILED;
  }
  LOG(INFO) << "Finished compacting " << roo_logging::hex << writer.vault_ref()
            << ", with end index " << roo_logging::dec << writer.write_index();
  return Writer::IN_PROGRESS;
}

VaultFileRef VaultFileRef::Lookup(int64_t timestamp, Resolution resolution) {
  Resolution range_resolution = Resolution(resolution + kRangeLength);
  int64_t range_floor = timestamp_ms_floor(timestamp, range_resolution);
  return VaultFileRef(range_floor, resolution);
}

void Collection::getVaultFilePath(const VaultFileRef& ref, String* path) const {
  // Introduce a 2nd level directory structure with max 256 (4^4) files.
  // Each file covers 256 (4 ^ range length) time steps, and each time step
  // covers 4^resolution milliseconds.
  Resolution group_range_resolution =
      Resolution(ref.resolution() + kRangeLength + 4);
  Filename filename = Filename::forTimestamp(ref.timestamp());
  Filename dirname = Filename::forTimestamp(
      timestamp_ms_floor(ref.timestamp(), group_range_resolution));
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

VaultIterator::VaultIterator(const Collection* collection, int64_t start,
                             Resolution resolution)
    : collection_(collection),
      current_ref_(VaultFileRef::Lookup(start, resolution)),
      current_(collection) {
  current_.open(current_ref_, 0, 0);
  current_.seekForward(start);
}

void VaultIterator::next(std::vector<Sample>* sample) {
  if (current_.past_eof()) {
    current_ref_ = current_ref_.next();
    LOG(INFO) << "Advancing to next file: " << roo_logging::hex
              << current_ref_.timestamp();
    current_.open(current_ref_, 0, 0);
  }
  current_.next(sample);
}

int64_t VaultIterator::cursor() const {
  return current_ref_.timestamp_at(current_.index());
}

}  // namespace roo_monitoring