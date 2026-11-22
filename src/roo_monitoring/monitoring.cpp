
#include <algorithm>
#include <map>

#include "common.h"
#include "compaction.h"
#include "log.h"
#include "roo_io/data/input_stream_reader.h"
#include "roo_io/data/output_stream_writer.h"
#include "roo_io/fs/fsutil.h"
#include "roo_logging.h"
#include "roo_monitoring.h"

#ifdef ROO_TESTING
const char* GetVfsRoot();
#endif

#ifndef MLOG_roo_monitoring_compaction
#define MLOG_roo_monitoring_compaction 0
#endif

#ifndef MLOG_roo_monitoring_vault_reader
#define MLOG_roo_monitoring_vault_reader 0
#endif

namespace roo_monitoring {

Collection::Collection(roo_io::Filesystem& fs, String name,
                       Resolution resolution)
    : fs_(fs),
      name_(name),
      resolution_(resolution),
      transform_(Transform::Linear(256, 0x8000)) {
  base_dir_ = kMonitoringBasePath;
  base_dir_ += "/";
  base_dir_ += name;
}

Writer::Writer(Collection* collection)
    : collection_(collection),
      log_dir_(subdir(collection->base_dir_, kLogSubPath)),
      cache_(collection->fs(), log_dir_.c_str()),
      writer_(collection->fs(), log_dir_.c_str(), cache_,
              collection->resolution()),
      io_state_(Writer::IOSTATE_OK),
      compaction_head_index_end_(0),
      compacting_historical_range_(false),
      flush_in_progress_(false) {}

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

roo_io::Status tryReadLogCompactionCursor(roo_io::Mount& fs,
                                          const char* cursor_path,
                                          LogCompactionCursor* result) {
  auto reader = roo_io::OpenDataFile(fs, cursor_path);
  if (!reader.ok()) {
    if (reader.status() != roo_io::kNotFound) {
      LOG(ERROR) << "Failed to open cursor file " << cursor_path << ": "
                 << roo_io::StatusAsString(reader.status());
    }
    return reader.status();
  }
  // Maybe can append.
  uint8_t target_datum_index = reader.readU8();
  uint64_t source_file = reader.readVarU64();
  uint64_t source_checkpoint = reader.readVarU64();
  if (!reader.ok()) {
    LOG(ERROR) << "Error reading data from the cursor file: "
               << roo_io::StatusAsString(reader.status())
               << ". Will ignore the cursor.";
    return reader.status();
  }
  *result = LogCompactionCursor(LogCursor(source_file, source_checkpoint),
                                target_datum_index);
  MLOG(roo_monitoring_compaction)
      << "Successfully read the cursor content " << cursor_path << ": "
      << roo_logging::hex << source_file << roo_logging::dec << ", "
      << source_checkpoint << ", " << (int)target_datum_index;
  return roo_io::kOk;
}

bool writeCursor(roo_io::Mount& fs, const char* cursor_path,
                 const LogCompactionCursor cursor) {
  auto writer = OpenDataFileForWrite(fs, cursor_path, roo_io::kFailIfExists);
  if (!writer.ok()) {
    LOG(ERROR) << "Error opening the cursor file " << cursor_path
               << "for write: " << roo_io::StatusAsString(writer.status());
  }
  MLOG(roo_monitoring_compaction)
      << "Writing cursor content " << cursor_path << ": " << roo_logging::hex
      << cursor.log_cursor().file() << roo_logging::dec << ", "
      << cursor.log_cursor().position() << ", "
      << (int)cursor.target_datum_index();

  writer.writeU8(cursor.target_datum_index());
  writer.writeVarU64(cursor.log_cursor().file());
  CHECK_GE(cursor.log_cursor().position(), 0);
  writer.writeVarU64(cursor.log_cursor().position());
  writer.close();
  if (writer.status() != roo_io::kClosed) {
    LOG(ERROR) << "Error writing to the cursor file " << cursor_path << ": "
               << strerror(errno);
    return false;
  }
  return true;
}

// A completed historical range is recorded before any of its logs are removed.
// The final name only appears after the complete record has been written.
roo_io::Status WriteCompletedRange(roo_io::Mount& fs, const String& path,
                                   int64_t end) {
  String temporary_path = path + ".tmp";
  roo_io::OutputStreamWriter writer = OpenDataFileForWrite(
      fs, temporary_path.c_str(), roo_io::kTruncateIfExists);
  if (!writer.ok()) return writer.status();
  writer.writeBeU64(end);
  writer.flush();
  roo_io::Status status = writer.status();
  writer.close();
  if (status != roo_io::kOk) return status;
  if (writer.status() != roo_io::kClosed) return writer.status();
  return fs.rename(temporary_path.c_str(), path.c_str());
}

// Reads the exclusive base-range boundary of a committed cleanup record.
roo_io::Status ReadCompletedRange(roo_io::Mount& fs, const String& path,
                                  int64_t range_span, int64_t* end) {
  auto reader = roo_io::OpenDataFile(fs, path.c_str());
  if (!reader.ok()) return reader.status();
  int64_t boundary = reader.readBeS64();
  if (!reader.ok() || boundary <= 0 || boundary % range_span != 0) {
    LOG(ERROR) << "Invalid completed range record " << path.c_str();
    return roo_io::kReadError;
  }
  *end = boundary;
  return roo_io::kOk;
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
  while (flush_in_progress_) flushSome();
  flushSome();
  while (flush_in_progress_) flushSome();
}

void Writer::flushSome() {
  // A failed writer cannot finish compaction. Keep source logs for a new one.
  if (io_state_ != IOSTATE_OK) return;
  roo_io::Mount fs = collection_->fs().mount();
  if (!fs.ok()) return;

  bool finished = flush_in_progress_;
  if (finished) {
    Status status = compactVaultOneLevel();
    if (status == Writer::IN_PROGRESS) return;
    flush_in_progress_ = false;
    if (status == Writer::FAILED) {
      LOG(ERROR) << "Vault compaction failed at resolution "
                 << compaction_head_.resolution();
      io_state_ = IOSTATE_ERROR;
      return;
    }
  }

  // At most one historical range is ever eligible for cleanup. When opening a
  // writer after a restart, finish that cleanup before choosing the next range.
  int64_t completed_end = -1;
  int64_t range_span =
      timestamp_increment(kRangeElementCount, collection_->resolution());
  String completed_path = subdir(log_dir_, "compaction.done");
  roo_io::Status status = roo_io::kOk;
  if (!finished) {
    status = ReadCompletedRange(fs, completed_path, range_span, &completed_end);
    if (status == roo_io::kNotFound) status = roo_io::kOk;
  } else if (compacting_historical_range_) {
    completed_end = pending_compacted_until_;
    status = WriteCompletedRange(fs, completed_path, completed_end);
  }
  if (status != roo_io::kOk) {
    LOG(ERROR) << "Failed to record or recover compaction: "
               << roo_io::StatusAsString(status);
    io_state_ = IOSTATE_ERROR;
    return;
  }

  if (finished || completed_end >= 0) {
    collection_->compacted_until_ =
        std::max(collection_->compacted_until_,
                 finished ? pending_compacted_until_ : completed_end);
  }
  if (completed_end >= 0) {
    status = cache_.deleteRange(fs, completed_end - range_span, completed_end);
    if (status == roo_io::kOk) status = fs.remove(completed_path.c_str());
    if (status != roo_io::kOk) {
      LOG(ERROR) << "Failed to clean up the completed log range: "
                 << roo_io::StatusAsString(status);
      io_state_ = IOSTATE_ERROR;
      return;
    }
  }

  LogReader reader(fs, log_dir_.c_str(), cache_, collection_->resolution(),
                   writer_.first_timestamp());
  if (!reader.nextRange() || (finished && reader.isHotRange())) return;
  compacting_historical_range_ = !reader.isHotRange();
  compaction_head_ =
      VaultFileRef::Lookup(reader.range_floor(), collection_->resolution());
  compaction_head_index_end_ = writeToVault(fs, reader, compaction_head_);
  if (io_state_ != IOSTATE_OK) return;
  flush_in_progress_ = true;
  MLOG(roo_monitoring_compaction) << "Starting vault compaction.";
}

int16_t Writer::writeToVault(roo_io::Mount& fs, LogReader& reader,
                             VaultFileRef ref) {
  VaultWriter writer(collection_, ref);

  // See if we can use cursor.
  String cursor_path = getLogCompactionCursorPath(collection_, ref);
  LogCompactionCursor cursor;
  roo_io::Status status;
  bool opened = false;
  status = tryReadLogCompactionCursor(fs, cursor_path.c_str(), &cursor);
  if (status == roo_io::kOk) {
    if (reader.seek(cursor.log_cursor())) {
      writer.openExisting(cursor.target_datum_index());
      if (writer.ok()) {
        opened = true;
      }
      if (fs.remove(cursor_path.c_str()) != roo_io::kOk) {
        io_state_ = IOSTATE_ERROR;
        return -1;
      }
    }
  }
  if (status != roo_io::kNotFound) {
    fs.remove(cursor_path.c_str());
  }
  if (!opened) {
    // Cursor not found.
    writer.openNew();
    if (!writer.ok()) {
      io_state_ = IOSTATE_ERROR;
      return -1;
    }
  }

  // In any case, now just iterate and compact.
  int64_t increment = timestamp_increment(1, collection_->resolution());
  int64_t current =
      writer.vault_ref().timestamp() +
      timestamp_increment(writer.write_index(), collection_->resolution());
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
  if (!writer.ok()) {
    io_state_ = IOSTATE_ERROR;
    return -1;
  }

  if (reader.isHotRange()) {
    if (!writeCursor(
            fs, cursor_path.c_str(),
            LogCompactionCursor(reader.tell(), writer.write_index()))) {
      io_state_ = IOSTATE_ERROR;
      return -1;
    }
  } else {
    while (writer.write_index() < kRangeElementCount) {
      writer.writeEmptyData();
      current += increment;
    }
  }
  // compaction_range.index_begin = compaction_index_begin;
  int16_t compaction_index_end = writer.write_index();
  writer.close();
  if (writer.status() != roo_io::kClosed) {
    io_state_ = IOSTATE_ERROR;
    return -1;
  }
  // Do not publish base-vault progress until all parent levels have finished.
  pending_compacted_until_ = current;
  return compaction_index_end;
}

Writer::Status Writer::compactVaultOneLevel() {
  roo_io::Mount fs = collection_->fs().mount();
  if (!fs.ok()) return Writer::FAILED;
  VaultFileRef parent = compaction_head_.parent();
  compaction_head_index_end_ =
      (kRangeElementCount / 4) * compaction_head_.sibling_index() +
      (compaction_head_index_end_ >> 2);
  compaction_head_ = parent;
  if (compaction_head_.resolution() > kMaxResolution) {
    MLOG(roo_monitoring_compaction) << "Vault compacton finished.";
    return Writer::OK;
  }
  if (compaction_head_index_end_ == 0) {
    MLOG(roo_monitoring_compaction) << "Compaction index = 0";
    // We're definitely done compacting.
    return Writer::OK;
  }
  CHECK_LE(compaction_head_index_end_, kRangeElementCount);
  CHECK_GT(compaction_head_index_end_, 0);

  VaultWriter writer(collection_, compaction_head_);
  VaultFileReader reader(collection_);
  MLOG(roo_monitoring_compaction)
      << "Compacting " << roo_logging::hex << writer.vault_ref()
      << ", with end index " << roo_logging::dec << compaction_head_index_end_;

  // See if we can use a cursor file.
  String cursor_path =
      getLogCompactionCursorPath(collection_, compaction_head_);
  bool opened = false;
  LogCompactionCursor cursor;
  roo_io::Status status =
      tryReadLogCompactionCursor(fs, cursor_path.c_str(), &cursor);
  if (status == roo_io::kOk) {
    reader.open(compaction_head_.child(cursor.target_datum_index() /
                                       (kRangeElementCount / 4)),
                (cursor.target_datum_index() % (kRangeElementCount / 4)) << 2,
                cursor.log_cursor().position());
    if (reader.ok()) {
      writer.openExisting(cursor.target_datum_index());
      if (writer.ok()) {
        opened = true;
      }
    }
  }
  if (opened && writer.write_index() >= compaction_head_index_end_) {
    // A restart can find this level complete while its parent is still pending.
    // Leave the cursor intact and continue up the hierarchy.
    return Writer::IN_PROGRESS;
  }
  if (status != roo_io::kNotFound &&
      fs.remove(cursor_path.c_str()) != roo_io::kOk) {
    return Writer::FAILED;
  }
  if (!opened) {
    reader.open(compaction_head_.child(0), 0, 0);
    writer.openNew();
  }

  // Now iterate and compact.
  std::vector<Sample> sample_group;
  Aggregator aggregator;
  do {
    CHECK_LE(reader.index(), kRangeElementCount - 4);
    for (int i = 0; i < 4; ++i) {
      // Ignore missing input files when compacting.
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
  } while (writer.write_index() < compaction_head_index_end_);
  writer.close();
  if (writer.status() != roo_io::kClosed) {
    LOG(ERROR) << "Failed to process the output vault file: "
               << roo_io::StatusAsString(writer.status());
    return Writer::FAILED;
  }
  if (writer.write_index() > 0 && writer.write_index() < kRangeElementCount) {
    // The vault file is unfinished; create a write cursor for it.
    if (!writeCursor(
            fs, cursor_path.c_str(),
            LogCompactionCursor(reader.tell(), writer.write_index()))) {
      return Writer::FAILED;
    }
  }
  reader.close();
  if (reader.status() != roo_io::kClosed) {
    LOG(ERROR) << "Failed to process the input vault file: "
               << roo_io::StatusAsString(reader.status());
    return Writer::FAILED;
  }
  MLOG(roo_monitoring_compaction)
      << "Finished compacting " << roo_logging::hex << writer.vault_ref()
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
    MLOG(roo_monitoring_vault_reader)
        << "Advancing to next file: " << roo_logging::hex
        << current_ref_.timestamp();
    current_.open(current_ref_, 0, 0);
  }
  current_.next(sample);
}

int64_t VaultIterator::cursor() const {
  return current_ref_.timestamp_at(current_.index());
}

}  // namespace roo_monitoring