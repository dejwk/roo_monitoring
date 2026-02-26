#pragma once

#include <vector>

#include "resolution.h"
#include "roo_collections/flat_small_hash_set.h"
#include "roo_io/data/multipass_input_stream_reader.h"
#include "roo_io/data/output_stream_writer.h"
#include "roo_io/fs/filesystem.h"

namespace roo_monitoring {

/// Sample stored in log files before compaction.
class LogSample {
 public:
  /// Creates a log sample for a stream/value pair.
  LogSample(uint64_t stream_id, uint16_t value)
      : stream_id_(stream_id), value_(value) {}

  /// Returns the stream identifier.
  uint64_t stream_id() const { return stream_id_; }
  /// Returns the encoded sample value.
  uint16_t value() const { return value_; }

 private:
  uint64_t stream_id_;
  uint16_t value_;
};

inline bool operator<(const LogSample& a, const LogSample& b) {
  return a.stream_id() < b.stream_id();
}

/// In-memory cache of log directory entries.
class CachedLogDir {
 public:
  /// Creates a cache for a specific filesystem and log directory.
  CachedLogDir(roo_io::Filesystem& fs, const char* log_dir)
      : fs_(fs), log_dir_(log_dir), synced_(false) {}

  /// Inserts an entry into the cache.
  void insert(int64_t entry) {
    sync();
    entries_.insert(entry);
  }

  /// Removes an entry from the cache.
  void erase(int64_t entry) {
    sync();
    entries_.erase(entry);
  }

  /// Returns the cached entries sorted by timestamp.
  std::vector<int64_t> list();

 private:
  void sync();

  roo_io::Filesystem& fs_;
  const char* log_dir_;
  bool synced_;

  roo_collections::FlatSmallHashSet<int64_t> entries_;
};

/// Reader for a single log file.
class LogFileReader {
 public:
  /// Creates a reader over the specified mount.
  LogFileReader(roo_io::Mount& mount) : fs_(mount) {}

  /// Opens the log file at path and seeks to checkpoint.
  bool open(const char* path, int64_t checkpoint);

  /// Returns true if a file is currently open.
  bool is_open() const { return reader_.isOpen(); }

  /// Closes the reader.
  void close() { reader_.close(); }

  /// Returns the current checkpoint position.
  int64_t checkpoint() const { return checkpoint_; }

  /// Reads the next entry from the file.
  bool next(int64_t* timestamp, std::vector<LogSample>* data, bool is_hot);

 private:
  roo_io::Mount& fs_;
  roo_io::MultipassInputStreamReader reader_;
  uint8_t lookahead_entry_type_;
  int64_t checkpoint_;
};

/// Cursor used when seeking through multiple log files.
class LogCursor {
 public:
  /// Creates a cursor at the start of the log sequence.
  LogCursor() : file_(0), position_(0) {}
  /// Creates a cursor for a specific file and position.
  LogCursor(int64_t file, int64_t position)
      : file_(file), position_(position) {}

  /// Returns the file timestamp associated with the cursor.
  int64_t file() const { return file_; }
  /// Returns the byte position within the file.
  int64_t position() const { return position_; }

 private:
  int64_t file_;
  int64_t position_;
};

/// Reader that walks across a sequence of log files.
class LogReader {
 public:
  /// Creates a reader for the specified log directory and resolution.
  LogReader(roo_io::Mount& fs, const char* log_dir, CachedLogDir& cache,
            Resolution resolution, int64_t hot_file = -1);

  /// Advances to the next time range.
  bool nextRange();
  /// Returns the lower bound of the current range.
  int64_t range_floor() const { return range_floor_; }
  /// Seeks to the specified cursor.
  bool seek(LogCursor cursor);
  /// Returns the current cursor.
  LogCursor tell();

  /// Returns true if the current range is hot (still being written).
  bool isHotRange();
  /// Deletes the current range files.
  void deleteRange();

  /// Reads the next sample in the current range.
  bool nextSample(int64_t* timestamp, std::vector<LogSample>* data);

 private:
  bool open(int64_t file, uint64_t position);

  roo_io::Mount& fs_;
  const char* log_dir_;
  CachedLogDir& cache_;
  Resolution resolution_;
  std::vector<int64_t> entries_;
  std::vector<int64_t>::const_iterator group_begin_;
  std::vector<int64_t>::const_iterator cursor_;
  std::vector<int64_t>::const_iterator group_end_;
  int64_t hot_file_;
  bool reached_hot_file_;
  int64_t range_floor_;
  int64_t range_ceil_;
  LogFileReader reader_;
};

/// Writer for log files at a fixed resolution.
class LogWriter {
 public:
  /// Creates a log writer for the specified directory and resolution.
  LogWriter(roo_io::Filesystem& fs, const char* log_dir, CachedLogDir& cache,
            Resolution resolution);

  /// Returns the resolution used for this writer.
  Resolution resolution() const { return resolution_; }

  /// Opens the log file according to the update policy.
  void open(roo_io::FileUpdatePolicy update_policy);
  /// Closes the log file.
  void close();

  /// Writes a single log sample.
  void write(int64_t timestamp, uint64_t stream_id, uint16_t datum);
  /// Returns true if a write can be skipped for this bucket.
  bool can_skip_write(int64_t timestamp, uint64_t stream_id);

  /// Returns the first timestamp recorded in the current file.
  int64_t first_timestamp() const { return first_timestamp_; }

 private:
  // const that contains the path where log files are stored.
  const char* log_dir_;
  CachedLogDir& cache_;
  Resolution resolution_;

  roo_io::Filesystem& fs_;
  roo_io::Mount mount_;
  roo_io::OutputStreamWriter writer_;

  // For tentatively deduplicating data reported in the same target
  // resolution bucket.
  roo_collections::FlatSmallHashSet<uint64_t> streams_;

  int64_t first_timestamp_;
  int64_t last_timestamp_;
  int64_t range_ceil_;
};

}  // namespace roo_monitoring