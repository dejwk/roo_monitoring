#pragma once

#include <vector>

#include "resolution.h"
#include "roo_collections/flat_small_hash_set.h"
#include "roo_io/data/multipass_input_stream_reader.h"
#include "roo_io/data/output_stream_writer.h"
#include "roo_io/fs/filesystem.h"

namespace roo_monitoring {

class LogSample {
 public:
  LogSample(uint64_t stream_id, uint16_t value)
      : stream_id_(stream_id), value_(value) {}

  uint64_t stream_id() const { return stream_id_; }
  uint16_t value() const { return value_; }

 private:
  uint64_t stream_id_;
  uint16_t value_;
};

inline bool operator<(const LogSample& a, const LogSample& b) {
  return a.stream_id() < b.stream_id();
}

class CachedLogDir {
 public:
  CachedLogDir(roo_io::Filesystem& fs, const char* log_dir)
      : fs_(fs), log_dir_(log_dir), synced_(false) {}

  void insert(int64_t entry) {
    sync();
    entries_.insert(entry);
  }

  void erase(int64_t entry) {
    sync();
    entries_.erase(entry);
  }

  std::vector<int64_t> list();

 private:
  void sync();

  roo_io::Filesystem& fs_;
  const char* log_dir_;
  bool synced_;

  roo_collections::FlatSmallHashSet<int64_t> entries_;
};

// For reading from a single log file.
class LogFileReader {
 public:
  LogFileReader(roo_io::Mount& mount) : fs_(mount) {}

  bool open(const char* path, int64_t checkpoint);

  bool is_open() const { return reader_.isOpen(); }

  void close() { reader_.close(); }

  int64_t checkpoint() const { return checkpoint_; }

  bool next(int64_t* timestamp, std::vector<LogSample>* data, bool is_hot);

 private:
  roo_io::Mount& fs_;
  roo_io::MultipassInputStreamReader reader_;
  uint8_t lookahead_entry_type_;
  int64_t checkpoint_;
};

// For reading from a sequence of log files.
class LogCursor {
 public:
  LogCursor() : file_(0), position_(0) {}
  LogCursor(int64_t file, int64_t position)
      : file_(file), position_(position) {}

  int64_t file() const { return file_; }
  int64_t position() const { return position_; }

 private:
  int64_t file_;
  int64_t position_;
};

class LogReader {
 public:
  LogReader(roo_io::Mount& fs, const char* log_dir, CachedLogDir& cache,
            Resolution resolution, int64_t hot_file = -1);

  bool nextRange();
  int64_t range_floor() const { return range_floor_; }
  bool seek(LogCursor cursor);
  LogCursor tell();

  bool isHotRange();
  void deleteRange();

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

class LogWriter {
 public:
  LogWriter(roo_io::Filesystem& fs, const char* log_dir, CachedLogDir& cache,
            Resolution resolution);

  Resolution resolution() const { return resolution_; }

  void open(roo_io::FileUpdatePolicy update_policy);
  void close();

  void write(int64_t timestamp, uint64_t stream_id, uint16_t datum);
  bool can_skip_write(int64_t timestamp, uint64_t stream_id);

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