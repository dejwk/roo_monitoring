#pragma once

#include <set>
#include <vector>

#include "datastream.h"

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

class LogFileReader {
 public:
  LogFileReader() {}

  bool open(const char* path, int64_t checkpoint);
  bool is_open() const { return is_.is_open(); }
  void close() { is_.close(); }

  int64_t checkpoint() const { return checkpoint_; }

  bool next(int64_t* timestamp, std::vector<LogSample>* data, bool is_hot);

 private:
  DataInputStream is_;
  int64_t checkpoint_;
};

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
  LogReader(const char* log_dir, int64_t hot_file = -1);

  bool nextRange();
  int64_t range_floor() const { return range_floor_; }
  bool seek(LogCursor cursor);
  LogCursor tell();

  bool isHotRange();
  void deleteRange();

  bool nextSample(int64_t* timestamp, std::vector<LogSample>* data);

 private:
  bool open(int64_t file, uint64_t position);

  const char* log_dir_;
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
  LogWriter(const char* log_dir);

  void open(std::ios_base::openmode mode);
  void close();

  void write(int64_t timestamp, uint64_t stream_id, uint16_t datum);
  bool can_skip_write(int64_t timestamp, uint64_t stream_id);

  int64_t first_timestamp() const { return first_timestamp_; }

 private:
  // const that contains the path where log files are stored.
  const char* log_dir_;

  DataOutputStream os_;

  // For tentatively deduplicating data reported in the same target
  // resolution bucket.
  std::set<uint64_t> streams_;

  int64_t first_timestamp_;
  int64_t last_timestamp_;
  int64_t range_ceil_;
};

}  // namespace roo_monitoring