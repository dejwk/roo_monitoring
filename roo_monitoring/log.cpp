#include "log.h"

#include <Arduino.h>

#include <algorithm>

#include "common.h"
#include "datastream.h"
#include "roo_glog/logging.h"

namespace roo_monitoring {

enum Code { CODE_ERROR = 0, CODE_TIMESTAMP = 1, CODE_DATUM = 2 };

bool LogFileReader::open(const char* path, int64_t checkpoint) {
  is_.close();
  LOG(INFO) << "Opening log file " << path << " at " << checkpoint;
  is_.open(path, std::ios_base::in);
  if (!is_.good()) {
    LOG(ERROR) << "Failed to open log file " << path << ": "
               << strerror(is_.my_errno());
    return false;
  }
  if (checkpoint > 0) {
    is_.seekg(checkpoint);
    if (!is_.good()) {
      LOG(ERROR) << "Failed to seek in the log file " << path << ": "
                 << strerror(is_.my_errno());
      return false;
    }
  }
  checkpoint_ = checkpoint;
  return true;
}

bool LogFileReader::next(int64_t* timestamp, std::vector<LogSample>* data,
                         bool is_hot) {
  data->clear();
  if (checkpoint_ < 0) return false;
  int next = is_.peek_uint8();
  if (!is_.good()) return false;
  if (next != CODE_TIMESTAMP) {
    LOG(ERROR) << "Unexpected content in the log file: " << next;
    return false;
  }
  is_.read_uint8();
  *timestamp = is_.read_varint();
  if (is_.bad()) return false;

  // LOG(INFO) << "Read log data at timestamp: " << std::hex << *timestamp;
  while (true) {
    int next = is_.peek_uint8();
    if (is_.eof()) {
      if (is_hot) {
        // Indicate that the data isn't complete yet. Not updating the
        // source checkpoint in this case.
        return false;
      } else {
        // Indicating that we have reached the end of a 'historical' log file.
        LOG(INFO) << "Reached EOF of a historical log file ";
        checkpoint_ = -1;
        break;
      }
    } else if (!is_.good()) {
      // Must be I/O error.
      LOG(ERROR) << "Failed to read timestamped data from a log file: "
                 << strerror(is_.my_errno());
      return false;
    } else if (next == CODE_DATUM) {
      uint64_t stream_id;
      uint16_t datum;
      is_.read_uint8();
      stream_id = is_.read_varint();
      datum = is_.read_uint16();
      if (is_.bad()) return false;
      data->emplace_back(stream_id, datum);
    } else if (next == CODE_TIMESTAMP) {
      checkpoint_ = (int64_t)is_.tellg();
      break;
    }
  }
  std::sort(data->begin(), data->end());
  return true;
}

LogReader::LogReader(const char* log_dir, int resolution, int64_t hot_file)
    : log_dir_(log_dir),
      resolution_(resolution),
      entries_(listFiles(log_dir)),
      group_begin_(entries_.begin()),
      cursor_(entries_.begin()),
      group_end_(entries_.begin()),
      hot_file_(hot_file >= 0      ? hot_file
                : entries_.empty() ? 0
                                   : entries_.back()),
      reached_hot_file_(false),
      range_floor_(0),
      range_ceil_(0),
      reader_() {
  // Ensure that the hot file is at the end of the list, even if it is not, for
  // some reason, chronologically the newest. This way, we will always delete
  // the non-hot logs and leave the hot file be.
}

bool LogReader::nextRange() {
  if (group_end_ == entries_.end() || reached_hot_file_) {
    LOG(INFO) << "No more log files to process.";
    return false;
  }
  cursor_ = group_begin_ = group_end_;
  int ms_per_range_exp = resolution_ + kRangeLength;
  range_floor_ = timestamp_ms_floor(*cursor_, ms_per_range_exp);
  range_ceil_ = timestamp_ms_ceil(*cursor_, ms_per_range_exp);
  while (!reached_hot_file_ && group_end_ != entries_.end() &&
         *group_end_ <= range_ceil_) {
    if (*group_end_ == hot_file_) {
      reached_hot_file_ = true;
    }
    ++group_end_;
  }
  LOG(INFO) << "Processing log files for the range starting at " << std::hex
            << *group_begin_;
  return true;
}

bool LogReader::isHotRange() { return hot_file_ < range_ceil_; }

bool LogReader::open(int64_t file, uint64_t position) {
  return reader_.open(filepath(log_dir_, file).c_str(), position);
}

bool LogReader::nextSample(int64_t* timestamp, std::vector<LogSample>* data) {
  for (; cursor_ != group_end_; ++cursor_) {
    if (!reader_.is_open()) {
      if (!open(*cursor_, 0)) {
        LOG(ERROR) << "Failed to open log file " << *cursor_;
        continue;
      }
    }
    CHECK(reader_.is_open());
    if (reader_.next(timestamp, data, *cursor_ == hot_file_)) return true;
    reader_.close();
  }
  return false;
}

bool LogReader::seek(LogCursor cursor) {
  auto i = std::lower_bound(group_begin_, group_end_, cursor.file());
  if (i == group_end_ || *i != cursor.file()) {
    LOG(WARNING) << "Seek failed; file not found: " << cursor.file();
    return false;
  }
  if (!reader_.open(filepath(log_dir_, cursor.file()).c_str(),
                    cursor.position())) {
    LOG(WARNING) << "Seek failed; could not open: " << cursor.file();
    return false;
  }
  cursor_ = i;
  return true;
}

LogCursor LogReader::tell() {
  CHECK(isHotRange() && cursor_ == group_end_);
  return LogCursor(hot_file_, reader_.checkpoint());
}

void LogReader::deleteRange() {
  CHECK(!isHotRange());
  for (auto i = group_begin_; i != group_end_; ++i) {
    LOG(INFO) << "Removing processed log file " << std::hex << *i;
    if (remove(filepath(log_dir_, *i).c_str()) != 0) {
      LOG(ERROR) << "Failed to remove processed log file " << std::hex << *i;
    }
  }
}

LogWriter::LogWriter(const char* log_dir, int resolution)
    : log_dir_(log_dir),
      resolution_(resolution),
      first_timestamp_(-1),
      last_timestamp_(-1),
      range_ceil_(-1) {}

void writeTimestamp(DataOutputStream& os, int64_t timestamp) {
  os.write_uint8(CODE_TIMESTAMP);
  os.write_varint(timestamp);
}

void writeDatum(DataOutputStream& os, uint64_t stream_id,
                uint16_t transformed_datum) {
  os.write_uint8(CODE_DATUM);
  os.write_varint(stream_id);
  os.write_uint16(transformed_datum);
  // write_float(file, datum);
}

void LogWriter::open(std::ios_base::openmode mode) {
  String path = log_dir_;
  path += "/";
  path += Filename::forTimestamp(first_timestamp_).filename();
  if (!recursiveMkDir(path.c_str())) return;
  //   last_log_file_path_ = path;
  os_.open(path.c_str(), mode);
}

void LogWriter::close() { os_.close(); }

bool LogWriter::can_skip_write(int64_t timestamp, uint64_t stream_id) {
  return timestamp == last_timestamp_ &&
         streams_.find(stream_id) != streams_.end();
}

void LogWriter::write(int64_t timestamp, uint64_t stream_id, uint16_t datum) {
  // Need to handle various cases:
  // 1. Log file not yet initiated since process start
  // 2. Log file initiated, but timestamp falls outside its range
  // 3. Log file initiated, and timestamp in range, but not yet opened
  // 4. Log file initiated, timestamp in range, file opened
  if (timestamp < last_timestamp_ || timestamp > range_ceil_) {
    // Log file either not yet created after start, or the timestamp
    // falls outside its range.
    close();
    int ms_per_range_exp = resolution_ + kRangeLength;
    first_timestamp_ = timestamp;
    range_ceil_ = timestamp_ms_ceil(timestamp, ms_per_range_exp);
    streams_.clear();
    open(std::ios_base::out);
  } else {
    if (!os_.is_open()) {
      open(std::ios_base::app);
    }
  }

  if (timestamp != last_timestamp_) {
    last_timestamp_ = timestamp;
    streams_.clear();
    writeTimestamp(os_, timestamp);
  }
  if (streams_.insert(stream_id).second) {
    // Did not exist.
    writeDatum(os_, stream_id, datum);
  }
}

}  // namespace roo_monitoring