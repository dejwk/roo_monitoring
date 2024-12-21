#include "log.h"

#include <Arduino.h>

#include <algorithm>

#include "common.h"
#include "roo_io/fs/fsutil.h"
#include "roo_logging.h"

#ifndef MLOG_roo_monitoring_compaction
#define MLOG_roo_monitoring_compaction 0
#endif

#ifndef MLOG_roo_logging_writer
#define MLOG_roo_logging_writer 0
#endif

namespace roo_monitoring {

enum Code { CODE_ERROR = 0, CODE_TIMESTAMP = 1, CODE_DATUM = 2 };

bool LogFileReader::open(const char* path, int64_t checkpoint) {
  MLOG(roo_monitoring_compaction)
      << "Opening log file " << path << " at " << checkpoint;
  reader_.reset(fs_.fopen(path));
  if (!reader_.isOpen()) {
    LOG(ERROR) << "Failed to open log file " << path << ": "
               << roo_io::StatusAsString(reader_.status());
    return false;
  }
  if (checkpoint > 0) {
    reader_.seek(checkpoint);
    if (!reader_.ok()) {
      LOG(ERROR) << "Failed to seek in the log file " << path << ": "
                 << roo_io::StatusAsString(reader_.status());
      return false;
    }
  }
  checkpoint_ = checkpoint;
  lookahead_entry_type_ = reader_.readU8();
  return true;
}

bool LogFileReader::next(int64_t* timestamp, std::vector<LogSample>* data,
                         bool is_hot) {
  data->clear();
  if (checkpoint_ < 0) return false;
  if (!reader_.ok()) return false;
  if (lookahead_entry_type_ != CODE_TIMESTAMP) {
    LOG(ERROR) << "Unexpected content in the log file: "
               << (int)lookahead_entry_type_;
    return false;
  }
  *timestamp = reader_.readVarU64();
  lookahead_entry_type_ = reader_.readU8();
  if (!reader_.ok()) return false;

  // LOG(INFO) << "Read log data at timestamp: " << roo_logging::hex <<
  // *timestamp;
  while (true) {
    if (reader_.status() == roo_io::kEndOfStream) {
      if (is_hot) {
        // Indicate that the data isn't complete yet. Not updating the
        // source checkpoint in this case.
        return false;
      } else {
        // Indicating that we have reached the end of a 'historical' log file.
        MLOG(roo_monitoring_compaction)
            << "Reached EOF of a historical log file ";
        checkpoint_ = -1;
        break;
      }
    } else if (!reader_.ok()) {
      // Must be I/O error.
      LOG(ERROR) << "Failed to read timestamped data from a log file: "
                 << roo_io::StatusAsString(reader_.status());
      return false;
    } else if (lookahead_entry_type_ == CODE_DATUM) {
      uint64_t stream_id;
      uint16_t datum;
      stream_id = reader_.readVarU64();
      datum = reader_.readBeU16();
      if (!reader_.ok()) return false;
      data->emplace_back(stream_id, datum);
      lookahead_entry_type_ = reader_.readU8();
      continue;
    } else if (lookahead_entry_type_ == CODE_TIMESTAMP) {
      checkpoint_ = (int64_t)reader_.position() - 1;
      break;
    } else {
      LOG(ERROR) << "Unexpected entry type " << (int)lookahead_entry_type_;
      return false;
    }
  }
  std::sort(data->begin(), data->end());
  return true;
}

std::vector<int64_t> CachedLogDir::list() {
  sync();
  std::vector<int64_t> result;
  for (int64_t e : entries_) {
    result.push_back(e);
  }
  std::sort(result.begin(), result.end());
  return result;
}

void CachedLogDir::sync() {
  if (synced_) return;
  entries_.clear();
  roo_io::Mount fs = fs_.mount();
  if (!fs.ok()) return;
  std::vector<int64_t> entries = listFiles(fs, log_dir_);
  for (int64_t e : entries) {
    entries_.insert(e);
  }
  synced_ = true;
}

LogReader::LogReader(roo_io::Mount& fs, const char* log_dir,
                     CachedLogDir& cache, Resolution resolution,
                     int64_t hot_file)
    : fs_(fs),
      log_dir_(log_dir),
      cache_(cache),
      resolution_(resolution),
      entries_(cache_.list()),
      group_begin_(entries_.begin()),
      cursor_(entries_.begin()),
      group_end_(entries_.begin()),
      hot_file_(hot_file >= 0      ? hot_file
                : entries_.empty() ? 0
                                   : entries_.back()),
      reached_hot_file_(false),
      range_floor_(0),
      range_ceil_(0),
      reader_(fs) {
  // Ensure that the hot file is at the end of the list, even if it is not, for
  // some reason, chronologically the newest. This way, we will always delete
  // the non-hot logs and leave the hot file be.
}

bool LogReader::nextRange() {
  if (group_end_ == entries_.end() || reached_hot_file_) {
    MLOG(roo_monitoring_compaction) << "No more log files to process.";
    return false;
  }
  cursor_ = group_begin_ = group_end_;
  Resolution range_resolution = Resolution(resolution_ + kRangeLength);
  range_floor_ = timestamp_ms_floor(*cursor_, range_resolution);
  range_ceil_ = timestamp_ms_ceil(*cursor_, range_resolution);
  while (!reached_hot_file_ && group_end_ != entries_.end() &&
         *group_end_ <= range_ceil_) {
    if (*group_end_ == hot_file_) {
      reached_hot_file_ = true;
    }
    ++group_end_;
  }
  MLOG(roo_monitoring_compaction)
      << "Processing log files for the range starting at " << roo_logging::hex
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
    LOG(WARNING) << "Seek failed; file not found: " << roo_logging::hex
                 << cursor.file();
    return false;
  }
  if (!reader_.open(filepath(log_dir_, cursor.file()).c_str(),
                    cursor.position())) {
    LOG(WARNING) << "Seek failed; could not open: " << roo_logging::hex
                 << cursor.file();
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
    MLOG(roo_monitoring_compaction)
        << "Removing processed log file " << roo_logging::hex << *i;
    if (fs_.remove(filepath(log_dir_, *i).c_str()) != roo_io::kOk) {
      LOG(ERROR) << "Failed to remove processed log file " << roo_logging::hex
                 << *i;
    }
    cache_.erase(*i);
  }
}

LogWriter::LogWriter(roo_io::Filesystem& fs, const char* log_dir,
                     CachedLogDir& cache, Resolution resolution)
    : log_dir_(log_dir),
      cache_(cache),
      resolution_(resolution),
      fs_(fs),
      mount_(),
      first_timestamp_(-1),
      last_timestamp_(-1),
      range_ceil_(-1) {}

void writeTimestamp(roo_io::OutputStreamWriter& writer, int64_t timestamp) {
  writer.writeU8(CODE_TIMESTAMP);
  writer.writeVarU64(timestamp);
}

void writeDatum(roo_io::OutputStreamWriter& writer, uint64_t stream_id,
                uint16_t transformed_datum) {
  writer.writeU8(CODE_DATUM);
  writer.writeVarU64(stream_id);
  writer.writeBeU16(transformed_datum);
  // write_float(file, datum);
}

void LogWriter::open(roo_io::FileUpdatePolicy update_policy) {
  String path = log_dir_;
  path += "/";
  path += Filename::forTimestamp(first_timestamp_).filename();
  mount_ = fs_.mount();
  roo_io::Status status = roo_io::MkParentDirRecursively(mount_, path.c_str());
  if (status != roo_io::kOk && status != roo_io::kDirectoryExists) return;
  //   last_log_file_path_ = path;
  writer_.reset(mount_.fopenForWrite(path.c_str(), update_policy));
  cache_.insert(first_timestamp_);
}

void LogWriter::close() {
  writer_.close();
  mount_.close();
}

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
    Resolution range_resolution = Resolution(resolution_ + kRangeLength);
    first_timestamp_ = timestamp;
    range_ceil_ = timestamp_ms_ceil(timestamp, range_resolution);
    streams_.clear();
    open(roo_io::kFailIfExists);
  } else {
    if (!writer_.ok()) {
      open(roo_io::kAppendIfExists);
    }
  }

  if (timestamp != last_timestamp_) {
    last_timestamp_ = timestamp;
    streams_.clear();
    writeTimestamp(writer_, timestamp);
  }
  if (streams_.insert(stream_id).second) {
    // Did not exist.
    writeDatum(writer_, stream_id, datum);
  }
}

}  // namespace roo_monitoring