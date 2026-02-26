#pragma once

/// Umbrella header for the roo_monitoring module.
///
/// Provides data collection, transformation, and vault APIs.

#include <Arduino.h>

#include <set>

#include "roo_io/fs/filesystem.h"
#include "roo_monitoring/common.h"
#include "roo_monitoring/log.h"
#include "roo_monitoring/resolution.h"
#include "roo_monitoring/sample.h"
#include "roo_monitoring/transform.h"
#include "roo_monitoring/vault.h"

namespace roo_monitoring {

/// Collection of timeseries sharing transform and source resolution.
///
/// Group streams that are commonly queried/plotted together.
class Collection {
 public:
  Collection(roo_io::Filesystem& fs, String name,
             Resolution resolution = kResolution_1024_ms);

  roo_io::Filesystem& fs() const { return fs_; }
  const String& name() const { return name_; }
  Resolution resolution() const { return resolution_; }
  const Transform& transform() const { return transform_; }

  void getVaultFilePath(const VaultFileRef& ref, String* path) const;

 private:
  friend class Writer;
  friend class WriteTransaction;

  roo_io::Filesystem& fs_;
  String name_;
  String base_dir_;
  Resolution resolution_;
  Transform transform_;
};

class LogReader;
class LogFileReader;
class VaultWriter;

/// Write interface for a monitoring collection.
class Writer {
 public:
  enum Status { OK, IN_PROGRESS, FAILED };

  enum IoState { IOSTATE_OK, IOSTATE_ERROR };

  Writer(Collection* collection);

  const Collection& collection() const { return *collection_; }

  /// Periodically flushes logged data into vault files.
  void flushAll();

  IoState io_state() const { return io_state_; }

  void flushSome();

  bool isFlushInProgress() { return flush_in_progress_; }

 private:
  friend class WriteTransaction;

  /// Writes logs to vault and returns past-end index written.
  int16_t writeToVault(roo_io::Mount& fs, LogReader& reader, VaultFileRef ref);

  Status compactVaultOneLevel();

  Collection* collection_;
  String log_dir_;
  CachedLogDir cache_;
  LogWriter writer_;
  IoState io_state_;

  VaultFileRef compaction_head_;
  int16_t compaction_head_index_end_;
  bool is_hot_range_;

  bool flush_in_progress_;
};

/// Represents a single write operation to a monitoring collection.
///
/// Intended as a transient RAII object; commit happens on destruction.
class WriteTransaction {
 public:
  WriteTransaction(Writer* writer);
  ~WriteTransaction();

  void write(int64_t timestamp, uint64_t stream_id, float data);

 private:
  const Transform* transform_;
  LogWriter* writer_;
};

/// Iterator that scans monitoring data at a given resolution.
///
/// Starts at a specified timestamp and reads across vault files. Missing vault
/// ranges yield empty samples.
class VaultIterator {
 public:
  /// Creates iterator over `collection` at `resolution`, starting at `start`.
  ///
  /// Start timestamp is rounded down to resolution boundary.
  VaultIterator(const Collection* collection, int64_t start,
                Resolution resolution);

  /// Returns current iterator timestamp.
  int64_t cursor() const;

  /// Advances by one resolution step and fills `sample`.
  void next(std::vector<Sample>* sample);

 private:
  const Collection* collection_;
  VaultFileRef current_ref_;
  VaultFileReader current_;
};

}  // namespace roo_monitoring