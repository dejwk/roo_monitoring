#pragma once

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

// Represents a collection of timeseries, using the same data mapping
// transformation and source resolution. Should be used to group together
// timeseries that will usually be plotted together.
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

// Represents a write interface to the monitoring collection.
class Writer {
 public:
  enum Status { OK, IN_PROGRESS, FAILED };

  enum IoState { IOSTATE_OK, IOSTATE_ERROR };

  Writer(Collection* collection);

  const Collection& collection() const { return *collection_; }

  // Needs to be called periodically in order to actually incorporate the
  // logged data into the vault.
  void flushAll();

  IoState io_state() const { return io_state_; }

  void flushSome();

  bool isFlushInProgress() { return flush_in_progress_; }

 private:
  friend class WriteTransaction;

  // Returns the index past written to the vault.
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

// Represents a single write operation to the monitoring collection. Should be
// created as a transient object, as the write commences only when the
// transaction is destructed.
class WriteTransaction {
 public:
  WriteTransaction(Writer* writer);
  ~WriteTransaction();

  void write(int64_t timestamp, uint64_t stream_id, float data);

 private:
  const Transform* transform_;
  LogWriter* writer_;
};

// An 'iterator' class that allows to scan the collected monitoring data
// at a specified resolution, starting at a specified timestamp.
//
// The implementation reads data from subsequent vault files. If the necessary
// vault files do not exist (e.g. because the scan goes into the future), empty
// samples are returned.
class VaultIterator {
 public:
  // Creates the iterator over a specified collection, at the specified
  // resolution, starting at the specified timestamp (rounded down to align with
  // the resolution).
  VaultIterator(const Collection* collection, int64_t start,
                Resolution resolution);

  // Returns the current timestamp that the iterator is pointed at.
  int64_t cursor() const;

  // Advances the iterator by the time step implied by the resolution, filling
  // up the specified sample.
  void next(std::vector<Sample>* sample);

 private:
  const Collection* collection_;
  VaultFileRef current_ref_;
  VaultFileReader current_;
};

}  // namespace roo_monitoring