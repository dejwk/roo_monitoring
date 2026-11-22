#pragma once

/// Umbrella header for the roo_monitoring module.
///
/// Provides data collection, transformation, and vault APIs.

#include <Arduino.h>

#include <set>

#include "roo_io.h"
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
  /// Creates a collection rooted in `fs` under the monitoring name `name`.
  Collection(roo_io::Filesystem& fs, String name,
             Resolution resolution = kResolution_1024_ms);

  /// Returns the filesystem that stores this collection.
  roo_io::Filesystem& fs() const { return fs_; }
  /// Returns the collection name.
  const String& name() const { return name_; }
  /// Returns the base log/vault resolution used for raw samples.
  Resolution resolution() const { return resolution_; }
  /// Returns the value transform applied when encoding float samples.
  const Transform& transform() const { return transform_; }

  /// Returns the exclusive base-bucket boundary of the last successful
  /// compaction through all resolutions, or -1 before one completes in this
  /// instance. Round down to the requested resolution for complete buckets.
  /// Append-only readers may cache earlier buckets. This is a data timestamp,
  /// not wall time; it does not advance when collection/compaction stops.
  /// Access must be serialized with Writer::flushSome()/flushAll().
  int64_t compactedUntil() const { return compacted_until_; }

  /// Writes the absolute vault file path for `ref` into `path`.
  void getVaultFilePath(const VaultFileRef& ref, String* path) const;

 private:
  friend class Writer;
  friend class WriteTransaction;

  roo_io::Filesystem& fs_;
  String name_;
  String base_dir_;
  Resolution resolution_;
  Transform transform_;
  int64_t compacted_until_ = -1;
};

class LogReader;
class LogFileReader;
class VaultWriter;

/// Write interface for a monitoring collection.
class Writer {
 public:
  /// Result of a flush or compaction step.
  enum Status {
    OK,           ///< The requested operation completed successfully.
    IN_PROGRESS,  ///< Additional calls are required to finish the operation.
    FAILED        ///< The operation failed due to an I/O or format error.
  };

  /// Persistent health of the writer's filesystem I/O.
  enum IoState {
    IOSTATE_OK,    ///< No I/O error has been observed.
    IOSTATE_ERROR  ///< At least one write/read operation has failed.
  };

  /// Creates a writer for the specified monitoring collection.
  Writer(Collection* collection);

  /// Returns the collection managed by this writer.
  const Collection& collection() const { return *collection_; }

  /// Flushes pending log data and runs compaction until no more work remains.
  void flushAll();

  /// Returns the last observed I/O health state.
  IoState io_state() const { return io_state_; }

  /// Performs a bounded amount of flush/compaction work.
  /// After an I/O failure, use a new writer to retry compaction.
  void flushSome();

  /// Returns true while a multi-step flush is still in progress.
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
  bool compacting_historical_range_;

  bool flush_in_progress_;
  int64_t pending_compacted_until_ = -1;
};

/// Represents a single write operation to a monitoring collection.
///
/// Intended as a transient RAII object; commit happens on destruction.
class WriteTransaction {
 public:
  /// Starts a write transaction that appends samples through `writer`.
  WriteTransaction(Writer* writer);
  /// Closes the underlying log writer, committing the transaction.
  ~WriteTransaction();

  /// Appends one sample at `timestamp` for `stream_id` with float `data`.
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
