#pragma once

#include <ostream>

#include "common.h"
#include "log.h"  // for LogCursor.
#include "roo_io/data/multipass_input_stream_reader.h"
#include "roo_logging.h"
#include "sample.h"

namespace roo_monitoring {

class Collection;

/// Identifies a specific file in the monitoring vault.
class VaultFileRef {
 public:
  /// Creates a reference that encloses the timestamp at the given resolution.
  static VaultFileRef Lookup(int64_t timestamp, Resolution resolution);

  VaultFileRef() : timestamp_(0), resolution_(kResolution_1024_ms) {}
  VaultFileRef(const VaultFileRef& other) = default;
  VaultFileRef& operator=(const VaultFileRef& other) = default;

  /// Returns the start timestamp for this vault file.
  int64_t timestamp() const { return timestamp_; }
  /// Returns the timestamp for the entry at the given position.
  int64_t timestamp_at(int position) const {
    return timestamp_ + time_steps(position);
  }
  /// Returns the resolution for this vault file.
  Resolution resolution() const { return resolution_; }

  /// Returns the time step between entries.
  int64_t time_step() const { return 1LL << (resolution_ << 1); }
  /// Returns the time step delta for the specified count.
  int64_t time_steps(int count) const {
    return (int64_t)count << (resolution_ << 1);
  }
  /// Returns the total time span covered by the file.
  int64_t time_span() const {
    return 1LL << ((resolution_ + kRangeLength) << 1);
  }

  /// Returns the parent vault file at the next coarser resolution.
  VaultFileRef parent() const {
    return Lookup(timestamp_, Resolution(resolution_ + 1));
  }

  /// Returns the child vault file at the next finer resolution.
  VaultFileRef child(int index) const {
    return VaultFileRef(timestamp_, Resolution(resolution_ - 1)).advance(index);
  }

  /// Returns the previous vault file at the same resolution.
  VaultFileRef prev() const {
    return VaultFileRef(timestamp_ - time_span(), resolution_);
  }

  /// Returns the next vault file at the same resolution.
  VaultFileRef next() const {
    return VaultFileRef(timestamp_ + time_span(), resolution_);
  }

  /// Returns the vault file advanced by n spans.
  VaultFileRef advance(int n) const {
    return VaultFileRef(timestamp_ + n * time_span(), resolution_);
  }

  /// Returns the index of this file within its parent range.
  int sibling_index() const {
    return (timestamp_ >> ((resolution_ + kRangeLength) << 1)) & 0x3;
  }

 private:
  VaultFileRef(int64_t timestamp, Resolution resolution)
      : timestamp_(timestamp), resolution_(resolution) {}

  int64_t timestamp_;
  Resolution resolution_;
};

/// Writes a human-readable representation of the vault file reference.
roo_logging::Stream& operator<<(roo_logging::Stream& os,
                                const VaultFileRef& file_ref);

/// Sequential reader for a single vault file.
///
/// A single vault file has the following format:
///
/// header:
///   major version (uint8): currently always 1
///   minor version (uint8): currently always 1
/// entry[]:
///   sample count (varint)
///   sample[]:
///     stream ID (varint)
///     avg       (uint16)
///     min       (uint16)
///     max       (uint16)
///     fill      (uint16)
///
/// The file name of the vault file implies the start timestamp.
/// The level implies the time resolution.
/// The finished vault always has 256 entries.
class VaultFileReader {
 public:
  /// Creates a reader bound to the specified collection.
  VaultFileReader(const Collection* collection);
  // VaultFileReader(VaultFileReader&& other) = default;
  // VaultFileReader(const VaultFileReader& other) = delete;
  // VaultFileReader& operator=(VaultFileReader&& other);

  /// Opens the file and seeks to the specified index and byte offset.
  bool open(const VaultFileRef& ref, int index, int64_t offset);
  /// Returns true if a file is currently open.
  bool is_open() const { return reader_.isOpen(); }

  /// Closes the reader.
  void close() { reader_.close(); }

  /// Advances the cursor to the first entry at or after the timestamp.
  void seekForward(int64_t timestamp);
  /// Reads the next entry and fills the sample vector.
  bool next(std::vector<Sample>* sample);
  /// Returns the current entry index.
  int index() const { return index_; }
  /// Returns true if the reader has passed the end of file.
  bool past_eof() const;

  /// Returns true if the file is either good or does not exist.
  ///
  /// If open fails for any reason other than not found, or if read fails,
  /// this returns false.
  bool ok() const {
    return reader_.status() == roo_io::kOk ||
           reader_.status() == roo_io::kNotFound;
  }

  /// Returns the current reader status.
  roo_io::Status status() const { return reader_.status(); }

  /// Returns the vault file reference for this reader.
  const VaultFileRef& vault_ref() const { return ref_; }

  /// Returns the current log cursor.
  LogCursor tell();

  ~VaultFileReader();

 private:
  const Collection* collection_;
  VaultFileRef ref_;
  roo_io::Mount fs_;
  roo_io::MultipassInputStreamReader reader_;
  int index_;
  int position_;
};

}  // namespace roo_monitoring
