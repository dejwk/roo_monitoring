#pragma once

#include <ostream>

#include "common.h"
#include "datastream.h"
#include "log.h"  // for LogCursor.
#include "sample.h"

#include "roo_logging.h"

namespace roo_monitoring {

class Collection;

// Helper class to identify a specific file in the monitoring vault.
class VaultFileRef {
 public:
  // Creates a reference for a vault file that encloses the specified timestamp,
  // and has the specified resolution.
  static VaultFileRef Lookup(int64_t timestamp, Resolution resolution);

  VaultFileRef() : timestamp_(0), resolution_(kResolution_1024_ms) {}
  VaultFileRef(const VaultFileRef& other) = default;
  VaultFileRef& operator=(const VaultFileRef& other) = default;

  int64_t timestamp() const { return timestamp_; }
  int64_t timestamp_at(int position) const {
    return timestamp_ + time_steps(position);
  }
  Resolution resolution() const { return resolution_; }

  int64_t time_step() const { return 1LL << (resolution_ << 1); }
  int64_t time_steps(int count) const {
    return (int64_t)count << (resolution_ << 1);
  }
  int64_t time_span() const {
    return 1LL << ((resolution_ + kRangeLength) << 1);
  }

  VaultFileRef parent() const {
    return Lookup(timestamp_, Resolution(resolution_ + 1));
  }

  VaultFileRef child(int index) const {
    return VaultFileRef(timestamp_, Resolution(resolution_ - 1)).advance(index);
  }

  VaultFileRef prev() const {
    return VaultFileRef(timestamp_ - time_span(), resolution_);
  }

  VaultFileRef next() const {
    return VaultFileRef(timestamp_ + time_span(), resolution_);
  }

  VaultFileRef advance(int n) const {
    return VaultFileRef(timestamp_ + n * time_span(), resolution_);
  }

  int sibling_index() const {
    return (timestamp_ >> ((resolution_ + kRangeLength) << 1)) & 0x3;
  }

 private:
  VaultFileRef(int64_t timestamp, Resolution resolution)
      : timestamp_(timestamp), resolution_(resolution) {}

  int64_t timestamp_;
  Resolution resolution_;
};

roo_logging::Stream& operator<<(roo_logging::Stream& os,
                                const VaultFileRef& file_ref);

// An 'iterator' class that allows to scan a single vault file sequentially.
//
// A single vault file has the following format:
//
// header:
//   major version (uint8): currently always 1
//   minor version (uint8): currently always 1
// entry[]:
//   sample count (varint)
//   sample[]:
//     stream ID (varint)
//     avg       (uint16)
//     min       (uint16)
//     max       (uint16)
//     fill      (uint16)
//
// The file name of the vault file implies the 'start timestamp'.
// The 'level' of the vault implies the time resolution.
// The finished vault always has 256 entries.
class VaultFileReader {
 public:
  VaultFileReader(const Collection* collection);
  // VaultFileReader(VaultFileReader&& other) = default;
  // VaultFileReader(const VaultFileReader& other) = delete;
  // VaultFileReader& operator=(VaultFileReader&& other);

  bool open(const VaultFileRef& ref, int index, int64_t offset);
  bool is_open() const { return file_.is_open(); }
  void close() {
    if (file_.is_open()) file_.close();
  }

  void seekForward(int64_t timestamp);
  bool next(std::vector<Sample>* sample);
  int index() const { return index_; }
  bool past_eof() const;

  // Make sure that even if the file hasn't been open because it did not
  // existed, we consider it 'good'. If open fails for any other reason than
  // 'does not exist', or if read fails for any reason, my_errno() will
  // return a non-zero value.
  bool good() const { return my_errno() == 0; }
  int my_errno() const {
    int result = file_.my_errno();
    return (result == ENOENT ? 0 : result);
  }

  const char* status() { return strerror(my_errno()); }

  const VaultFileRef& vault_ref() const { return ref_; }

  LogCursor tell();

  ~VaultFileReader();

 private:
  const Collection* collection_;
  VaultFileRef ref_;
  DataInputStream file_;
  int index_;
  int position_;
};

}  // namespace roo_monitoring
