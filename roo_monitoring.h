#pragma once

#include <Arduino.h>
#include <fstream>
#include <set>

#include "roo_monitoring/common.h"
#include "roo_monitoring/log.h"

namespace roo_monitoring {

// Maps floating-point data in the application domain, to 16-bit unsigned
// integer data actually stored on disk.
class Transform {
 public:
  // Creates a linear transformation ax+b, for the specified multiplier (a) and
  // offset (b). The result is rounded to the nearest integer.
  static Transform Linear(float multiplier, float offset);

  // Creates a linear transformation, given the minimum and maximum
  // representable values. The minimum value will be mapped to 0, and the
  // maximum value will be mapped to 65535.
  static Transform LinearRange(float min_value, float max_value);

  // Applies the transformation to the specified input data. The result is
  // truncated to the [0, 65535] range.
  uint16_t apply(float value) const;

  // Recovers the application-domain data from the encoded uint16_t data.
  float unapply(uint16_t value) const;

  float multiplier() const { return multiplier_; }
  float offset() const { return offset_; }

 private:
  Transform(float multiplier, float offset)
      : multiplier_(multiplier), offset_(offset) {}

  float multiplier_;
  float offset_;
};

// Helper class to identify a specific file in the monitoring vault.
class VaultFileRef {
 public:
  // Creates a reference for a vault file that encloses the specified timestamp,
  // and has the specified resolution.
  static VaultFileRef Lookup(int64_t timestamp, int resolution);

  VaultFileRef() : timestamp_(0), resolution_(kTargetResolution) {}
  VaultFileRef(const VaultFileRef& other) = default;
  VaultFileRef& operator=(const VaultFileRef& other) = default;

  int64_t timestamp() const { return timestamp_; }
  int64_t timestamp_at(int position) const {
    return timestamp_ + time_steps(position);
  }
  int resolution() const { return resolution_; }

  int64_t time_step() const { return 1LL << (resolution_ << 1); }
  int64_t time_steps(int count) const {
    return (int64_t)count << (resolution_ << 1);
  }
  int64_t time_span() const {
    return 1LL << ((resolution_ + kRangeLength) << 1);
  }

  VaultFileRef parent() const { return Lookup(timestamp_, resolution_ + 1); }

  VaultFileRef child(int index) const {
    return VaultFileRef(timestamp_, resolution_ - 1).advance(index);
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
  VaultFileRef(int64_t timestamp, int resolution)
      : timestamp_(timestamp), resolution_(resolution) {}

  int64_t timestamp_;
  int resolution_;
};

std::ostream& operator<<(std::ostream& os, const VaultFileRef& file_ref);

// Represents a colletion of timeseries, using the same data mapping
// transformation. Should be used to group together timeseries that will usually
// be plotted together.
class Collection {
 public:
  Collection(String name);
  const String& name() const { return name_; }
  const Transform& transform() const { return transform_; }

  void getVaultFilePath(const VaultFileRef& ref, String* path) const;

 private:
  friend class Writer;
  friend class WriteTransaction;

  String name_;
  String base_dir_;
  Transform transform_;
};

class LogReader;
class LogFileReader;
class VaultWriter;

// Represents a write interface to the monitoring collection.
class Writer {
 public:
  Writer(Collection* collection);
  const Collection& collection() const { return *collection_; }

  bool Compact();

 private:
  bool CompactVault(VaultFileRef ref, int16_t index_begin, int16_t index_end,
                    bool hot);
  bool CompactVaultOneLevel(VaultFileRef ref, int16_t index_begin,
                            int16_t index_end, bool hot);

  Collection* collection_;
  String log_dir_;
  LogWriter writer_;
  friend class WriteTransaction;
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
  // void open(std::ios_base::openmode mode);
  // void close();
  const Transform* transform_;
  LogWriter* writer_;
};

// Represents a single data sample stored in a vault file.
class Sample {
 public:
  Sample(uint64_t stream_id, uint16_t avg_value, uint16_t min_value,
         uint16_t max_value, uint16_t fill)
      : stream_id_(stream_id),
        avg_value_(avg_value),
        min_value_(min_value),
        max_value_(max_value),
        fill_(fill) {}

  uint64_t stream_id() const { return stream_id_; }
  uint16_t avg_value() const { return avg_value_; }
  uint16_t min_value() const { return min_value_; }
  uint16_t max_value() const { return max_value_; }
  uint16_t fill() const { return fill_; }

 private:
  uint64_t stream_id_;
  uint16_t avg_value_;
  uint16_t min_value_;
  uint16_t max_value_;
  uint16_t fill_;  // 0x2000 = 100%.
};

// An 'iterator' class that allows to scan a single vault file sequentially.
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

// An 'iterator' class that allows to scan data from subsequent vault files,
// starting and ending at arbitrary timestamps. If the necessary vault files
// do not exist (e.g. because the scan goes into the future), empty samples
// are returned.
class VaultIterator {
 public:
  VaultIterator(const Collection* collection, int64_t start, int resolution);
  int64_t cursor() const;
  void next(std::vector<Sample>* sample);
  const VaultFileRef& vault_ref() const { return current_ref_; }

 private:
  const Collection* collection_;
  VaultFileRef current_ref_;
  VaultFileReader current_;
};

}  // namespace roo_monitoring