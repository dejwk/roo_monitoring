#pragma once

#include <Arduino.h>
#include <stdint.h>

#include <vector>

#include "roo_io/fs/filesystem.h"

namespace roo_monitoring {

/// Number of base-4 digits used per range.
///
/// 4^4 = 256 items per range.
static const int kRangeLength = 4;
/// Number of items in a range (4^(kRangeLength)).
static const int kRangeElementCount = 1 << (kRangeLength << 1);

/// Base directory for monitoring storage on the filesystem.
extern const char* kMonitoringBasePath;
/// Subdirectory name used for raw log files.
extern const char* kLogSubPath;

/// Converts a 0-15 value to an uppercase hex digit.
static char toHexDigit(int d) { return (d < 10) ? d + '0' : d - 10 + 'A'; }

/// Returns a path formed by joining the base directory and subdirectory.
String subdir(String base, const String& sub);
/// Returns a file path for the given directory and timestamp-like value.
String filepath(String dir, int64_t file);

/// Lists timestamp-named files in the directory and returns their timestamps.
///
/// The timestamps are in milliseconds since Epoch and sorted ascending.
std::vector<int64_t> listFiles(roo_io::Mount& fs, const char* dirname);

/// Helper class for generating filenames corresponding to timestamps.
class Filename {
 public:
  /// Creates a filename for the specified timestamp.
  static Filename forTimestamp(int64_t nanosSinceEpoch);
  /// Returns the generated filename as a null-terminated string.
  const char* filename() const { return data_; }

 private:
  Filename() {}
  char data_[13];
};

}  // namespace roo_monitoring