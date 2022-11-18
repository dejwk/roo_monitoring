#pragma once

#include <stdint.h>
#include <vector>

#include <Arduino.h>

namespace roo_monitoring {

static const int kRangeLength = 4;       // 4^4 = 256 items per range.
static const int kRangeElementCount = 1 << (kRangeLength << 1);

extern const char* kMonitoringBasePath;
extern const char* kLogSubPath;

static char toHexDigit(int d) { return (d < 10) ? d + '0' : d - 10 + 'A'; }

bool isdir(const char* path);
bool recursiveMkDir(const char* path);

String subdir(String base, const String& sub);
String filepath(String dir, int64_t file);

// Lists 'timestamp-named' files in the specified dir, and returns a vector of
// uint64 corresponding to the timestamps (in ms since Epoch), sorted by
// timestamp.
std::vector<int64_t> listFiles(const char* dirname);

// Helper class for generating filenames corresponding to timestamps.
class Filename {
 public:
  static Filename forTimestamp(int64_t nanosSinceEpoch);
  const char* filename() const { return data_; }

 private:
  Filename() {}
  char data_[13];
};

}  // namespace roo_monitoring