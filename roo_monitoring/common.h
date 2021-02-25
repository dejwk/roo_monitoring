#pragma once

#include <stdint.h>
#include <vector>

#include <Arduino.h>

namespace roo_monitoring {

static const int kRangeLength = 4;       // 4^4 = 256 items per range.
static const int kTargetResolution = 5;  // 4^5 = 1024ms ~= 1s
static const int kMaxResolution = 18;  // 4^18 ~= 2 years
static const int kRangeElementCount = 1 << (kRangeLength << 1);

static const int kInterpolationResolution = 8;  // 4^8 = 65.536s = 1.09 min

extern const char* kMonitoringBasePath;
extern const char* kLogSubPath;

static char toHexDigit(int d) { return (d < 10) ? d + '0' : d - 10 + 'A'; }

static int64_t timestamp_ms_floor(int64_t timestamp_ms, int resolution) {
  // Resolution is the exponent with base 4, so we need to multiply
  // by 2 when converting to base 2. Then, using shift to generate
  // the required number of zeros in the mask.
  return timestamp_ms & (0xFFFFFFFFFFFFFFFFLL << (resolution << 1));
}

static int64_t timestamp_ms_ceil(int64_t timestamp_ms, int resolution) {
  // Like the above, but mask is negated (so it has the requested count
  // of trailing 1s) and ORed with the timestamp.
  return timestamp_ms | ~(0xFFFFFFFFFFFFFFFFLL << (resolution << 1));
}

static int64_t timestamp_increment(int64_t steps, int resolution) {
  return steps << (resolution << 1);
}

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