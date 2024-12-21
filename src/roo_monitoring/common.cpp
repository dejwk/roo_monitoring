#include "common.h"

#include <algorithm>
#include <memory>

#include "roo_logging.h"

namespace roo_monitoring {

const char* kMonitoringBasePath = "/monitoring";
const char* kLogSubPath = "log";

String subdir(String base, const String& sub) {
  base += '/';
  base += sub;
  return base;
}

String filepath(String dir, int64_t file) {
  dir += "/";
  dir += Filename::forTimestamp(file).filename();
  return dir;
}

namespace {

static uint8_t fromHexDigit(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  return 0;
}

static int64_t decodeHex(const char* filename) {
  int64_t result = 0;
  for (const char* c = filename; *c != 0; ++c) {
    result <<= 4;
    result |= (fromHexDigit(*c));
  }
  return result;
}

}  // namespace

std::vector<int64_t> listFiles(roo_io::Mount& fs, const char* dirname) {
  std::vector<int64_t> result;
  roo_io::Directory dir = fs.opendir(dirname);
  if (!dir.isOpen()) {
    LOG(WARNING) << "Failed to open directory " << dirname << ": "
                 << roo_io::StatusAsString(dir.status());
    return result;
  }
  while (dir.read()) {
    if (dir.entry().isDirectory()) {
      // Skip "." and ".."
      continue;
    }
    if (strlen(dir.entry().name()) != 12) {
      // Skip .cursor files, and other auxiliary files.
      continue;
    }
    result.push_back(decodeHex(dir.entry().name()));
  }
  std::sort(result.begin(), result.end());
  dir.close();
  return result;
}

Filename Filename::forTimestamp(int64_t timestamp_ms) {
  Filename filename;
  for (int i = 11; i >= 0; --i) {
    filename.data_[i] = toHexDigit(timestamp_ms & 0xF);
    timestamp_ms >>= 4;
  }
  filename.data_[12] = 0;
  return filename;
}

}  // namespace roo_monitoring