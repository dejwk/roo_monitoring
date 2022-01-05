#include "glog/logging.h"

#include "common.h"

#include <dirent.h>
#include <stdio.h>
#include <sys/stat.h>
#include <algorithm>
#include <memory>

namespace roo_monitoring {

const char* kMonitoringBasePath = "/sd/monitoring";
const char* kLogSubPath = "log";

bool isdir(const char* path) {
  struct stat s;
  if (stat(path, &s) == 0) {
    return (s.st_mode & S_IFDIR);
  } else {
    return false;
  }
}

namespace {

bool doMkDir(const char* path) {
  int result = mkdir(path, S_IRWXU);
  if (result == 0) {
    LOG(INFO) << "Directory " << path << " created.";
    return true;
  }
  if (errno == EEXIST) {
    errno = 0;
    // LOG(INFO) << "Directory " << path << " not created, because it exists.";
  } else {
    LOG(ERROR) << "Failed to create directory " << path << ": "
               << strerror(errno);
    return false;
  }
  return true;
}

}  // namespace

bool recursiveMkDir(const char* path) {
  size_t len = strlen(path);
  std::unique_ptr<char[]> tmp(new char[len + 1]);
  memcpy(tmp.get(), path, len + 1);
  for (char* p = tmp.get() + 1; *p; p++)
    if (*p == '/') {
      *p = 0;
      if (!doMkDir(tmp.get())) return false;
      *p = '/';
    }
  if (tmp[len - 1] == '/') {
    tmp[len - 1] = 0;
    if (!doMkDir(tmp.get())) return false;
  }
  return true;
}

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

std::vector<int64_t> listFiles(const char* dirname) {
  std::vector<int64_t> result;

  DIR* dir = opendir(dirname);
  if (dir == nullptr) {
    LOG(WARNING) << "Failed to open directory " << dirname << ": "
                 << strerror(errno);
    return result;
  }
  struct dirent* ent;
  while ((ent = readdir(dir)) != nullptr) {
    if (ent->d_type == DT_DIR) {
      // Skip "." and ".."
      continue;
    }
    if (strlen(ent->d_name) != 12) {
      // Skip .cursor files, and other auxiliary files.
      continue;
    }
    result.push_back(decodeHex(ent->d_name));
  }
  std::sort(result.begin(), result.end());
  closedir(dir);
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