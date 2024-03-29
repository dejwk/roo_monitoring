#pragma once

#include <fstream>
#include <cstring>
#include "stdint.h"

namespace roo_monitoring {

class DataInputStream {
 public:
  DataInputStream() : is_(), error_(0), read_error_(0) {}
  DataInputStream(const char* filename,
                  std::ios_base::openmode mode = std::ios_base::in) {
    open(filename, mode);
  }

  void seekg(std::streampos pos);
  std::streampos tellg();

  bool good() const { return is_.good(); }
  bool eof() const { return is_.eof(); }
  bool bad() const { return is_.bad(); }

  int my_errno() const { return error_; }

  void open(const char* filename,
            std::ios_base::openmode mode = std::ios_base::in) {
    error_ = 0;
    is_.open(filename, mode);
    if (!is_.good()) {
      error_ = errno;
    }
  }

  bool is_open() const { return is_.is_open(); }

  void close() { is_.close(); }

  void clear_error() {
    is_.clear();
    error_ = 0;
  }

  uint8_t read_uint8();
  uint16_t read_uint16();
  uint64_t read_varint();

  uint8_t peek_uint8();

  const char* status() {
    if (error_ != 0) return strerror(error_);
    if (good()) return "OK";
    if (eof()) return "EOF";
    return "Unknown, not good";
  }

 private:
  inline void update_status(bool force = false) {
    if (!is_.good() && (force || error_ == 0)) error_ = errno;
  }

  std::ifstream is_;
  int error_;
  int read_error_;
};

class DataOutputStream {
 public:
  DataOutputStream() : os_(), error_(0) {}

  DataOutputStream(const char* filename,
                   std::ios_base::openmode mode = std::ios_base::out) {
    open(filename, mode);
  }

  void open(const char* filename,
            std::ios_base::openmode mode = std::ios_base::out) {
    os_.open(filename, mode);
    error_ = errno;
  }

  std::ios_base::io_state rdstate() const { return os_.rdstate(); }

  void write(const char* data, size_t size);
  void write_uint8(uint8_t d);
  void write_uint16(uint16_t d);
  void write_varint(uint64_t data);

  bool good() const { return os_.good(); }
  bool eof() const { return os_.eof(); }
  bool bad() const { return os_.bad(); }

  bool is_open() const { return os_.is_open(); }
  void close() { os_.close(); }

  int my_errno() const { return error_; }

  const char* status() {
    if (good()) return "OK";
    if (eof()) return "EOF";
    return strerror(error_);
  }

 private:
  inline void update_status(bool force = false) {
    if (force || error_ == 0) error_ = errno;
  }

  std::ofstream os_;
  int error_;
};

}  // namespace roo_monitoring