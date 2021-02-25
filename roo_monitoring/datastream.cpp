#include "datastream.h"

namespace roo_monitoring {

uint8_t DataInputStream::read_uint8() {
  uint8_t result = is_.get();
  update_status();
  return result;
}

uint8_t DataInputStream::peek_uint8() {
  uint8_t result = is_.peek();
  update_status();
  return result;
}

uint16_t DataInputStream::read_uint16() {
  uint8_t hi = read_uint8();
  uint8_t lo = read_uint8();
  return hi << 8 | lo;
}

uint64_t DataInputStream::read_varint() {
  uint64_t result = 0;
  int read;
  int shift = 0;
  do {
    read = is_.get();
    if (read < 0) {
      update_status();
      return 0;
    }
    result |= ((uint64_t)(read & 0x7F) << shift);
    shift += 7;
  } while ((read & 0x80) != 0);
  return result;
}

void DataOutputStream::write(const char* data, size_t size) {
  os_.write(data, size); 
  update_status();
}

void DataOutputStream::write_uint8(uint8_t d) {
  write((const char*)&d, 1);
}

void DataOutputStream::write_uint16(uint16_t d) {
  write_uint8((d >> 8) & 0xFF);
  write_uint8((d >> 0) & 0xFF);
}

void DataOutputStream::write_varint(uint64_t data) {
  char buffer[10];
  if (data <= 0x7F) {
    // Fast-path and special-case for data == 0.
    write_uint8(data);
    return;
  }

  size_t size = 0;
  while (data > 0) {
    buffer[size++] = (uint8_t)((data & 0x7F) | 0x80);
    data >>= 7;
  }
  buffer[size - 1] &= 0x7F;
  os_.write(buffer, size);
  update_status();
}

}  // namespace roo_monitoring