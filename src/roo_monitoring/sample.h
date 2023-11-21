#pragma once

namespace roo_monitoring {

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

}  // namespace roo_monitoring
