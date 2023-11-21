#include "transform.h"

namespace roo_monitoring {

Transform Transform::Linear(float multiplier, float offset) {
  return Transform(multiplier, offset);
}

Transform Transform::LinearRange(float min_value, float max_value) {
  return Transform(65535.0 / (max_value - min_value), -min_value);
}

uint16_t Transform::apply(float value) const {
  float transformed = multiplier_ * value + offset_;
  if (transformed < 0) {
    return 0;
  }
  if (transformed >= 65535) {
    return 65535;
  }
  return (uint16_t)(transformed + 0.5);
}

float Transform::unapply(uint16_t value) const {
  return (value - offset_) / multiplier_;
}

}  // namespace roo_monitoring
