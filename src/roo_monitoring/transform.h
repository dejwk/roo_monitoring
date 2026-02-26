#pragma once

#include <stdint.h>

namespace roo_monitoring {

/// Maps application-domain floats to 16-bit stored values.
///
/// Currently implemented as a linear transformation.
class Transform {
 public:
  /// Creates a linear transformation ax+b.
  ///
  /// The result is rounded to the nearest integer.
  static Transform Linear(float multiplier, float offset);

  /// Creates a linear transform from min/max representable values.
  ///
  /// The minimum maps to 0 and the maximum maps to 65535.
  static Transform LinearRange(float min_value, float max_value);

  /// Applies the transform and clamps to [0, 65535].
  uint16_t apply(float value) const;

  /// Recovers the application-domain value from encoded data.
  float unapply(uint16_t value) const;

  /// Returns the multiplier used by the transform.
  float multiplier() const { return multiplier_; }
  /// Returns the offset used by the transform.
  float offset() const { return offset_; }

 private:
  Transform(float multiplier, float offset)
      : multiplier_(multiplier), offset_(offset) {}

  float multiplier_;
  float offset_;
};

}  // namespace roo_monitoring