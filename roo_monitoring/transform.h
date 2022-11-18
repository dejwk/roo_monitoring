#pragma once

#include <stdint.h>

namespace roo_monitoring {

// Maps floating-point data in the application domain, to 16-bit unsigned
// integer data actually stored on disk. Currently, implemented as a linear
// transformation.
class Transform {
 public:
  // Creates a linear transformation ax+b, for the specified multiplier (a) and
  // offset (b). The result is rounded to the nearest integer.
  static Transform Linear(float multiplier, float offset);

  // Creates a linear transformation, given the minimum and maximum
  // representable values. The minimum value will be mapped to 0, and the
  // maximum value will be mapped to 65535.
  static Transform LinearRange(float min_value, float max_value);

  // Applies the transformation to the specified input data. The result is
  // truncated to the [0, 65535] range.
  uint16_t apply(float value) const;

  // Recovers the application-domain data from the encoded uint16_t data.
  float unapply(uint16_t value) const;

  float multiplier() const { return multiplier_; }
  float offset() const { return offset_; }

 private:
  Transform(float multiplier, float offset)
      : multiplier_(multiplier), offset_(offset) {}

  float multiplier_;
  float offset_;
};

}  // namespace roo_monitoring