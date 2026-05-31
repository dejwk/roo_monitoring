#pragma once

#include <inttypes.h>

namespace roo_monitoring {

/// Time resolution used for log and vault files.
enum Resolution {
  kResolution_1_ms = 0,             ///< 1 millisecond buckets.
  kResolution_4_ms = 1,             ///< 4 millisecond buckets.
  kResolution_16_ms = 2,            ///< 16 millisecond buckets.
  kResolution_64_ms = 3,            ///< 64 millisecond buckets.
  kResolution_256_ms = 4,           ///< 256 millisecond buckets.
  kResolution_1024_ms = 5,          ///< Approximately 1 second buckets.
  kResolution_4096_ms = 6,          ///< Approximately 4 second buckets.
  kResolution_16384_ms = 7,         ///< Approximately 16 second buckets.
  kResolution_65536_ms = 8,         ///< Approximately 1.09 minute buckets.
  kResolution_262144_ms = 9,        ///< Approximately 4.37 minute buckets.
  kResolution_1048576_ms = 10,      ///< Approximately 17.47 minute buckets.
  kResolution_4194304_ms = 11,      ///< Approximately 70 minute buckets.
  kResolution_16777216_ms = 12,     ///< Approximately 4.66 hour buckets.
  kResolution_67108864_ms = 13,     ///< Approximately 18.64 hour buckets.
  kResolution_268435456_ms = 14,    ///< Approximately 3.1 day buckets.
  kResolution_1073741824_ms = 15,   ///< Approximately 12.4 day buckets.
  kResolution_4294967296_ms = 16,   ///< Approximately 49.7 day buckets.
  kResolution_17179869184_ms = 17,  ///< Approximately 199 day buckets.
  kResolution_68719476736_ms = 18,  ///< Approximately 2.18 year buckets.
};

/// Highest resolution enum value supported by the library.
static const Resolution kMaxResolution = kResolution_68719476736_ms;

/// Rounds the timestamp down to the specified resolution bucket.
inline constexpr static int64_t timestamp_ms_floor(int64_t timestamp_ms,
                                                   Resolution resolution) {
  // Resolution is the exponent with base 4, so we need to multiply
  // by 2 when converting to base 2. Then, using shift to generate
  // the required number of zeros in the mask.
  return timestamp_ms & (0xFFFFFFFFFFFFFFFFLL << (resolution << 1));
}

/// Rounds the timestamp up to the specified resolution bucket.
inline constexpr static int64_t timestamp_ms_ceil(int64_t timestamp_ms,
                                                  Resolution resolution) {
  // Like the above, but mask is negated (so it has the requested count
  // of trailing 1s) and ORed with the timestamp.
  return timestamp_ms | ~(0xFFFFFFFFFFFFFFFFLL << (resolution << 1));
}

/// Returns the timestamp delta for the given number of resolution steps.
inline constexpr static int64_t timestamp_increment(int64_t steps,
                                                    Resolution resolution) {
  return steps << (resolution << 1);
}

}  // namespace roo_monitoring
