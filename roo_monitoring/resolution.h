#pragma once

namespace roo_monitoring {

enum Resolution {
  kResolution_1_ms = 0,             // 1 ms
  kResolution_4_ms = 1,             // 4 ms
  kResolution_16_ms = 2,            // 16 ms
  kResolution_64_ms = 3,            // 64 ms
  kResolution_256_ms = 4,           // 256 s
  kResolution_1024_ms = 5,          // ~ 1 s
  kResolution_4096_ms = 6,          // ~ 4 s
  kResolution_16384_ms = 7,         // ~ 16 s
  kResolution_65536_ms = 8,         // ~ 1.09 min
  kResolution_262144_ms = 9,        // ~ 4.37 min
  kResolution_1048576_ms = 10,      // ~ 17.47 min
  kResolution_4194304_ms = 11,      // ~ 70 min
  kResolution_16777216_ms = 12,     // ~ 4.66 h
  kResolution_67108864_ms = 13,     // ~ 18.64 h
  kResolution_268435456_ms = 14,    // ~ 3.1 days
  kResolution_1073741824_ms = 15,   // ~ 12.4 days
  kResolution_4294967296_ms = 16,   // ~ 49.7 days
  kResolution_17179869184_ms = 17,  // ~ 199 days
  kResolution_68719476736_ms = 18,  // ~ 2.18 years
};

static const Resolution kMaxResolution = kResolution_68719476736_ms;

static int64_t timestamp_ms_floor(int64_t timestamp_ms, Resolution resolution) {
  // Resolution is the exponent with base 4, so we need to multiply
  // by 2 when converting to base 2. Then, using shift to generate
  // the required number of zeros in the mask.
  return timestamp_ms & (0xFFFFFFFFFFFFFFFFLL << (resolution << 1));
}

static int64_t timestamp_ms_ceil(int64_t timestamp_ms, Resolution resolution) {
  // Like the above, but mask is negated (so it has the requested count
  // of trailing 1s) and ORed with the timestamp.
  return timestamp_ms | ~(0xFFFFFFFFFFFFFFFFLL << (resolution << 1));
}

static int64_t timestamp_increment(int64_t steps, Resolution resolution) {
  return steps << (resolution << 1);
}

}  // namespace roo_monitoring
