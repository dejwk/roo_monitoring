#include <vector>

#include "fakefs_reference.h"
#include "gtest/gtest.h"
#include "roo_monitoring.h"
#include "roo_monitoring/compaction.h"

namespace roo_monitoring {
namespace {

constexpr const char* kLogDir = "/log";

TEST(TransformTest, LinearRangeRoundTrip) {
  Transform transform = Transform::LinearRange(0.0f, 100.0f);
  EXPECT_EQ(transform.apply(0.0f), 0u);
  EXPECT_EQ(transform.apply(100.0f), 65535u);
  EXPECT_NEAR(transform.unapply(0), 0.0f, 1e-3f);
  EXPECT_NEAR(transform.unapply(65535), 100.0f, 1e-2f);
}

TEST(ResolutionTest, FloorCeilIncrement) {
  int64_t timestamp = 123;
  EXPECT_EQ(timestamp_ms_floor(timestamp, kResolution_4_ms), 120);
  EXPECT_EQ(timestamp_ms_ceil(timestamp, kResolution_4_ms), 123);
  EXPECT_EQ(timestamp_increment(1, kResolution_4_ms), 4);
}

TEST(LogIoTest, WriteAndReadBack) {
  roo_io::fakefs::FakeFs fake_fs;
  roo_io::fakefs::FakeReferenceFs fs(fake_fs);
  CachedLogDir cache(fs, kLogDir);
  LogWriter writer(fs, kLogDir, cache, kResolution_1_ms);

  writer.write(1000, 2, 20);
  writer.write(1000, 1, 10);
  writer.write(1001, 2, 30);
  writer.close();

  roo_io::Mount mount = fs.mount();
  LogFileReader reader(mount);
  String path = filepath(String(kLogDir), 1000);
  ASSERT_TRUE(reader.open(path.c_str(), 0));

  int64_t timestamp = 0;
  std::vector<LogSample> samples;
  ASSERT_TRUE(reader.next(&timestamp, &samples, false));
  EXPECT_EQ(timestamp, 1000);
  ASSERT_EQ(samples.size(), 2u);
  EXPECT_EQ(samples[0].stream_id(), 1u);
  EXPECT_EQ(samples[0].value(), 10u);
  EXPECT_EQ(samples[1].stream_id(), 2u);
  EXPECT_EQ(samples[1].value(), 20u);

  samples.clear();
  ASSERT_TRUE(reader.next(&timestamp, &samples, false));
  EXPECT_EQ(timestamp, 1001);
  ASSERT_EQ(samples.size(), 1u);
  EXPECT_EQ(samples[0].stream_id(), 2u);
  EXPECT_EQ(samples[0].value(), 30u);
}

TEST(VaultReaderTest, SeekForwardPositionsAtExpectedEntry) {
  roo_io::fakefs::FakeFs fake_fs;
  roo_io::fakefs::FakeReferenceFs fs(fake_fs);
  Collection collection(fs, "test", kResolution_1_ms);

  VaultFileRef ref = VaultFileRef::Lookup(0, kResolution_1_ms);
  VaultWriter writer(&collection, ref);
  ASSERT_EQ(writer.openNew(), roo_io::kOk);

  for (uint16_t i = 0; i < 5; ++i) {
    std::vector<LogSample> data;
    data.emplace_back(1, i);
    writer.writeLogData(data);
  }
  writer.close();

  VaultFileReader reader(&collection);
  ASSERT_TRUE(reader.open(ref, 0, 0));
  reader.seekForward(2);

  std::vector<Sample> samples;
  ASSERT_TRUE(reader.next(&samples));
  ASSERT_EQ(samples.size(), 1u);
  EXPECT_EQ(samples[0].avg_value(), 2u);
}

}  // namespace
}  // namespace roo_monitoring
