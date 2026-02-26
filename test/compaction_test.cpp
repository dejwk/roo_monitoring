#include <algorithm>
#include <vector>

#include "fakefs_reference.h"
#include "gtest/gtest.h"
#include "roo_monitoring.h"

namespace roo_monitoring {
namespace {

TEST(VaultCompactionTest, AggregatesToParentVault) {
  roo_io::fakefs::FakeFs fake_fs;
  roo_io::fakefs::FakeReferenceFs fs(fake_fs);
  Collection collection(fs, "test", kResolution_1_ms);
  Writer writer(&collection);

  {
    WriteTransaction tx(&writer);
    tx.write(0, 1, 10.0f);
    tx.write(1, 1, 20.0f);
    tx.write(2, 1, 30.0f);
    tx.write(3, 1, 40.0f);
    // Force the next entry so the first range of 4 entries is treated as
    // non-hot.
    tx.write(4, 1, 50.0f);
  }

  writer.flushAll();

  Resolution parent_resolution = Resolution(kResolution_1_ms + 1);
  VaultFileRef parent_ref = VaultFileRef::Lookup(0, parent_resolution);
  VaultFileReader reader(&collection);
  ASSERT_TRUE(reader.open(parent_ref, 0, 0));

  std::vector<Sample> samples;
  ASSERT_TRUE(reader.next(&samples));
  ASSERT_EQ(samples.size(), 1u);

  const Transform& transform = collection.transform();
  uint16_t v1 = transform.apply(10.0f);
  uint16_t v2 = transform.apply(20.0f);
  uint16_t v3 = transform.apply(30.0f);
  uint16_t v4 = transform.apply(40.0f);
  uint16_t expected_avg = (v1 + v2 + v3 + v4) / 4;
  uint16_t expected_min = std::min(std::min(v1, v2), std::min(v3, v4));
  uint16_t expected_max = std::max(std::max(v1, v2), std::max(v3, v4));

  EXPECT_EQ(samples[0].avg_value(), expected_avg);
  EXPECT_EQ(samples[0].min_value(), expected_min);
  EXPECT_EQ(samples[0].max_value(), expected_max);
  EXPECT_EQ(samples[0].fill(), 0x2000);

  String parent_path;
  collection.getVaultFilePath(parent_ref, &parent_path);
  String cursor_path = parent_path + ".cursor";
  roo_io::Mount mount = fs.mount();
  roo_io::Stat cursor_stat = mount.stat(cursor_path.c_str());
  EXPECT_EQ(cursor_stat.status(), roo_io::kOk);
  EXPECT_TRUE(cursor_stat.isFile());
}

TEST(VaultCompactionTest, AggregatesAcrossTwoLevels) {
  roo_io::fakefs::FakeFs fake_fs;
  roo_io::fakefs::FakeReferenceFs fs(fake_fs);
  Collection collection(fs, "test", kResolution_1_ms);
  Writer writer(&collection);

  {
    WriteTransaction tx(&writer);
    for (int i = 0; i < 16; ++i) {
      tx.write(i, 1, static_cast<float>((i + 1) * 10));
    }
    // Force a second range so the first is treated as non-hot.
    tx.write(16, 1, 999.0f);
  }

  writer.flushAll();

  Resolution grand_resolution = Resolution(kResolution_1_ms + 2);
  VaultFileRef grand_ref = VaultFileRef::Lookup(0, grand_resolution);
  VaultFileReader reader(&collection);
  ASSERT_TRUE(reader.open(grand_ref, 0, 0));

  std::vector<Sample> samples;
  ASSERT_TRUE(reader.next(&samples));
  ASSERT_EQ(samples.size(), 1u);

  const Transform& transform = collection.transform();
  uint16_t expected_min = transform.apply(10.0f);
  uint16_t expected_max = transform.apply(160.0f);
  uint32_t total = 0;
  for (int i = 0; i < 16; ++i) {
    total += transform.apply(static_cast<float>((i + 1) * 10));
  }
  uint16_t expected_avg = total / 16;

  EXPECT_EQ(samples[0].avg_value(), expected_avg);
  EXPECT_EQ(samples[0].min_value(), expected_min);
  EXPECT_EQ(samples[0].max_value(), expected_max);
  EXPECT_EQ(samples[0].fill(), 0x2000);
}

}  // namespace
}  // namespace roo_monitoring
