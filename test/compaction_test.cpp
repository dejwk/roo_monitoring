#include <algorithm>
#include <functional>
#include <utility>
#include <vector>

#include "fakefs_reference.h"
#include "gtest/gtest.h"
#include "roo_io/data/output_stream_writer.h"
#include "roo_monitoring.h"

namespace roo_monitoring {
namespace {

class RemovableFs : public roo_io::fakefs::FakeReferenceFs {
 public:
  using FakeReferenceFs::FakeReferenceFs;
  bool present = true;

 protected:
  roo_io::MountImpl::MountResult mountImpl(
      std::function<void()> unmount_fn) override {
    if (!present) return roo_io::MountImpl::MountError(roo_io::kNoMedia);
    return FakeReferenceFs::mountImpl(std::move(unmount_fn));
  }
};

// Verifies progress is published only after the complete hierarchy is flushed,
// excludes the hot bucket, survives missing media and idle flushes, and is
// recovered after restart.
TEST(VaultCompactionTest, PublishesCompletedDataBoundary) {
  roo_io::fakefs::FakeFs fake_fs;
  RemovableFs fs(fake_fs);
  Collection collection(fs, "progress", kResolution_1_ms);
  Writer writer(&collection);
  EXPECT_EQ(-1, collection.compactedUntil());
  writer.flushAll();
  EXPECT_EQ(-1, collection.compactedUntil());
  for (int end : {16, 32}) {
    {
      WriteTransaction tx(&writer);
      for (int time = end == 16 ? 0 : 17; time <= end; ++time) {
        tx.write(time, 1, 20.0f);
      }
    }
    int64_t previous = collection.compactedUntil();
    writer.flushSome();
    ASSERT_TRUE(writer.isFlushInProgress());
    EXPECT_EQ(previous, collection.compactedUntil());
    fs.present = false;
    writer.flushSome();
    EXPECT_TRUE(writer.isFlushInProgress());
    EXPECT_EQ(previous, collection.compactedUntil());
    fs.present = true;
    int steps = 0;
    while (writer.isFlushInProgress() && steps++ < 64) {
      writer.flushSome();
      if (writer.isFlushInProgress()) {
        EXPECT_EQ(previous, collection.compactedUntil());
      }
    }
    ASSERT_FALSE(writer.isFlushInProgress());
    ASSERT_EQ(Writer::IOSTATE_OK, writer.io_state());
    EXPECT_EQ(end, collection.compactedUntil());
    VaultIterator reader(&collection, end - 16, kResolution_16_ms);
    std::vector<Sample> samples;
    reader.next(&samples);
    ASSERT_EQ(1u, samples.size());
    EXPECT_GT(samples[0].fill(), 0);
    writer.flushAll();
    EXPECT_EQ(end, collection.compactedUntil());
  }
  Collection reopened(fs, "progress", kResolution_1_ms);
  Writer resumed(&reopened);
  EXPECT_EQ(-1, reopened.compactedUntil());
  resumed.flushAll();
  EXPECT_EQ(32, reopened.compactedUntil());
}

// Verifies an unwritable vault or cursor leaves the hot log available for
// retry and prevents compaction progress from being published.
TEST(VaultCompactionTest, FullDiskDoesNotPublishHotRange) {
  for (size_t available : {0u, 12u}) {
    SCOPED_TRACE(available);
    roo_io::fakefs::FakeFs fake_fs;
    roo_io::fakefs::FakeReferenceFs fs(fake_fs);
    Collection collection(fs, "full", kResolution_1_ms);
    Writer writer(&collection);
    {
      WriteTransaction tx(&writer);
      tx.write(0, 1, 20.0f);
      tx.write(1, 1, 20.0f);
    }
    String log_dir = subdir(subdir(kMonitoringBasePath, "full"), kLogSubPath);
    String log_path = filepath(log_dir, 0);
    roo_io::fakefs::StatResult log = fake_fs.stat(log_path.c_str());
    ASSERT_EQ(roo_io::kOk, log.status);

    // The first bucket occupies 12 bytes including the header. Allow either
    // no writes or enough room for the vault but none for the cursor.
    fake_fs.setCapacity(log.size + available);
    writer.flushAll();
    EXPECT_EQ(Writer::IOSTATE_ERROR, writer.io_state());
    EXPECT_EQ(-1, collection.compactedUntil());
    EXPECT_EQ(roo_io::kOk, fake_fs.stat(log_path.c_str()).status);
    String vault_path;
    collection.getVaultFilePath(VaultFileRef::Lookup(0, kResolution_1_ms),
                                &vault_path);
    String cursor_path = vault_path + ".cursor";
    EXPECT_EQ(roo_io::kNotFound, fake_fs.stat(cursor_path.c_str()).status);
  }
}

// Verifies historical logs survive a vault write failure and remain available
// for a new writer to retry.
TEST(VaultCompactionTest, FullDiskPreservesHistoricalLog) {
  roo_io::fakefs::FakeFs fake_fs;
  roo_io::fakefs::FakeReferenceFs fs(fake_fs);
  Collection collection(fs, "full", kResolution_1_ms);
  Writer writer(&collection);
  {
    WriteTransaction tx(&writer);
    tx.write(0, 1, 20.0f);
    tx.write(256, 1, 20.0f);
  }
  String log_dir = subdir(subdir(kMonitoringBasePath, "full"), kLogSubPath);
  String old_path = filepath(log_dir, 0);
  String hot_path = filepath(log_dir, 256);
  roo_io::fakefs::StatResult old_log = fake_fs.stat(old_path.c_str());
  roo_io::fakefs::StatResult hot_log = fake_fs.stat(hot_path.c_str());
  ASSERT_EQ(roo_io::kOk, old_log.status);
  ASSERT_EQ(roo_io::kOk, hot_log.status);
  fake_fs.setCapacity(old_log.size + hot_log.size);

  writer.flushSome();
  EXPECT_EQ(Writer::IOSTATE_ERROR, writer.io_state());
  EXPECT_EQ(-1, collection.compactedUntil());
  EXPECT_EQ(roo_io::kOk, fake_fs.stat(old_path.c_str()).status);

  fake_fs.setCapacity(4096);
  writer.flushAll();
  EXPECT_EQ(roo_io::kOk, fake_fs.stat(old_path.c_str()).status);

  Collection reopened(fs, "full", kResolution_1_ms);
  Writer retry(&reopened);
  retry.flushAll();
  EXPECT_EQ(Writer::IOSTATE_OK, retry.io_state());
  VaultIterator reader(&reopened, 0, kResolution_1_ms);
  std::vector<Sample> samples;
  reader.next(&samples);
  ASSERT_EQ(1u, samples.size());
  EXPECT_EQ(reopened.transform().apply(20.0f), samples[0].avg_value());
}

// Verifies a parent write failure stops compaction before saving its cursor.
TEST(VaultCompactionTest, FullDiskRejectsParentVault) {
  roo_io::fakefs::FakeFs fake_fs;
  roo_io::fakefs::FakeReferenceFs fs(fake_fs);
  Collection collection(fs, "full", kResolution_1_ms);
  Writer writer(&collection);
  {
    WriteTransaction tx(&writer);
    tx.write(0, 1, 20.0f);
    tx.write(3, 1, 20.0f);
    tx.write(4, 1, 20.0f);
  }
  writer.flushSome();
  ASSERT_TRUE(writer.isFlushInProgress());
  ASSERT_EQ(Writer::IOSTATE_OK, writer.io_state());
  String log_dir = subdir(subdir(kMonitoringBasePath, "full"), kLogSubPath);
  String log_path = filepath(log_dir, 0);
  String vault_path;
  collection.getVaultFilePath(VaultFileRef::Lookup(0, kResolution_1_ms),
                              &vault_path);
  String cursor_path = vault_path + ".cursor";
  roo_io::fakefs::StatResult log = fake_fs.stat(log_path.c_str());
  roo_io::fakefs::StatResult vault = fake_fs.stat(vault_path.c_str());
  roo_io::fakefs::StatResult cursor = fake_fs.stat(cursor_path.c_str());
  ASSERT_EQ(roo_io::kOk, log.status);
  ASSERT_EQ(roo_io::kOk, vault.status);
  ASSERT_EQ(roo_io::kOk, cursor.status);
  fake_fs.setCapacity(log.size + vault.size + cursor.size);

  writer.flushSome();
  EXPECT_EQ(Writer::IOSTATE_ERROR, writer.io_state());
  EXPECT_EQ(-1, collection.compactedUntil());
  String parent_path;
  collection.getVaultFilePath(VaultFileRef::Lookup(0, kResolution_4_ms),
                              &parent_path);
  String parent_cursor = parent_path + ".cursor";
  EXPECT_EQ(roo_io::kNotFound, fake_fs.stat(parent_cursor.c_str()).status);
}

// Verifies an incomplete record in a historical log cannot be mistaken for
// normal EOF and cause the source file to be deleted.
TEST(VaultCompactionTest, RetainsTruncatedHistoricalLog) {
  roo_io::fakefs::FakeFs fake_fs;
  roo_io::fakefs::FakeReferenceFs fs(fake_fs);
  Collection collection(fs, "truncated", kResolution_1_ms);
  Writer writer(&collection);
  {
    WriteTransaction tx(&writer);
    tx.write(0, 1, 20.0f);
    tx.write(256, 1, 20.0f);
  }
  String log_dir =
      subdir(subdir(kMonitoringBasePath, "truncated"), kLogSubPath);
  String path = filepath(log_dir, 0);
  roo_io::fakefs::FileStream log =
      fake_fs.open(path.c_str(), roo_io::fakefs::FakeFs::kWrite |
                                     roo_io::fakefs::FakeFs::kTruncate);
  ASSERT_TRUE(log.isOpen());
  // Timestamp and stream ID followed by only one byte of the value.
  using roo_io::byte;
  const byte contents[] = {byte{1}, byte{0}, byte{2}, byte{1}, byte{0}};
  ASSERT_EQ(sizeof(contents), log.write(contents, sizeof(contents)));
  log.close();

  writer.flushSome();
  EXPECT_EQ(Writer::IOSTATE_ERROR, writer.io_state());
  EXPECT_EQ(-1, collection.compactedUntil());
  EXPECT_EQ(roo_io::kOk, fake_fs.stat(path.c_str()).status);
}

// Verifies a truncated child bucket prevents publication of the hierarchy.
TEST(VaultCompactionTest, RejectsTruncatedChildBucket) {
  roo_io::fakefs::FakeFs fake_fs;
  roo_io::fakefs::FakeReferenceFs fs(fake_fs);
  Collection collection(fs, "truncated", kResolution_1_ms);
  Writer writer(&collection);
  {
    WriteTransaction tx(&writer);
    tx.write(0, 1, 20.0f);
    tx.write(3, 1, 20.0f);
    tx.write(4, 1, 20.0f);
  }
  writer.flushSome();
  ASSERT_TRUE(writer.isFlushInProgress());
  ASSERT_EQ(Writer::IOSTATE_OK, writer.io_state());
  String path;
  collection.getVaultFilePath(VaultFileRef::Lookup(0, kResolution_1_ms), &path);
  roo_io::fakefs::FileStream child =
      fake_fs.open(path.c_str(), roo_io::fakefs::FakeFs::kWrite |
                                     roo_io::fakefs::FakeFs::kTruncate);
  ASSERT_TRUE(child.isOpen());
  // A valid vault header followed by an incomplete sample count.
  using roo_io::byte;
  const byte contents[] = {byte{1}, byte{1}, byte{0x80}};
  ASSERT_EQ(sizeof(contents), child.write(contents, sizeof(contents)));
  child.close();

  writer.flushSome();
  EXPECT_EQ(Writer::IOSTATE_ERROR, writer.io_state());
  EXPECT_EQ(-1, collection.compactedUntil());
}

// Verifies a restart before parent compaction or after saving the 4 ms
// cursor replays the base range and completes the remaining hierarchy.
TEST(VaultCompactionTest, ResumesHistoricalRangeAfterRestart) {
  for (int steps_before_restart : {1, 2}) {
    SCOPED_TRACE(steps_before_restart);
    roo_io::fakefs::FakeFs fake_fs;
    roo_io::fakefs::FakeReferenceFs fs(fake_fs);
    String log_dir =
        subdir(subdir(kMonitoringBasePath, "restart"), kLogSubPath);
    String log_path = filepath(log_dir, 0);
    {
      Collection collection(fs, "restart", kResolution_1_ms);
      Writer writer(&collection);
      {
        WriteTransaction tx(&writer);
        tx.write(0, 1, 20.0f);
        tx.write(1024, 1, 20.0f);
      }
      for (int i = 0; i < steps_before_restart; ++i) writer.flushSome();
      ASSERT_TRUE(writer.isFlushInProgress());
      ASSERT_EQ(Writer::IOSTATE_OK, writer.io_state());
      EXPECT_EQ(-1, collection.compactedUntil());
      ASSERT_EQ(roo_io::kOk, fake_fs.stat(log_path.c_str()).status);
      VaultIterator grandparent(&collection, 0, kResolution_16_ms);
      std::vector<Sample> samples;
      grandparent.next(&samples);
      EXPECT_TRUE(samples.empty());
    }

    Collection reopened(fs, "restart", kResolution_1_ms);
    Writer resumed(&reopened);
    resumed.flushAll();
    ASSERT_EQ(Writer::IOSTATE_OK, resumed.io_state());
    EXPECT_GE(reopened.compactedUntil(), 16);
    EXPECT_EQ(roo_io::kNotFound, fake_fs.stat(log_path.c_str()).status);
    for (Resolution resolution :
         {kResolution_1_ms, kResolution_4_ms, kResolution_16_ms}) {
      VaultIterator reader(&reopened, 0, resolution);
      std::vector<Sample> samples;
      reader.next(&samples);
      ASSERT_EQ(1u, samples.size());
      EXPECT_EQ(reopened.transform().apply(20.0f), samples[0].avg_value());
    }
  }
}

// Verifies restart resumes removal of a completed range with one log left
// or with every log gone, while preserving samples from the deleted log.
TEST(VaultCompactionTest, ResumesCleanupOfCompletedRange) {
  for (bool remaining_log : {true, false}) {
    SCOPED_TRACE(remaining_log);
    roo_io::fakefs::FakeFs fake_fs;
    roo_io::fakefs::FakeReferenceFs fs(fake_fs);
    String log_dir =
        subdir(subdir(kMonitoringBasePath, "cleanup"), kLogSubPath);
    String first_path = filepath(log_dir, 0);
    String second_path = filepath(log_dir, 4);
    String newer_path = filepath(log_dir, 1024);
    String marker_path = subdir(log_dir, "compaction.done");
    Collection collection(fs, "cleanup", kResolution_1_ms);
    {
      Writer writer(&collection);
      WriteTransaction tx(&writer);
      tx.write(0, 1, 20.0f);
    }
    std::vector<roo_io::byte> second_log;
    {
      Writer writer(&collection);
      {
        WriteTransaction tx(&writer);
        tx.write(4, 1, 40.0f);
        tx.write(1024, 1, 60.0f);
      }
      second_log = roo_io::fakefs::ReadFile(fake_fs, second_path.c_str());
      ASSERT_FALSE(second_log.empty());
      writer.flushAll();
      ASSERT_EQ(Writer::IOSTATE_OK, writer.io_state());
      ASSERT_EQ(16, collection.compactedUntil());
    }
    ASSERT_EQ(roo_io::kNotFound, fake_fs.stat(first_path.c_str()).status);
    ASSERT_EQ(roo_io::kNotFound, fake_fs.stat(second_path.c_str()).status);
    ASSERT_EQ(roo_io::kNotFound, fake_fs.stat(marker_path.c_str()).status);

    // Restore the on-disk state left by a restart at the end of log cleanup.
    {
      roo_io::Mount mount = fs.mount();
      roo_io::OutputStreamWriter marker(
          mount.fopenForWrite(marker_path.c_str(), roo_io::kFailIfExists));
      ASSERT_TRUE(marker.ok());
      marker.writeBeU64(16);
      marker.close();
      ASSERT_EQ(roo_io::kClosed, marker.status());
    }
    if (remaining_log) {
      roo_io::fakefs::FileStream log =
          fake_fs.open(second_path.c_str(), roo_io::fakefs::FakeFs::kWrite);
      ASSERT_TRUE(log.isOpen());
      ASSERT_EQ(second_log.size(),
                log.write(second_log.data(), second_log.size()));
      log.close();
    }

    Collection reopened(fs, "cleanup", kResolution_1_ms);
    Writer resumed(&reopened);
    resumed.flushAll();
    ASSERT_EQ(Writer::IOSTATE_OK, resumed.io_state());
    EXPECT_GE(reopened.compactedUntil(), 16);
    EXPECT_EQ(roo_io::kNotFound, fake_fs.stat(second_path.c_str()).status);
    EXPECT_EQ(roo_io::kNotFound, fake_fs.stat(marker_path.c_str()).status);
    EXPECT_EQ(roo_io::kOk, fake_fs.stat(newer_path.c_str()).status);
    VaultIterator reader(&reopened, 0, kResolution_4_ms);
    std::vector<Sample> samples;
    reader.next(&samples);
    ASSERT_EQ(1u, samples.size());
    EXPECT_EQ(reopened.transform().apply(20.0f), samples[0].avg_value());
  }
}

// Verifies historical logs survive a failure to publish the cleanup record.
TEST(VaultCompactionTest, KeepsLogsWhenCompletionMarkerCannotBeWritten) {
  roo_io::fakefs::FakeFs fake_fs;
  roo_io::fakefs::FakeReferenceFs fs(fake_fs);
  String log_dir =
      subdir(subdir(kMonitoringBasePath, "marker_error"), kLogSubPath);
  String log_path = filepath(log_dir, 0);
  String marker_path = subdir(log_dir, "compaction.done");
  String temporary_path = marker_path + ".tmp";
  {
    Collection collection(fs, "marker_error", kResolution_1_ms);
    Writer writer(&collection);
    {
      WriteTransaction tx(&writer);
      tx.write(0, 1, 20.0f);
      tx.write(1024, 1, 20.0f);
    }
    ASSERT_EQ(roo_io::kOk, fake_fs.mkdir(temporary_path.c_str()));
    writer.flushAll();
    EXPECT_EQ(Writer::IOSTATE_ERROR, writer.io_state());
    EXPECT_EQ(-1, collection.compactedUntil());
    EXPECT_EQ(roo_io::kOk, fake_fs.stat(log_path.c_str()).status);
    EXPECT_EQ(roo_io::kNotFound, fake_fs.stat(marker_path.c_str()).status);
    VaultIterator reader(&collection, 0, kResolution_16_ms);
    std::vector<Sample> samples;
    reader.next(&samples);
    ASSERT_EQ(1u, samples.size());
  }

  ASSERT_EQ(roo_io::kOk, fake_fs.rmdir(temporary_path.c_str()));
  Collection reopened(fs, "marker_error", kResolution_1_ms);
  Writer resumed(&reopened);
  resumed.flushAll();
  EXPECT_EQ(Writer::IOSTATE_OK, resumed.io_state());
  EXPECT_EQ(16, reopened.compactedUntil());
  EXPECT_EQ(roo_io::kNotFound, fake_fs.stat(log_path.c_str()).status);
}

// Verifies that a hot log opened in the last bucket of a range remains
// available until the writer actually moves into the next range.
TEST(VaultCompactionTest, KeepsHotLogAtEndOfRange) {
  roo_io::fakefs::FakeFs fake_fs;
  roo_io::fakefs::FakeReferenceFs fs(fake_fs);
  Collection collection(fs, "last_bucket", kResolution_1_ms);
  Writer writer(&collection);
  {
    WriteTransaction tx(&writer);
    tx.write(15, 1, 20.0f);
  }
  writer.flushAll();
  ASSERT_EQ(Writer::IOSTATE_OK, writer.io_state());
  EXPECT_EQ(0, collection.compactedUntil());
  String log_dir =
      subdir(subdir(kMonitoringBasePath, "last_bucket"), kLogSubPath);
  String path = filepath(log_dir, 15);
  EXPECT_EQ(roo_io::kOk, fake_fs.stat(path.c_str()).status);

  {
    WriteTransaction tx(&writer);
    tx.write(16, 1, 20.0f);
  }
  writer.flushAll();
  ASSERT_EQ(Writer::IOSTATE_OK, writer.io_state());
  EXPECT_EQ(16, collection.compactedUntil());
  EXPECT_EQ(roo_io::kNotFound, fake_fs.stat(path.c_str()).status);
}

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
