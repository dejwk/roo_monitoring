# Programming guide

## What roo_monitoring is for

`roo_monitoring` stores time-series data on an embedded filesystem. It is built
for the common pattern of writing small sensor samples continuously and reading
them back later at several time scales.

The important point is that it is meant to stay lightweight enough to live
entirely on the microcontroller. It does not assume an external collector,
database, or export pipeline. A device can sample its own metrics, write them
to local flash or an SD card, compact and downsample the history incrementally,
and then serve that history back locally at several resolutions.

That makes it a good fit for self-contained embedded products. A typical use
case is a microcontroller with a local SD card that continuously records sensor
values, counters, or system-health metrics, and an attached TFT display that
lets the user inspect those metrics directly on the device. The same unit that
captures the data can also answer questions such as "what happened in the last
few seconds", "what was the trend over the last hour", or "how did the system
behave over the last few days" without shipping the raw stream to another
machine first.

The library keeps that feasible by doing only small append work on the write
path, then incrementally compacting finer-grained data into coarser summaries.
That hierarchy is what lets a microcontroller retain useful history and serve
it back at multiple resolutions with minimal ongoing overhead.

Most applications only need four concepts:

- A [Collection](../src/roo_monitoring.h), which groups related streams.
- App-defined stream IDs, one per sensor or metric.
- A [Writer](../src/roo_monitoring.h) and [WriteTransaction](../src/roo_monitoring.h),
  which append samples.
- A [VaultIterator](../src/roo_monitoring.h), which reads compacted data back.

Internally, writes first go to raw log files. Later, flushing compacts those
logs into hierarchical vault files that can be read efficiently at the
collection's own [Resolution](../src/roo_monitoring/resolution.h) or at coarser
resolutions. You do not need to manage file names or directory layout yourself.

## First working example

The smallest useful setup is:

- Pick a [roo_io filesystem](../../roo_io/src/roo_io/fs/filesystem.h).
- Create one collection.
- Create one writer for that collection.
- Open a short-lived transaction whenever you want to record data.
- Call `flushSome()` occasionally so logs are compacted into vaults.

For example, on ESP32 with LittleFS:

```cpp
#include <Arduino.h>

#include "roo_io/fs/esp32/littlefs.h"
#include "roo_monitoring.h"

using namespace roo_monitoring;

constexpr uint64_t kTemperatureStream = 1;
constexpr uint64_t kHumidityStream = 2;

roo_io::Filesystem& storage = roo_io::LITTLEFS;
Collection telemetry(storage, "telemetry", kResolution_1024_ms);
Writer writer(&telemetry);

float readTemperatureC();
float readHumidityPct();

void setup() {
  Serial.begin(115200);
}

void loop() {
  int64_t now_ms = millis();

  {
    WriteTransaction tx(&writer);
    tx.write(now_ms, kTemperatureStream, readTemperatureC());
    tx.write(now_ms, kHumidityStream, readHumidityPct());
  }

  writer.flushSome();
  delay(1000);
}
```

This example already shows the intended high-level workflow.

- The collection defines where the data lives and what its base resolution is.
- The writer owns the append/flush machinery.
- The transaction is a transient RAII object. When it goes out of scope, the
  current log writer is closed.
- `flushSome()` performs a bounded amount of maintenance work. It is appropriate
  to call from `loop()` or another background task.

If you need a stronger synchronization point, call `flushAll()` instead. That
runs compaction until there is no more pending work.

## Collections, streams, and timestamps

A collection is the unit of storage and readback. Put streams in the same
collection when they naturally belong together and should share the same base
resolution.

Each stream is identified by an application-defined `uint64_t`. `roo_monitoring`
stores stream IDs, but it does not assign names, units, or metadata for you.
Most applications should define stream IDs as constants near the sensor code.

Each write has three pieces of information:

- A timestamp in milliseconds.
- A stream ID.
- A float value.

The writer expects timestamps to move forward over time. In practice, treat the
log as append-only and write samples in nondecreasing timestamp order.

## Choosing a resolution

The collection's base resolution is the most important decision you make up
front.

When you write a sample, its timestamp is rounded down to the collection's base
bucket. This has two immediate consequences.

- If you sample more often than the base resolution, several writes can land in
  the same bucket.
- If you write the same stream more than once in the same bucket, the first
  sample wins and later writes for that stream and bucket are skipped.

In other words, choose the finest raw resolution you actually care to preserve.
You can always read the data back later at coarser resolutions after compaction,
but you cannot recover detail that was discarded at write time.

Some practical starting points:

- Use `kResolution_1_ms` or `kResolution_4_ms` for genuinely high-rate signals.
- Use `kResolution_1024_ms` for once-per-second telemetry.
- Use a coarser resolution only when you know that sub-bucket variation does
  not matter.

If you are unsure, start finer rather than coarser, then let the vault
hierarchy supply downsampled views later.

## Writing samples

Most write sites should stay simple. Open a transaction, write one sample per
stream you care about, then let the transaction go out of scope.

```cpp
void publishTelemetry(int64_t now_ms) {
  WriteTransaction tx(&writer);
  tx.write(now_ms, kTemperatureStream, readTemperatureC());
  tx.write(now_ms, kHumidityStream, readHumidityPct());
}
```

The library takes care of encoding the float to the collection's stored format,
opening the right log file, and appending the record.

A few rules of thumb keep the write path predictable:

- Keep transactions short.
- Use stable stream IDs.
- Feed timestamps in chronological order.
- Do not expect repeated writes for the same stream in the same bucket to be
  averaged or overwritten; they are skipped.

## What a sample becomes on disk

`roo_monitoring` stores values in an encoded 16-bit representation. The
[Transform](../src/roo_monitoring/transform.h) attached to the collection is
used on the way in and on the way out.

Most application code does not need to think about this during writes, but it
does matter during readback. The numbers stored in a
[Sample](../src/roo_monitoring/sample.h) are encoded values. To recover the
application-domain float, call `collection.transform().unapply(...)`.

```cpp
float avg = telemetry.transform().unapply(sample.avg_value());
float min_value = telemetry.transform().unapply(sample.min_value());
float max_value = telemetry.transform().unapply(sample.max_value());
```

## Flushing and compaction

Writes go into raw log files first. Those files are cheap to append to, but
they are not the main readback format. Flushing turns those logs into vault
files, and repeated compaction builds coarser resolutions automatically.

Use the two writer maintenance calls like this:

- `flushSome()` for opportunistic background work.
- `flushAll()` when you want to block until everything pending has been folded
  into the vault hierarchy.

Typical uses for `flushAll()` are test code, explicit sync operations, or a
controlled shutdown path such as preparing for deep sleep.

If a filesystem error occurs, the writer reports it through `io_state()`.
Applications that care about data durability should check that state after
flushes and surface failures in logs or diagnostics.

## Reading data back

The normal readback API is [VaultIterator](../src/roo_monitoring.h). It reads a
continuous timeline at the resolution you request.

```cpp
#include <vector>

void dumpRecentBuckets(int64_t start_ms, int count) {
  VaultIterator it(&telemetry, start_ms, kResolution_1024_ms);
  std::vector<Sample> bucket;

  for (int i = 0; i < count; ++i) {
    int64_t bucket_start = it.cursor();
    it.next(&bucket);

    if (bucket.empty()) {
      Serial.print(bucket_start);
      Serial.println(": no data");
      continue;
    }

    for (const Sample& sample : bucket) {
      float avg = telemetry.transform().unapply(sample.avg_value());
      float min_value = telemetry.transform().unapply(sample.min_value());
      float max_value = telemetry.transform().unapply(sample.max_value());

      Serial.print(bucket_start);
      Serial.print(" stream=");
      Serial.print((unsigned long long) sample.stream_id());
      Serial.print(" avg=");
      Serial.print(avg);
      Serial.print(" min=");
      Serial.print(min_value);
      Serial.print(" max=");
      Serial.println(max_value);
    }
  }
}
```

Two details are worth remembering:

- `cursor()` returns the timestamp of the next bucket to be read, so capture it
  before calling `next()`.
- Missing ranges are presented as empty buckets. This makes it easy to scan a
  continuous timeline without manually checking whether each vault file exists.

## Reading at coarser resolutions

One of the main benefits of the vault hierarchy is that the same collection can
be read back at several time scales.

If you wrote raw data at one-second resolution, you can later request a coarser
view such as sixteen-second buckets:

```cpp
VaultIterator summary(&telemetry, start_ms, kResolution_16384_ms);
```

Each returned `Sample` then represents an aggregate over the requested bucket.

- `avg_value()` is the bucket average.
- `min_value()` is the bucket minimum.
- `max_value()` is the bucket maximum.
- `fill()` indicates how full the bucket was, where `0x2000` means 100%.

This lets you keep a reasonably fine raw history while still supporting quick
trend views at minute, hour, or day scale.

## Understanding the storage model

You can use the library productively without caring about the file format, but
the rough model is simple.

- Raw writes go to log files.
- Flushing compacts completed log ranges into vault files at the collection's
  base resolution.
- Four adjacent buckets at one level are aggregated into one bucket at the next
  coarser level.
- Hot ranges may be compacted incrementally, so partially built vault files can
  advance over time instead of being rewritten from scratch every time.

That hierarchy is why it is cheap to read the same history back at different
resolutions.

## Lower-level APIs

The high-level path is enough for most applications:

- [Collection](../src/roo_monitoring.h)
- [Writer](../src/roo_monitoring.h)
- [WriteTransaction](../src/roo_monitoring.h)
- [VaultIterator](../src/roo_monitoring.h)

There are also lower-level APIs for specialized tooling and debugging:

- [LogFileReader](../src/roo_monitoring/log.h) and [LogReader](../src/roo_monitoring/log.h)
  for raw log traversal.
- [VaultFileRef](../src/roo_monitoring/vault.h) and [VaultFileReader](../src/roo_monitoring/vault.h)
  for direct vault navigation.
- [VaultWriter](../src/roo_monitoring/compaction.h) when working with vault
  generation explicitly.

Reach for those only when you are building custom maintenance tools, recovery
flows, or tests that need to inspect the storage format directly.

## Caveats

The current library surface is intentionally small, but there are a few limits
you should know about.

- Collection-level value encoding is currently fixed by the implementation. You
  can inspect the transform with `collection.transform()`, but there is no
  public setter yet.
- In the current implementation, the built-in transform gives roughly
  1/256-unit precision across about `[-128, 128)`. Values outside that range
  clip.
- Stream metadata such as names, units, and display labels is the application's
  responsibility.
- Reads are sequential. There is no higher-level query language or index beyond
  the vault hierarchy itself.

Within those constraints, the library's intended workflow is straightforward:
append in chronological order, flush regularly, and read back through
`VaultIterator` at the resolution that matches the question you are asking.