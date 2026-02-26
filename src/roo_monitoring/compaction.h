#pragma once

#include <map>
#include <memory>

#include "log.h"
#include "roo_io/data/output_stream_writer.h"
#include "roo_monitoring.h"
#include "stdint.h"

namespace roo_monitoring {

/// Aggregates samples for a vault file time bucket.
class Aggregator {
 public:
  /// Clears any accumulated data.
  void clear();
  /// Adds a sample into the aggregation state.
  void add(const Sample& sample);

 private:
  friend class VaultWriter;

  struct SampleAggregator {
    SampleAggregator()
        : weighted_total(0), weight(0), min_value(0xFFFF), max_value(0) {}

    uint32_t weighted_total;
    uint16_t weight;
    uint16_t min_value;
    uint16_t max_value;
  };

  std::vector<SampleAggregator> data_;
  std::map<uint64_t, int> index_;
};

/// Writes vault files for a collection at a specific resolution.
class VaultWriter {
 public:
  /// Creates a writer for the given collection and vault file.
  VaultWriter(Collection* collection, VaultFileRef ref);
  /// Returns the reference to the vault file being written.
  const VaultFileRef& vault_ref() const { return ref_; }

  /// Opens a new vault file for writing.
  roo_io::Status openNew();

  /// Opens an existing vault file, seeking to the specified entry index.
  roo_io::Status openExisting(int write_index);

  /// Closes the underlying writer.
  void close() { writer_.close(); }

  /// Returns the current write index within the vault file.
  int write_index() const { return write_index_; }

  /// Writes an empty vault file payload.
  void writeEmptyData();

  /// Writes raw log samples into the vault file.
  void writeLogData(const std::vector<LogSample>& data);

  /// Writes aggregated samples into the vault file.
  void writeAggregatedData(const Aggregator& aggregator);

  /// Returns true if the writer is in a good state.
  bool ok() const { return writer_.ok(); }

  /// Returns the current writer status.
  roo_io::Status status() const { return writer_.status(); }

 private:
  void writeHeader();

  const Collection* collection_;
  VaultFileRef ref_;
  int write_index_;
  roo_io::OutputStreamWriter writer_;
};

}  // namespace roo_monitoring