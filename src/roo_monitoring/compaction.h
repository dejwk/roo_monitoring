#pragma once

#include <map>
#include <memory>

#include "log.h"
#include "roo_io/data/output_stream_writer.h"
#include "roo_monitoring.h"
#include "stdint.h"

namespace roo_monitoring {

class Aggregator {
 public:
  void clear();
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

class VaultWriter {
 public:
  VaultWriter(Collection* collection, VaultFileRef ref);
  const VaultFileRef& vault_ref() const { return ref_; }

  roo_io::Status openNew();

  roo_io::Status openExisting(int write_index);

  void close() { writer_.close(); }

  int write_index() const { return write_index_; }

  void writeEmptyData();

  void writeLogData(const std::vector<LogSample>& data);
  void writeAggregatedData(const Aggregator& aggregator);

  bool ok() const { return writer_.ok(); }

  roo_io::Status status() const { return writer_.status(); }

 private:
  void writeHeader();

  const Collection* collection_;
  VaultFileRef ref_;
  int write_index_;
  roo_io::OutputStreamWriter writer_;
};

}  // namespace roo_monitoring