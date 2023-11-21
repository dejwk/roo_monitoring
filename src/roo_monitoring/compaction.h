#pragma once

#include "roo_monitoring.h"

#include "log.h"
#include "stdint.h"

#include "datastream.h"

#include <map>

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

  std::ios_base::io_state openNew();

  std::ios_base::io_state openExisting(int write_index);

  void close() { os_.close(); }

  int write_index() const { return write_index_; }

  void writeEmptyData();

  void writeLogData(const std::vector<LogSample>& data);
  void writeAggregatedData(const Aggregator& aggregator);

  bool good() const { return os_.good(); }
  int my_errno() const { return os_.my_errno(); }

  const char* status() { return os_.status(); }

 private:
  void writeHeader();

  const Collection* collection_;
  VaultFileRef ref_;
  int write_index_;
  DataOutputStream os_;
};

}  // namespace roo_monitoring