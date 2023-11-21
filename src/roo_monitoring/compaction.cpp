#include "compaction.h"
#include "common.h"
#include "datastream.h"

#include "roo_glog/logging.h"

namespace roo_monitoring {

void Aggregator::clear() {
  data_.clear();
  index_.clear();
}

void Aggregator::add(const Sample& input) {
  int idx;
  auto pos = index_.find(input.stream_id());
  if (pos == index_.end()) {
    idx = data_.size();
    data_.emplace_back();
    index_.insert(std::make_pair(input.stream_id(), idx));
  } else {
    idx = pos->second;
  }
  SampleAggregator& output = data_[idx];
  output.weighted_total += (input.avg_value() * input.fill());
  output.weight += input.fill();
  if (output.min_value > input.min_value()) {
    output.min_value = input.min_value();
  }
  if (output.max_value < input.max_value()) {
    output.max_value = input.max_value();
  }
}

VaultWriter::VaultWriter(Collection* collection, VaultFileRef ref)
    : collection_(collection), ref_(ref), write_index_(0) {}

std::ios_base::io_state VaultWriter::openNew() {
  String path;
  collection_->getVaultFilePath(ref_, &path);
  if (!recursiveMkDir(path.c_str())) {
    return std::ios_base::failbit;
  }
  LOG(INFO) << "Opening a new vault file " << path.c_str() << " for write";
  os_.open(path.c_str(), std::ios_base::out);
  write_index_ = 0;
  writeHeader();
  if (!os_.good()) {
    LOG(ERROR) << "Failed to open vault file " << path.c_str()
               << " for write: " << strerror(os_.my_errno());
  }
  return os_.rdstate();
}

std::ios_base::io_state VaultWriter::openExisting(int write_index) {
  CHECK_GE(write_index, 0);
  CHECK_LT(write_index, kRangeElementCount);
  String path;
  collection_->getVaultFilePath(ref_, &path);
  LOG(INFO) << "Opening an existing vault file " << path.c_str()
            << " for append";
  os_.open(path.c_str(), std::ios_base::app);
  write_index_ = write_index;
  if (!os_.good()) {
    LOG(ERROR) << "Failed to open vault file " << path.c_str()
               << " for append: " << strerror(os_.my_errno());
  }
  return os_.rdstate();
}

void VaultWriter::writeEmptyData() {
  CHECK_LE(write_index_, kRangeElementCount);
  os_.write_varint(0);
  if (!os_.good()) {
    LOG(ERROR) << "Failed to write empty data at index " << write_index_ << ": "
               << strerror(os_.my_errno());
  }
  ++write_index_;
}

void VaultWriter::writeLogData(const std::vector<LogSample>& data) {
  CHECK_LE(write_index_, kRangeElementCount);
  os_.write_varint(data.size());
  for (const auto& sample : data) {
    os_.write_varint(sample.stream_id());
    // Write the 'average'
    os_.write_uint16(sample.value());
    // Write the 'min'
    os_.write_uint16(sample.value());
    // Write the 'max'
    os_.write_uint16(sample.value());
    // Write the 'fill ratio'
    os_.write_uint16(0x2000);
  }
  if (!os_.good()) {
    LOG(ERROR) << "Failed to write real data (" << data.size() << ") at index "
               << write_index_ << ": " << strerror(os_.my_errno());
  }
  ++write_index_;
}

void VaultWriter::writeAggregatedData(const Aggregator& data) {
  CHECK_LE(write_index_, kRangeElementCount);
  os_.write_varint(data.data_.size());
  for (const auto& entry : data.index_) {
    const Aggregator::SampleAggregator& sample = data.data_[entry.second];
    // uint16_t fill = sample.weight / 4;
    os_.write_varint(entry.first);
    // Write the 'average'
    os_.write_uint16(sample.weight > 0 ? sample.weighted_total / sample.weight
                                       : 0);
    // Write the 'min'
    os_.write_uint16(sample.min_value);
    // Write the 'max'
    os_.write_uint16(sample.max_value);
    // Write the 'fill ratio'
    os_.write_uint16(sample.weight / 4);  // Can become zero.
    if (!os_.good()) {
      LOG(ERROR) << "Failed to write aggregated data (" << data.data_.size()
                 << ") at index " << write_index_ << ": "
                 << strerror(os_.my_errno());
      return;
    }
  }
  ++write_index_;
}

void VaultWriter::writeHeader() {
  CHECK_EQ(0, write_index_);
  os_.write_uint8(0x01);
  os_.write_uint8(0x01);
}

}  // namespace roo_monitoring