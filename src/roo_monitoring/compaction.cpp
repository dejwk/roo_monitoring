#include "compaction.h"

#include "common.h"
#include "roo_io/data/output_stream_writer.h"
#include "roo_io/fs/fsutil.h"
#include "roo_logging.h"

#ifndef MLOG_roo_monitoring_compaction
#define MLOG_roo_monitoring_compaction 0
#endif

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

roo_io::Status VaultWriter::openNew() {
  String path;
  collection_->getVaultFilePath(ref_, &path);
  roo_io::Mount fs = collection_->fs().mount();
  if (!fs.ok()) return fs.status();
  roo_io::Status result = roo_io::MkParentDirRecursively(fs, path.c_str());
  if (result != roo_io::kOk && result != roo_io::kDirectoryExists) {
    return result;
  }
  MLOG(roo_monitoring_compaction)
      << "Opening a new vault file " << path.c_str() << " for write";
  writer_.reset(fs.fopenForWrite(path.c_str(), roo_io::kTruncateIfExists));
  write_index_ = 0;
  writeHeader();
  if (!writer_.ok()) {
    LOG(ERROR) << "Failed to open vault file " << path.c_str()
               << " for write: " << roo_io::StatusAsString(writer_.status());
  }
  return writer_.status();
}

roo_io::Status VaultWriter::openExisting(int write_index) {
  CHECK_GE(write_index, 0);
  CHECK_LT(write_index, kRangeElementCount);
  String path;
  collection_->getVaultFilePath(ref_, &path);
  MLOG(roo_monitoring_compaction)
      << "Opening an existing vault file " << path.c_str() << " for append";
  roo_io::Mount fs = collection_->fs().mount();
  if (!fs.ok()) return fs.status();
  writer_.reset(fs.fopenForWrite(path.c_str(), roo_io::kAppendIfExists));
  write_index_ = write_index;
  if (!writer_.ok()) {
    LOG(ERROR) << "Failed to open vault file " << path.c_str()
               << " for append: " << roo_io::StatusAsString(writer_.status());
  }
  return writer_.status();
}

void VaultWriter::writeEmptyData() {
  CHECK_LE(write_index_, kRangeElementCount);
  writer_.writeVarU64(0);
  if (!writer_.ok()) {
    LOG(ERROR) << "Failed to write empty data at index " << write_index_ << ": "
               << roo_io::StatusAsString(writer_.status());
  }
  ++write_index_;
}

void VaultWriter::writeLogData(const std::vector<LogSample>& data) {
  CHECK_LE(write_index_, kRangeElementCount);
  writer_.writeVarU64(data.size());
  for (const auto& sample : data) {
    writer_.writeVarU64(sample.stream_id());
    // Write the 'average'
    writer_.writeBeU16(sample.value());
    // Write the 'min'
    writer_.writeBeU16(sample.value());
    // Write the 'max'
    writer_.writeBeU16(sample.value());
    // Write the 'fill ratio'
    writer_.writeBeU16(0x2000);
  }
  if (!writer_.ok()) {
    LOG(ERROR) << "Failed to write real data (" << data.size() << ") at index "
               << write_index_ << ": "
               << roo_io::StatusAsString(writer_.status());
  }
  ++write_index_;
}

void VaultWriter::writeAggregatedData(const Aggregator& data) {
  CHECK_LE(write_index_, kRangeElementCount);
  writer_.writeVarU64(data.data_.size());
  for (const auto& entry : data.index_) {
    const Aggregator::SampleAggregator& sample = data.data_[entry.second];
    // uint16_t fill = sample.weight / 4;
    writer_.writeVarU64(entry.first);
    // Write the 'average'
    writer_.writeBeU16(sample.weight > 0 ? sample.weighted_total / sample.weight
                                         : 0);
    // Write the 'min'
    writer_.writeBeU16(sample.min_value);
    // Write the 'max'
    writer_.writeBeU16(sample.max_value);
    // Write the 'fill ratio'
    writer_.writeBeU16(sample.weight / 4);  // Can become zero.
    if (!writer_.ok()) {
      LOG(ERROR) << "Failed to write aggregated data (" << data.data_.size()
                 << ") at index " << write_index_ << ": "
                 << roo_io::Status(writer_.status());
      return;
    }
  }
  ++write_index_;
}

void VaultWriter::writeHeader() {
  CHECK_EQ(0, write_index_);
  writer_.writeU8(0x01);
  writer_.writeU8(0x01);
}

}  // namespace roo_monitoring