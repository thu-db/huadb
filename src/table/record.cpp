#include "table/record.h"

#include <cassert>

namespace huadb {

Record::Record(std::vector<Value> values, Rid rid)
    : null_bitmap_(values.size()), values_(std::move(values)), rid_(rid) {
  for (size_t i = 0; i < values_.size(); i++) {
    if (values_[i].IsNull()) {
      null_bitmap_.Set(i);
    }
  }
}

void Record::Append(const Record &record) {
  for (const auto &value : record.GetValues()) {
    values_.push_back(value);
  }
  null_bitmap_.Resize(null_bitmap_.GetSize() + values_.size());
}

Value Record::GetValue(size_t col_idx) const { return values_[col_idx]; }

void Record::SetValue(size_t col_idx, const Value &value) { values_[col_idx] = value; }

const std::vector<Value> &Record::GetValues() const { return values_; }

db_size_t Record::GetSize() const {
  auto size = RECORD_HEADER_SIZE;
  size += null_bitmap_.GetBytes();
  for (const auto &value : values_) {
    if (value.IsNull()) {
      continue;
    }
    size += value.GetSize();
    if (TypeUtil::IsString(value.GetType())) {
      size += 2;
    }
  }
  return size;
}

std::string Record::ToString() {
  std::string result;
  for (const auto &value : values_) {
    result += value.ToString();
    result += ", ";
  }
  return result;
}

db_size_t Record::SerializeTo(char *data) const {
  auto offset = header_.SerializeTo(data);
  offset += null_bitmap_.SerializeTo(data + offset);
  for (const auto &value : values_) {
    if (value.IsNull()) {
      continue;
    }
    offset += value.SerializeTo(data + offset);
  }
  assert(offset == GetSize());
  return offset;
}

db_size_t Record::DeserializeFrom(const char *data, const ColumnList &column_list) {
  auto offset = header_.DeserializeFrom(data);
  null_bitmap_.Resize(column_list.Length());
  offset += null_bitmap_.DeserializeFrom(data + offset);
  auto columns = column_list.GetColumns();
  for (size_t i = 0; i < columns.size(); i++) {
    if (null_bitmap_.Test(i)) {
      values_.push_back(Value());
    } else {
      auto value = Value(columns[i].type_, columns[i].max_size_);
      offset += value.DeserializeFrom(data + offset);
      values_.push_back(value);
    }
  }
  assert(offset == GetSize());
  return offset;
}

void Record::SerializeHeaderTo(char *data) const { header_.SerializeTo(data); }

void Record::DeserializeHeaderFrom(const char *data) { header_.DeserializeFrom(data); }

bool Record::IsDeleted() const { return header_.deleted_; }

xid_t Record::GetXmin() const { return header_.xmin_; }

xid_t Record::GetXmax() const { return header_.xmax_; }

cid_t Record::GetCid() const { return header_.cid_; }

void Record::SetDeleted(bool deleted) { header_.deleted_ = deleted; }

void Record::SetXmin(xid_t xmin) { header_.xmin_ = xmin; }

void Record::SetXmax(xid_t xmax) { header_.xmax_ = xmax; }

void Record::SetCid(cid_t cid) { header_.cid_ = cid; }

Rid Record::GetRid() const { return rid_; }

void Record::SetRid(Rid rid) { rid_ = rid; }

}  // namespace huadb
