#include "table/record.h"

#include <cassert>

#include "common/exceptions.h"

namespace huadb {

Record::Record(std::vector<Value> values, Rid rid)
    : null_bitmap_(values.size()), values_(std::move(values)), rid_(rid) {
  for (size_t i = 0; i < values_.size(); i++) {
    if (values_[i].IsNull()) {
      null_bitmap_.Set(i);
    }
  }
  UpdateSize();
}

void Record::Append(const Record &record) {
  null_bitmap_.Resize(null_bitmap_.GetSize() + record.GetValues().size());
  for (const auto &value : record.GetValues()) {
    if (value.IsNull()) {
      null_bitmap_.Set(values_.size());
    } else {
      null_bitmap_.Clear(values_.size());
    }
    values_.push_back(value);
  }
  UpdateSize();
}

Value Record::GetValue(size_t col_idx) const {
  if (col_idx >= values_.size()) {
    throw DbException("Column index out of range");
  }
  return values_[col_idx];
}

void Record::SetValue(size_t col_idx, const Value &value) {
  values_[col_idx] = value;
  if (value.IsNull()) {
    null_bitmap_.Set(col_idx);
  } else {
    null_bitmap_.Clear(col_idx);
  }
  UpdateSize();
}

const std::vector<Value> &Record::GetValues() const { return values_; }

db_size_t Record::GetSize() const { return size_; }

std::string Record::ToString() const {
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
  UpdateSize();
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

void Record::UpdateSize() {
  size_ = RECORD_HEADER_SIZE;
  size_ += null_bitmap_.GetBytes();
  for (const auto &value : values_) {
    if (value.IsNull()) {
      continue;
    }
    size_ += value.GetSize();
    if (TypeUtil::IsString(value.GetType())) {
      size_ += 2;
    }
  }
}

}  // namespace huadb
