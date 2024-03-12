#pragma once

#include <string>
#include <vector>

#include "catalog/column_list.h"
#include "common/bitmap.h"
#include "common/value.h"
#include "table/record_header.h"

namespace huadb {

class Record {
 public:
  Record() = default;
  explicit Record(std::vector<Value> values, Rid rid = {0, 0});
  // 记录合并，用于 join 算子
  void Append(const Record &record);
  // 获取第 col_idx 个 column 的值
  Value GetValue(size_t col_idx) const;
  // 设置第 col_idx 个 column 的值
  void SetValue(size_t col_idx, const Value &value);
  // 获取所有 column 的值
  const std::vector<Value> &GetValues() const;
  // 获取记录的大小
  db_size_t GetSize() const;
  // 将记录转换为字符串
  std::string ToString() const;
  // 记录序列化
  db_size_t SerializeTo(char *data) const;
  // 记录反序列化
  db_size_t DeserializeFrom(const char *data, const ColumnList &column_list);
  // 记录头序列化
  void SerializeHeaderTo(char *data) const;
  // 记录头反序列化
  void DeserializeHeaderFrom(const char *data);
  // 获取记录头信息
  bool IsDeleted() const;
  xid_t GetXmin() const;
  xid_t GetXmax() const;
  cid_t GetCid() const;

  // 设置记录头信息
  void SetDeleted(bool deleted);
  void SetXmin(xid_t xmin);
  void SetXmax(xid_t xmax);
  void SetCid(cid_t cid);

  // 获取 rid
  Rid GetRid() const;
  // 设置 rid
  void SetRid(Rid rid);

 private:
  void UpdateSize();

  // 空值位图
  Bitmap null_bitmap_;
  std::vector<Value> values_;
  RecordHeader header_;
  Rid rid_;
  db_size_t size_;
};

}  // namespace huadb
