#pragma once

#include <cstdint>
#include <vector>

#include "common/types.h"

namespace huadb {

class Bitmap {
 public:
  Bitmap() = default;
  explicit Bitmap(db_size_t size);

  void Resize(db_size_t size);

  void Set(db_size_t index);
  void Clear(db_size_t index);
  bool Test(db_size_t index) const;

  // Bitmap 大小
  db_size_t GetSize() const;
  // Bitmap 占用字节数
  db_size_t GetBytes() const;

  db_size_t SerializeTo(char *data) const;
  db_size_t DeserializeFrom(const char *data);

 private:
  db_size_t size_ = 0;
  std::vector<uint8_t> bits_;
};

}  // namespace huadb
