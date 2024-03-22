#include "common/bitmap.h"

#include <cstring>

#include "common/exceptions.h"

namespace huadb {

Bitmap::Bitmap(db_size_t size) : size_(size) { Resize(size); }

void Bitmap::Resize(db_size_t size) {
  size_ = size;
  bits_.resize((size + 7) / 8, 0);
}

void Bitmap::Set(db_size_t index) {
  if (index >= bits_.size() * 8) {
    throw DbException("Index out of range in bitmap Set");
  }
  bits_[index / 8] |= (1U << (index % 8));
}

void Bitmap::Clear(db_size_t index) {
  if (index >= bits_.size() * 8) {
    throw DbException("Index out of range in bitmap Clear");
  }
  bits_[index / 8] &= ~(1U << (index % 8));
}

bool Bitmap::Test(db_size_t index) const {
  if (index >= bits_.size() * 8) {
    throw DbException("Index out of range in bitmap Test");
  }
  return (bits_[index / 8] & (1U << (index % 8))) != 0;
}

db_size_t Bitmap::GetSize() const { return size_; }

db_size_t Bitmap::GetBytes() const { return bits_.size(); }

db_size_t Bitmap::SerializeTo(char *data) const {
  memcpy(data, bits_.data(), GetBytes());
  return GetBytes();
}

db_size_t Bitmap::DeserializeFrom(const char *data) {
  db_size_t offset = 0;
  for (db_size_t i = 0; i < GetBytes(); i++) {
    memcpy(&bits_[i], data + offset, sizeof(uint8_t));
    offset += sizeof(uint8_t);
  }
  return offset;
}

}  // namespace huadb
