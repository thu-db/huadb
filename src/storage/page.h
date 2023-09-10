#pragma once

namespace huadb {

class Page {
 public:
  Page();
  ~Page();
  void SetDirty();
  bool IsDirty() const;
  char *GetData() const;

 private:
  char *data_;
  bool is_dirty_ = false;
};

}  // namespace huadb
