#include "storage/page.h"

#include "common/constants.h"

namespace huadb {

Page::Page() { data_ = new char[DB_PAGE_SIZE]; }

Page::~Page() { delete[] data_; }

void Page::SetDirty() { is_dirty_ = true; }

bool Page::IsDirty() const { return is_dirty_; }

char *Page::GetData() const { return data_; }

}  // namespace huadb
