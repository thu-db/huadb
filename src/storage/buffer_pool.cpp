#include "storage/buffer_pool.h"

#include "common/constants.h"
#include "common/exceptions.h"
#include "log/log_manager.h"
#include "table/table_page.h"

namespace huadb {

BufferPool::BufferPool(Disk &disk, LogManager &log_manager) : disk_(disk), log_manager_(log_manager) {
  buffers_.reserve(BUFFER_SIZE);
  hashmap_.reserve(BUFFER_SIZE);
  buffer_strategy_ = std::make_unique<LRUBufferStrategy>();
}

std::shared_ptr<Page> BufferPool::GetPage(oid_t db_oid, oid_t table_oid, pageid_t page_id) {
  auto &buffers = (db_oid == SYSTEM_DATABASE_OID) ? systable_buffers_ : buffers_;
  auto &hashmap = (db_oid == SYSTEM_DATABASE_OID) ? systable_hashmap_ : hashmap_;
  auto entry = hashmap.find({table_oid, page_id});
  if (entry == hashmap.end()) {
    auto page = std::make_shared<Page>();
    disk_.ReadPage(Disk::GetFilePath(db_oid, table_oid), page_id, page->GetData());
    AddToBuffer(db_oid, table_oid, page_id, page);
    return page;
  } else {
    if (db_oid != SYSTEM_DATABASE_OID) {
      buffer_strategy_->Access(entry->second);
    }
    return buffers[entry->second].page_;
  }
}

std::shared_ptr<Page> BufferPool::NewPage(oid_t db_oid, oid_t table_oid, pageid_t page_id) {
  auto page = std::make_shared<Page>();
  AddToBuffer(db_oid, table_oid, page_id, page);
  return page;
}

void BufferPool::Flush(bool regular_only) {
  for (size_t i = 0; i < buffers_.size(); i++) {
    FlushPage(i);
  }
  buffers_.clear();
  if (!regular_only) {
    for (size_t i = 0; i < systable_buffers_.size(); i++) {
      FlushSysTablePage(i);
    }
    systable_buffers_.clear();
  }
}

void BufferPool::Clear() {
  buffers_.clear();
  hashmap_.clear();
  systable_buffers_.clear();
  systable_hashmap_.clear();
}

void BufferPool::AddToBuffer(oid_t db_oid, oid_t table_oid, pageid_t page_id, std::shared_ptr<Page> page) {
  if (db_oid == SYSTEM_DATABASE_OID) {
    systable_hashmap_[{table_oid, page_id}] = systable_buffers_.size();
    systable_buffers_.push_back({db_oid, table_oid, page_id, page});
  } else {
    if (buffers_.size() == BUFFER_SIZE) {
      size_t victim = buffer_strategy_->Evict();
      FlushPage(victim);
      buffer_strategy_->Access(victim);
      buffers_[victim] = {db_oid, table_oid, page_id, page};
      hashmap_[{table_oid, page_id}] = victim;
    } else {
      buffer_strategy_->Access(buffers_.size());
      hashmap_[{table_oid, page_id}] = buffers_.size();
      buffers_.push_back({db_oid, table_oid, page_id, page});
    }
  }
}

void BufferPool::FlushPage(size_t frame_id) {
  auto &buffer_entry = buffers_[frame_id];
  if (buffer_entry.page_->IsDirty()) {
    auto table_page = std::make_unique<TablePage>(buffer_entry.page_);
    log_manager_.FlushPage(buffer_entry.table_oid_, buffer_entry.page_id_, table_page->GetPageLSN());
    assert(buffer_entry.db_oid_ != SYSTEM_DATABASE_OID);
    disk_.WritePage(Disk::GetFilePath(buffer_entry.db_oid_, buffer_entry.table_oid_), buffer_entry.page_id_,
                    buffer_entry.page_->GetData());
  }
  hashmap_.erase({buffer_entry.table_oid_, buffer_entry.page_id_});
}

void BufferPool::FlushSysTablePage(size_t frame_id) {
  auto &buffer_entry = systable_buffers_[frame_id];
  if (buffer_entry.page_->IsDirty()) {
    assert(buffer_entry.db_oid_ == SYSTEM_DATABASE_OID);
    disk_.WritePage(Disk::GetFilePath(buffer_entry.db_oid_, buffer_entry.table_oid_), buffer_entry.page_id_,
                    buffer_entry.page_->GetData());
  }
  systable_hashmap_.erase({buffer_entry.table_oid_, buffer_entry.page_id_});
}

}  // namespace huadb
