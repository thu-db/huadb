#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "common/typedefs.h"
#include "storage/disk.h"
#include "storage/lru_buffer_strategy.h"
#include "storage/page.h"

namespace huadb {

struct BufferPoolEntry {
  oid_t db_oid_;
  oid_t table_oid_;
  pageid_t page_id_;
  std::shared_ptr<Page> page_;
};

class LogManager;

class BufferPool {
 public:
  BufferPool(Disk &disk, LogManager &log_manager);

  std::shared_ptr<Page> GetPage(oid_t db_oid, oid_t table_oid, pageid_t page_id);
  std::shared_ptr<Page> NewPage(oid_t db_oid, oid_t table_oid, pageid_t page_id);
  // 将所有页面刷到磁盘
  void Flush(bool regular_only = false);
  // 清空 buffer pool，不刷脏，用于数据库故障模拟
  void Clear();

 private:
  // 将页面加入 buffer pool
  void AddToBuffer(oid_t db_oid, oid_t table_oid, pageid_t page_id, std::shared_ptr<Page> page);
  // 将 buffer 中对应的页面刷到磁盘
  void FlushPage(size_t frame_id);
  // 将 systable_buffer 中对应的页面刷到磁盘
  void FlushSysTablePage(size_t frame_id);

  Disk &disk_;
  LogManager &log_manager_;
  std::unique_ptr<BufferStrategy> buffer_strategy_;  // 缓存替换策略

  // 普通表缓存
  std::vector<BufferPoolEntry> buffers_;
  // page_id 到 buffer pool 中下标的映射
  std::unordered_map<TablePageid, size_t> hashmap_;
  // 系统表专用缓存
  std::vector<BufferPoolEntry> systable_buffers_;
  // 系统表专用映射
  std::unordered_map<TablePageid, size_t> systable_hashmap_;
};

}  // namespace huadb
