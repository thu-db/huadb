#pragma once

#include <cstddef>

namespace huadb {

// 缓存替换策略的模板类
class BufferStrategy {
 public:
  virtual ~BufferStrategy() = default;
  // 页面访问接口
  virtual void Access(size_t frame_no) = 0;
  // 页面替换接口
  virtual size_t Evict() = 0;
};

}  // namespace huadb
