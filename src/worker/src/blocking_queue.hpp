#pragma once

#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>

namespace dte {

template <typename T>
class BlockingQueue {
 public:
  void push(T value) {
    {
      std::lock_guard<std::mutex> lk(m_);
      if (closed_) return;
      q_.push(std::move(value));
    }
    cv_.notify_one();
  }

  std::optional<T> pop() {
    std::unique_lock<std::mutex> lk(m_);
    cv_.wait(lk, [&] { return closed_ || !q_.empty(); });
    if (q_.empty()) return std::nullopt;
    T v = std::move(q_.front());
    q_.pop();
    return v;
  }

  void close() {
    {
      std::lock_guard<std::mutex> lk(m_);
      closed_ = true;
    }
    cv_.notify_all();
  }

  size_t size() const {
    std::lock_guard<std::mutex> lk(m_);
    return q_.size();
  }

 private:
  mutable std::mutex m_;
  std::condition_variable cv_;
  std::queue<T> q_;
  bool closed_ = false;
};

}  // namespace dte
