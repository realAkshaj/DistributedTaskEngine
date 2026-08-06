#pragma once

#include <atomic>
#include <functional>
#include <thread>
#include <vector>

#include "blocking_queue.hpp"
#include "dte.pb.h"
#include "job_registry.hpp"

namespace dte {

class ExecutorPool {
 public:
  using Sender = std::function<void(dte::v1::WorkerMessage)>;

  ExecutorPool(size_t threads, JobRegistry& registry, Sender send);
  ~ExecutorPool();

  void Submit(dte::v1::Assignment a);
  void Shutdown();  // stop accepting, drain in-flight, join

 private:
  void Run();

  JobRegistry& registry_;
  Sender send_;
  BlockingQueue<dte::v1::Assignment> queue_;
  std::atomic<bool> cancel_all_{false};
  std::vector<std::thread> threads_;
};

}  // namespace dte
