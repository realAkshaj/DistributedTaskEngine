#include "executor_pool.hpp"

#include <chrono>
#include <nlohmann/json.hpp>

#include "jobs/ijob.hpp"
#include "logging.hpp"

namespace dte {

ExecutorPool::ExecutorPool(size_t threads, JobRegistry& registry, Sender send)
    : registry_(registry), send_(std::move(send)) {
  threads_.reserve(threads);
  for (size_t i = 0; i < threads; ++i) threads_.emplace_back(&ExecutorPool::Run, this);
}

ExecutorPool::~ExecutorPool() { Shutdown(); }

void ExecutorPool::Submit(dte::v1::Assignment a) { queue_.push(std::move(a)); }

void ExecutorPool::Shutdown() {
  queue_.close();
  cancel_all_.store(true, std::memory_order_relaxed);
  for (auto& t : threads_) if (t.joinable()) t.join();
  threads_.clear();
}

void ExecutorPool::Run() {
  while (auto opt = queue_.pop()) {
    auto a = std::move(*opt);
    const auto& task_id = a.task_id();

    dte::v1::WorkerMessage started;
    started.mutable_started()->set_task_id(task_id);
    send_(std::move(started));

    auto* job = registry_.Find(a.job_type());
    if (!job) {
      dte::v1::WorkerMessage failed;
      failed.mutable_failed()->set_task_id(task_id);
      failed.mutable_failed()->set_error("unknown job_type: " + a.job_type());
      send_(std::move(failed));
      continue;
    }

    auto start = std::chrono::steady_clock::now();
    JobContext ctx{task_id, cancel_all_};

    try {
      nlohmann::json payload = a.payload().empty()
          ? nlohmann::json::object()
          : nlohmann::json::parse(a.payload());
      auto result = job->Run(payload, ctx);
      auto wall_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count();

      dte::v1::WorkerMessage done;
      auto* c = done.mutable_completed();
      c->set_task_id(task_id);
      c->set_result(result.dump());
      c->mutable_metrics()->set_wall_ms(wall_ms);
      send_(std::move(done));
    } catch (const std::exception& ex) {
      LOG_WARN("job %s failed: %s", task_id.c_str(), ex.what());
      dte::v1::WorkerMessage failed;
      failed.mutable_failed()->set_task_id(task_id);
      failed.mutable_failed()->set_error(ex.what());
      send_(std::move(failed));
    }
  }
}

}  // namespace dte
