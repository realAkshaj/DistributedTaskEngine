#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "executor_pool.hpp"
#include "job_registry.hpp"
#include "jobs/graph_bfs.hpp"
#include "jobs/noop.hpp"
#include "logging.hpp"
#include "orchestrator_client.hpp"

namespace {

std::atomic<bool> g_stop{false};

void on_signal(int) { g_stop.store(true); }

std::string env_or(const char* name, std::string fallback) {
  const char* v = std::getenv(name);
  return (v && *v) ? std::string(v) : std::move(fallback);
}

int env_int(const char* name, int fallback) {
  const char* v = std::getenv(name);
  if (!v || !*v) return fallback;
  try { return std::stoi(v); } catch (...) { return fallback; }
}

std::vector<std::string> split_csv(const std::string& s) {
  std::vector<std::string> out;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, ',')) {
    if (!item.empty()) out.push_back(item);
  }
  return out;
}

std::string generate_worker_id() {
  std::ifstream in("/proc/sys/kernel/random/uuid");
  std::string uuid;
  std::getline(in, uuid);
  return uuid.empty() ? "00000000-0000-0000-0000-000000000000" : uuid;
}

}  // namespace

int main() {
  std::signal(SIGINT,  on_signal);
  std::signal(SIGTERM, on_signal);

  auto address     = env_or("DTE_ORCHESTRATOR", "orchestrator:5001");
  auto worker_id   = env_or("DTE_WORKER_ID", generate_worker_id());
  int  max_parallel = env_int("DTE_MAX_PARALLEL", 4);
  auto job_types    = split_csv(env_or("DTE_JOB_TYPES", "graph.bfs,noop"));

  LOG_INFO("dte-worker starting: id=%s, parallel=%d, orchestrator=%s",
           worker_id.c_str(), max_parallel, address.c_str());

  dte::JobRegistry registry;
  registry.Register("graph.bfs", std::make_unique<dte::jobs::GraphBfs>());
  registry.Register("noop",      std::make_unique<dte::jobs::Noop>());

  dte::OrchestratorClient client(address, worker_id, max_parallel, job_types);

  dte::ExecutorPool executor(
      static_cast<size_t>(max_parallel), registry,
      [&](dte::v1::WorkerMessage msg) { client.Send(std::move(msg)); });

  client.Start([&](dte::v1::Assignment a) { executor.Submit(std::move(a)); });

  while (!g_stop.load() && client.Running()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  LOG_INFO("shutting down");
  client.Shutdown();
  executor.Shutdown();
  LOG_INFO("bye");
  return 0;
}
