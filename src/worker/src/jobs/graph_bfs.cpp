#include "jobs/graph_bfs.hpp"

#include <chrono>
#include <queue>
#include <stdexcept>
#include <vector>

namespace dte::jobs {

nlohmann::json GraphBfs::Run(const nlohmann::json& payload, JobContext& ctx) {
  int nodes = payload.value("nodes", 1000);
  int branching = payload.value("branching", 4);
  if (nodes < 1 || branching < 1) throw std::invalid_argument("nodes and branching must be >= 1");

  auto start = std::chrono::steady_clock::now();

  std::vector<std::vector<int>> adj(nodes);
  for (int i = 0; i < nodes; ++i) {
    for (int c = 1; c <= branching; ++c) {
      int child = i * branching + c;
      if (child >= nodes) break;
      adj[i].push_back(child);
    }
  }

  std::vector<int> depth(nodes, -1);
  std::queue<int> q;
  q.push(0);
  depth[0] = 0;
  int visited = 0;
  int max_depth = 0;

  while (!q.empty()) {
    if (ctx.cancel.load(std::memory_order_relaxed)) throw std::runtime_error("cancelled");
    int u = q.front();
    q.pop();
    ++visited;
    for (int v : adj[u]) {
      if (depth[v] == -1) {
        depth[v] = depth[u] + 1;
        max_depth = std::max(max_depth, depth[v]);
        q.push(v);
      }
    }
  }

  auto wall_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start)
                     .count();

  return {
      {"visited", visited},
      {"depth", max_depth},
      {"wall_ms", wall_ms},
      {"task_id", ctx.task_id},
  };
}

}  // namespace dte::jobs
