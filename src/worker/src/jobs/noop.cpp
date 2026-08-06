#include "jobs/noop.hpp"

#include <chrono>
#include <thread>

namespace dte::jobs {

nlohmann::json Noop::Run(const nlohmann::json& payload, JobContext&) {
  int sleep_ms = payload.value("sleep_ms", 0);
  if (sleep_ms > 0) std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
  return {{"ok", true}, {"slept_ms", sleep_ms}};
}

}  // namespace dte::jobs
