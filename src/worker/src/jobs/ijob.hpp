#pragma once

#include <atomic>
#include <nlohmann/json.hpp>
#include <string>

namespace dte {

struct JobContext {
  std::string task_id;
  const std::atomic<bool>& cancel;
};

class IJob {
 public:
  virtual ~IJob() = default;
  virtual nlohmann::json Run(const nlohmann::json& payload, JobContext& ctx) = 0;
};

}  // namespace dte
