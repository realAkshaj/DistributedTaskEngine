#pragma once

#include "jobs/ijob.hpp"

namespace dte::jobs {

class GraphBfs : public IJob {
 public:
  nlohmann::json Run(const nlohmann::json& payload, JobContext& ctx) override;
};

}  // namespace dte::jobs
