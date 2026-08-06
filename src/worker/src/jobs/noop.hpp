#pragma once

#include "jobs/ijob.hpp"

namespace dte::jobs {

class Noop : public IJob {
 public:
  nlohmann::json Run(const nlohmann::json& payload, JobContext& ctx) override;
};

}  // namespace dte::jobs
