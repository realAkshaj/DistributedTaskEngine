#include "job_registry.hpp"

namespace dte {

void JobRegistry::Register(std::string name, std::unique_ptr<IJob> job) {
  jobs_.emplace(std::move(name), std::move(job));
}

IJob* JobRegistry::Find(const std::string& name) const {
  auto it = jobs_.find(name);
  return it == jobs_.end() ? nullptr : it->second.get();
}

std::vector<std::string> JobRegistry::Names() const {
  std::vector<std::string> names;
  names.reserve(jobs_.size());
  for (const auto& [k, _] : jobs_) names.push_back(k);
  return names;
}

}  // namespace dte
