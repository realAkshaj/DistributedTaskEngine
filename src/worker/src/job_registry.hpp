#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "jobs/ijob.hpp"

namespace dte {

class JobRegistry {
 public:
  void Register(std::string name, std::unique_ptr<IJob> job);
  IJob* Find(const std::string& name) const;
  std::vector<std::string> Names() const;

 private:
  std::unordered_map<std::string, std::unique_ptr<IJob>> jobs_;
};

}  // namespace dte
