#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "blocking_queue.hpp"
#include "dte.grpc.pb.h"

namespace dte {

class OrchestratorClient {
 public:
  using AssignmentHandler = std::function<void(dte::v1::Assignment)>;

  OrchestratorClient(std::string address, std::string worker_id, int max_parallel,
                     std::vector<std::string> job_types);
  ~OrchestratorClient();

  void Start(AssignmentHandler on_assignment);
  void Send(dte::v1::WorkerMessage msg);   // thread-safe
  void Shutdown();                          // cancels stream, joins threads
  bool Running() const { return running_.load(); }

 private:
  void ReaderLoop();
  void WriterLoop();
  void HeartbeatLoop();

  std::string address_;
  std::string worker_id_;
  int max_parallel_;
  std::vector<std::string> job_types_;

  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<dte::v1::TaskDispatch::Stub> stub_;
  std::unique_ptr<grpc::ClientContext> ctx_;
  std::unique_ptr<grpc::ClientReaderWriter<dte::v1::WorkerMessage, dte::v1::OrchestratorMessage>> stream_;

  BlockingQueue<dte::v1::WorkerMessage> outbound_;
  AssignmentHandler on_assignment_;

  std::atomic<bool> running_{false};
  std::atomic<bool> shutdown_requested_{false};
  std::thread reader_, writer_, heartbeat_;
  std::mutex shutdown_mu_;
};

}  // namespace dte
