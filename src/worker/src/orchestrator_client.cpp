#include "orchestrator_client.hpp"

#include <chrono>

#include "logging.hpp"

namespace dte {

OrchestratorClient::OrchestratorClient(std::string address, std::string worker_id,
                                       int max_parallel, std::vector<std::string> job_types)
    : address_(std::move(address)),
      worker_id_(std::move(worker_id)),
      max_parallel_(max_parallel),
      job_types_(std::move(job_types)) {}

OrchestratorClient::~OrchestratorClient() { Shutdown(); }

void OrchestratorClient::Start(AssignmentHandler on_assignment) {
  on_assignment_ = std::move(on_assignment);
  channel_ = grpc::CreateChannel(address_, grpc::InsecureChannelCredentials());
  stub_ = dte::v1::TaskDispatch::NewStub(channel_);

  int attempt = 0;
  while (!shutdown_requested_.load()) {
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(3);
    if (!channel_->WaitForConnected(deadline)) {
      ++attempt;
      int backoff = std::min(15, 1 << std::min(attempt, 4));
      LOG_WARN("channel not ready (attempt %d); retrying in %ds", attempt, backoff);
      std::this_thread::sleep_for(std::chrono::seconds(backoff));
      continue;
    }

    ctx_ = std::make_unique<grpc::ClientContext>();
    stream_ = stub_->Stream(ctx_.get());

    dte::v1::WorkerMessage hello;
    auto* h = hello.mutable_hello();
    h->set_worker_id(worker_id_);
    h->set_version("cpp-0.1");
    h->set_max_parallel(max_parallel_);
    for (auto& jt : job_types_) h->add_job_types(jt);
    if (stream_->Write(hello)) break;

    LOG_WARN("hello write failed; resetting stream");
    stream_.reset();
    ctx_.reset();
    ++attempt;
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  if (shutdown_requested_.load()) return;

  running_.store(true);
  reader_    = std::thread(&OrchestratorClient::ReaderLoop, this);
  writer_    = std::thread(&OrchestratorClient::WriterLoop, this);
  heartbeat_ = std::thread(&OrchestratorClient::HeartbeatLoop, this);
}

void OrchestratorClient::Send(dte::v1::WorkerMessage msg) {
  if (!shutdown_requested_.load()) outbound_.push(std::move(msg));
}

void OrchestratorClient::Shutdown() {
  {
    std::lock_guard<std::mutex> lk(shutdown_mu_);
    if (shutdown_requested_.exchange(true)) return;
  }
  outbound_.close();
  if (ctx_) ctx_->TryCancel();
  if (reader_.joinable())    reader_.join();
  if (writer_.joinable())    writer_.join();
  if (heartbeat_.joinable()) heartbeat_.join();
  if (stream_) {
    stream_->WritesDone();
    stream_->Finish();
  }
  running_.store(false);
}

void OrchestratorClient::ReaderLoop() {
  dte::v1::OrchestratorMessage msg;
  while (stream_->Read(&msg)) {
    switch (msg.kind_case()) {
      case dte::v1::OrchestratorMessage::kWelcome:
        LOG_INFO("welcome from orchestrator (heartbeat=%dms)",
                 msg.welcome().heartbeat_interval_ms());
        break;
      case dte::v1::OrchestratorMessage::kAssignment:
        if (on_assignment_) on_assignment_(std::move(*msg.mutable_assignment()));
        break;
      case dte::v1::OrchestratorMessage::kCancel:
        LOG_INFO("cancel request for task %s", msg.cancel().task_id().c_str());
        break;
      case dte::v1::OrchestratorMessage::kShutdown:
        LOG_INFO("shutdown request from orchestrator");
        shutdown_requested_.store(true);
        return;
      default:
        break;
    }
  }
  LOG_INFO("stream reader ended");
  running_.store(false);
}

void OrchestratorClient::WriterLoop() {
  while (auto opt = outbound_.pop()) {
    if (!stream_->Write(*opt)) {
      LOG_WARN("stream write failed; dropping outbound message");
      return;
    }
  }
}

void OrchestratorClient::HeartbeatLoop() {
  using namespace std::chrono_literals;
  while (!shutdown_requested_.load()) {
    for (int i = 0; i < 50 && !shutdown_requested_.load(); ++i)
      std::this_thread::sleep_for(100ms);
    if (shutdown_requested_.load()) break;
    dte::v1::WorkerMessage hb;
    hb.mutable_heartbeat();
    outbound_.push(std::move(hb));
  }
}

}  // namespace dte
