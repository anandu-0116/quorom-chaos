#include "raft_node.h"

#include <chrono>
#include <random>
#include <thread>

#include "logger.h"

namespace quorumchaos {

namespace {
constexpr auto kElectionTimeoutMin = std::chrono::milliseconds(150);
constexpr auto kElectionTimeoutMax = std::chrono::milliseconds(300);
constexpr auto kHeartbeatInterval = std::chrono::milliseconds(50);
constexpr auto kRpcDeadline = std::chrono::milliseconds(100);

// state_ is constructed in the member-init list below, before the
// constructor body runs -- and PersistentState::Load() logs. Route the
// self_id_str_ initializer through this so Logger::Init() has already run
// by the time state_'s constructor (and its log line) executes.
std::string InitLoggerAndFormatId(int self_id) {
  Logger::Init(self_id);
  return std::to_string(self_id);
}
}  // namespace

RaftNode::RaftNode(NodeConfig config)
    : config_(std::move(config)),
      self_id_str_(InitLoggerAndFormatId(config_.self_id)),
      state_(config_.data_dir + "/node" + self_id_str_ + ".state"),
      election_timer_(kElectionTimeoutMin, kElectionTimeoutMax,
                       std::random_device{}(),
                       [this] { OnElectionTimeout(); }),
      heartbeat_timer_(kHeartbeatInterval, kHeartbeatInterval,
                        std::random_device{}(),
                        [this] { OnHeartbeatTick(); }) {
  for (const auto& peer : config_.peers) {
    stubs_[peer.id] = Raft::NewStub(grpc::CreateChannel(
        peer.address, grpc::InsecureChannelCredentials()));
  }
}

void RaftNode::Run() {
  election_timer_.Start();
  heartbeat_timer_.Start();

  grpc::ServerBuilder builder;
  builder.AddListeningPort(config_.self_address,
                            grpc::InsecureServerCredentials());
  builder.RegisterService(this);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  QC_LOG("listening on %s with %zu peer(s)", config_.self_address.c_str(),
      config_.peers.size());
  server->Wait();
}

const char* RaftNode::RoleName(Role role) {
  switch (role) {
    case Role::Follower:
      return "Follower";
    case Role::Candidate:
      return "Candidate";
    case Role::Leader:
      return "Leader";
  }
  return "Unknown";
}

void RaftNode::SetRoleLocked(Role role) {
  if (role_ != role) {
    QC_LOG("%s -> %s (term %lld)", RoleName(role_), RoleName(role),
        static_cast<long long>(state_.current_term()));
  }
  role_ = role;
}

void RaftNode::BecomeCandidateLocked() {
  int64_t new_term = state_.current_term() + 1;
  state_.SetTermAndVotedFor(new_term, self_id_str_);
  votes_received_ = 1;  // vote for self
  SetRoleLocked(Role::Candidate);
}

void RaftNode::StepDownToFollowerLocked(int64_t new_term) {
  if (new_term > state_.current_term()) {
    state_.SetTermAndVotedFor(new_term, std::nullopt);
  }
  SetRoleLocked(Role::Follower);
}

bool RaftNode::HandleRequestVoteReplyLocked(int peer_id, int64_t request_term,
                                             const RequestVoteReply& reply,
                                             int total_nodes) {
  if (reply.term() > state_.current_term()) {
    StepDownToFollowerLocked(reply.term());
    return false;
  }
  // Ignore stale replies: either we're no longer a Candidate, or a newer
  // election has since started in a later term.
  if (role_ != Role::Candidate || state_.current_term() != request_term) {
    return false;
  }
  if (!reply.vote_granted()) {
    return false;
  }
  ++votes_received_;
  QC_LOG("received vote from node%d (%d/%d)", peer_id, votes_received_,
      total_nodes);
  if (votes_received_ * 2 > total_nodes) {
    SetRoleLocked(Role::Leader);
    return true;
  }
  return false;
}

grpc::Status RaftNode::RequestVote(grpc::ServerContext* /*context*/,
                                    const RequestVoteArgs* request,
                                    RequestVoteReply* reply) {
  std::lock_guard<std::mutex> lock(mutex_);
  const int64_t term = request->term();

  if (term < state_.current_term()) {
    reply->set_term(state_.current_term());
    reply->set_vote_granted(false);
    return grpc::Status::OK;
  }
  if (term > state_.current_term()) {
    StepDownToFollowerLocked(term);
  }

  const bool grant = !state_.voted_for().has_value() ||
                      state_.voted_for().value() == request->candidate_id();
  if (grant) {
    // Persist the vote before replying -- a crash right after this call
    // returns can never leave disk behind what we're about to tell the
    // candidate.
    state_.SetTermAndVotedFor(state_.current_term(), request->candidate_id());
    election_timer_.Reset();  // valid RPC: granted a vote to a candidate
    QC_LOG("granted vote to node%s for term %lld", request->candidate_id().c_str(),
        static_cast<long long>(state_.current_term()));
  }

  reply->set_term(state_.current_term());
  reply->set_vote_granted(grant);
  return grpc::Status::OK;
}

grpc::Status RaftNode::AppendEntries(grpc::ServerContext* /*context*/,
                                      const AppendEntriesArgs* request,
                                      AppendEntriesReply* reply) {
  std::lock_guard<std::mutex> lock(mutex_);
  const int64_t term = request->term();

  if (term < state_.current_term()) {
    reply->set_term(state_.current_term());
    reply->set_success(false);
    return grpc::Status::OK;
  }
  if (term > state_.current_term()) {
    StepDownToFollowerLocked(term);
  } else if (role_ != Role::Follower) {
    // Same term, but we were Candidate (lost this election) or -- in
    // correct Raft, shouldn't happen -- another Leader. Either way, a
    // heartbeat at the current term means someone else already won it.
    SetRoleLocked(Role::Follower);
  }

  election_timer_.Reset();  // valid RPC: heartbeat from the current leader
  reply->set_term(state_.current_term());
  reply->set_success(true);
  return grpc::Status::OK;
}

void RaftNode::OnElectionTimeout() {
  int64_t term;
  int total_nodes;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (role_ == Role::Leader) return;  // heartbeats keep leaders from firing
    BecomeCandidateLocked();
    term = state_.current_term();
    total_nodes = static_cast<int>(config_.peers.size()) + 1;
  }
  QC_LOG("election timeout -> starting election for term %lld",
      static_cast<long long>(term));
  BroadcastRequestVote(term, total_nodes);
}

void RaftNode::OnHeartbeatTick() {
  int64_t term;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (role_ != Role::Leader) return;
    term = state_.current_term();
  }
  BroadcastHeartbeats(term);
}

void RaftNode::BroadcastRequestVote(int64_t term, int total_nodes) {
  for (const auto& peer : config_.peers) {
    std::thread(&RaftNode::SendRequestVoteToPeer, this, peer, term,
                total_nodes)
        .detach();
  }
}

void RaftNode::SendRequestVoteToPeer(PeerAddress peer, int64_t term,
                                      int total_nodes) {
  RequestVoteArgs request;
  request.set_term(term);
  request.set_candidate_id(self_id_str_);
  RequestVoteReply reply;

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + kRpcDeadline);
  grpc::Status status =
      stubs_[peer.id]->RequestVote(&context, request, &reply);
  if (!status.ok()) {
    return;  // peer unreachable or timed out this round -- just skip it
  }

  bool became_leader = false;
  int64_t leader_term = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    became_leader =
        HandleRequestVoteReplyLocked(peer.id, term, reply, total_nodes);
    if (became_leader) leader_term = state_.current_term();
  }
  if (became_leader) {
    QC_LOG("won election for term %lld, sending initial heartbeats",
        static_cast<long long>(leader_term));
    BroadcastHeartbeats(leader_term);
    heartbeat_timer_.Reset();
  }
}

void RaftNode::BroadcastHeartbeats(int64_t term) {
  for (const auto& peer : config_.peers) {
    std::thread(&RaftNode::SendHeartbeatToPeer, this, peer, term).detach();
  }
}

void RaftNode::SendHeartbeatToPeer(PeerAddress peer, int64_t term) {
  AppendEntriesArgs request;
  request.set_term(term);
  request.set_leader_id(self_id_str_);
  AppendEntriesReply reply;

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + kRpcDeadline);
  grpc::Status status =
      stubs_[peer.id]->AppendEntries(&context, request, &reply);
  if (!status.ok()) {
    return;  // peer unreachable or timed out -- next tick will retry
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (reply.term() > state_.current_term()) {
    StepDownToFollowerLocked(reply.term());
  }
}

}  // namespace quorumchaos
