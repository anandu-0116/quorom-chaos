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
      // Grace period: a just-started node hasn't heard from anyone yet, but
      // acts as if it just saw a leader so it doesn't immediately grant
      // pre-votes to whatever happens to time out first.
      last_leader_contact_(std::chrono::steady_clock::now()),
      election_timer_(kElectionTimeoutMin, kElectionTimeoutMax,
                       std::random_device{}(),
                       [this] { OnElectionTimeout(); }),
      heartbeat_timer_(kHeartbeatInterval, kHeartbeatInterval,
                        std::random_device{}(),
                        [this] { OnHeartbeatTick(); }) {
  // gRPC's default reconnect backoff runs from 1s up to a 120s ceiling on a
  // clock of its own, independent of how often we call RPCs against the
  // channel -- a call against a non-READY channel just fails fast, it
  // doesn't force a new connection attempt. Left at the defaults, a peer
  // that's been down for a while can stay unreachable for up to two
  // minutes after it comes back even though we heartbeat it every 50ms,
  // which reads to the rest of the cluster as a missing leader and
  // triggers an avoidable election. Capping the backoff keeps reconnect
  // attempts on roughly the heartbeat cadence instead.
  grpc::ChannelArguments channel_args;
  channel_args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 20);
  channel_args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 20);
  channel_args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 100);

  for (const auto& peer : config_.peers) {
    stubs_[peer.id] = Raft::NewStub(grpc::CreateCustomChannel(
        peer.address, grpc::InsecureChannelCredentials(), channel_args));
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
  // Figure 2 (Candidates): "On conversion to candidate, start election: ...
  // reset election timer". BecomeCandidateLocked() is called asynchronously
  // now (after a pre-vote round-trip), not synchronously inside the timer's
  // own callback like before pre-vote existed -- so it can no longer rely
  // on the timer's fire-time self-renewal to already be fresh. Without this
  // explicit reset, a slow pre-vote round leaves the new candidate holding
  // an already-stale deadline that can fire again almost immediately,
  // touching off a competing round -- exactly the runaway term growth seen
  // under load.
  election_timer_.Reset();
}

void RaftNode::StepDownToFollowerLocked(int64_t new_term) {
  if (new_term > state_.current_term()) {
    state_.SetTermAndVotedFor(new_term, std::nullopt);
  }
  SetRoleLocked(Role::Follower);
  // Adopting a higher term -- from an inbound RPC, or (just as often here)
  // from a *reply* to our own outgoing RequestVote/RequestPreVote/
  // AppendEntries -- is solid evidence someone else is actively contesting
  // leadership right now. Without a fresh window here, a deposed node's
  // election timer can already be near-expired (it was armed back when it
  // *started* its own candidacy, possibly a full round-trip ago) and fires
  // again almost instantly, competing again before the other side has had
  // any chance to stabilize -- two nodes can then leapfrog each other's
  // leadership indefinitely, which is exactly the runaway churn seen after
  // a single real leader failure.
  election_timer_.Reset();
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

  last_leader_contact_ = std::chrono::steady_clock::now();
  election_timer_.Reset();  // valid RPC: heartbeat from the current leader
  reply->set_term(state_.current_term());
  reply->set_success(true);
  return grpc::Status::OK;
}

grpc::Status RaftNode::RequestPreVote(grpc::ServerContext* /*context*/,
                                       const RequestVoteArgs* request,
                                       RequestVoteReply* reply) {
  std::lock_guard<std::mutex> lock(mutex_);

  // A pre-vote never mutates currentTerm/votedFor and never resets
  // election_timer_ -- it's purely a poll of "would you vote for me".
  if (request->term() < state_.current_term()) {
    reply->set_term(state_.current_term());
    reply->set_vote_granted(false);
    return grpc::Status::OK;
  }

  // A Leader never calls AppendEntries on itself, so last_leader_contact_
  // alone would stay stale the instant a node wins an election -- it's only
  // stamped by *accepting someone else's* heartbeat as a Follower. Without
  // this explicit role_ == Leader check, a fresh leader would immediately
  // believe no leader exists and grant pre-votes to any challenger,
  // deposing itself within one election-timeout window, forever -- which is
  // exactly the spontaneous, no-fault-required oscillation observed.
  const bool leader_believed_alive =
      role_ == Role::Leader ||
      std::chrono::steady_clock::now() - last_leader_contact_ <
          kElectionTimeoutMin;
  // state_.voted_for() only reflects who we voted for at state_.current_term()
  // -- meaningless for a strictly higher prospective term, since a real
  // RequestVote at that term would call StepDownToFollowerLocked() and wipe
  // votedFor *before* ever checking it (see the real handler below). Without
  // this term check, a node that has ever voted for anyone becomes stuck
  // refusing every future pre-vote, for every term, forever, the moment a
  // higher term is proposed -- a permanent liveness lockup the instant the
  // node everyone last voted for goes away.
  const bool would_grant_real_vote =
      request->term() > state_.current_term() ||
      !state_.voted_for().has_value() ||
      state_.voted_for().value() == request->candidate_id();
  const bool grant = !leader_believed_alive && would_grant_real_vote;

  if (grant) {
    QC_LOG("granted pre-vote to node%s for prospective term %lld",
        request->candidate_id().c_str(),
        static_cast<long long>(request->term()));
  }
  reply->set_term(state_.current_term());
  reply->set_vote_granted(grant);
  return grpc::Status::OK;
}

void RaftNode::OnElectionTimeout() {
  int64_t prospective_term;
  int total_nodes;
  uint64_t round;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (role_ == Role::Leader) return;  // heartbeats keep leaders from firing
    prospective_term = state_.current_term() + 1;
    total_nodes = static_cast<int>(config_.peers.size()) + 1;
    round = ++pre_vote_round_;
    pre_votes_received_ = 1;  // count self
  }
  QC_LOG("election timeout -> canvassing pre-votes for prospective term %lld",
      static_cast<long long>(prospective_term));
  BroadcastRequestPreVote(prospective_term, round, total_nodes);
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

void RaftNode::BroadcastRequestPreVote(int64_t prospective_term,
                                        uint64_t round, int total_nodes) {
  for (const auto& peer : config_.peers) {
    std::thread(&RaftNode::SendRequestPreVoteToPeer, this, peer,
                prospective_term, round, total_nodes)
        .detach();
  }
}

void RaftNode::SendRequestPreVoteToPeer(PeerAddress peer,
                                         int64_t prospective_term,
                                         uint64_t round, int total_nodes) {
  RequestVoteArgs request;
  request.set_term(prospective_term);
  request.set_candidate_id(self_id_str_);
  RequestVoteReply reply;

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + kRpcDeadline);
  grpc::Status status =
      stubs_[peer.id]->RequestPreVote(&context, request, &reply);
  if (!status.ok()) {
    return;  // peer unreachable or timed out this round -- just skip it
  }

  int64_t real_term = 0;
  int total = 0;
  bool won_pre_vote = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (reply.term() > state_.current_term()) {
      StepDownToFollowerLocked(reply.term());
      return;
    }
    // Discard stale replies: a newer canvass has since started, we're no
    // longer eligible to start one (already Candidate/Leader), or this
    // peer said no.
    if (round != pre_vote_round_ || role_ != Role::Follower ||
        !reply.vote_granted()) {
      return;
    }
    if (++pre_votes_received_ * 2 <= total_nodes) {
      return;  // not yet a majority
    }
    // Just crossed majority. Advance the round first so any other in-flight
    // replies for it see round != pre_vote_round_ and no-op, then do the
    // real term bump + persist + self-vote that BecomeCandidateLocked
    // already implements.
    ++pre_vote_round_;
    BecomeCandidateLocked();
    real_term = state_.current_term();
    total = total_nodes;
    won_pre_vote = true;
  }
  if (won_pre_vote) {
    QC_LOG("won pre-vote canvass, starting real election for term %lld",
        static_cast<long long>(real_term));
    BroadcastRequestVote(real_term, total);
  }
}

}  // namespace quorumchaos
