#pragma once

#include <grpcpp/grpcpp.h>

#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "config.h"
#include "election_timer.h"
#include "persistent_state.h"
#include "raft.grpc.pb.h"

namespace quorumchaos {

// One Raft node: the gRPC service implementation plus the election state
// machine. Leader-election only -- no log, no client API.
class RaftNode final : public Raft::Service {
 public:
  explicit RaftNode(NodeConfig config);

  // Starts the timers and the gRPC server, then blocks (server->Wait())
  // for the lifetime of the process. There is no clean-shutdown RPC at
  // this stage; stop a node with SIGTERM/SIGKILL.
  void Run();

  grpc::Status RequestVote(grpc::ServerContext* context,
                            const RequestVoteArgs* request,
                            RequestVoteReply* reply) override;
  grpc::Status AppendEntries(grpc::ServerContext* context,
                              const AppendEntriesArgs* request,
                              AppendEntriesReply* reply) override;

 private:
  enum class Role { Follower, Candidate, Leader };
  static const char* RoleName(Role role);

  // Timer callbacks. Invoked by ElectionTimer's background thread with no
  // lock held; each takes mutex_ itself for the part that touches state.
  void OnElectionTimeout();
  void OnHeartbeatTick();

  // Outgoing RPCs. Each runs on its own short-lived std::thread so a
  // slow/unreachable peer can't stall the caller; mutex_ is held only for
  // the brief bit of state each one reads/updates, never across the network
  // call itself.
  void BroadcastRequestVote(int64_t term, int total_nodes);
  void SendRequestVoteToPeer(PeerAddress peer, int64_t term, int total_nodes);
  void BroadcastHeartbeats(int64_t term);
  void SendHeartbeatToPeer(PeerAddress peer, int64_t term);

  // State-machine transitions. Caller must already hold mutex_.
  void SetRoleLocked(Role role);
  void BecomeCandidateLocked();
  void StepDownToFollowerLocked(int64_t new_term);
  // Returns true iff this reply just won the election (majority reached).
  bool HandleRequestVoteReplyLocked(int peer_id, int64_t request_term,
                                     const RequestVoteReply& reply,
                                     int total_nodes);

  const NodeConfig config_;
  const std::string self_id_str_;

  std::mutex mutex_;
  PersistentState state_;       // currentTerm, votedFor -- guarded by mutex_
  Role role_ = Role::Follower;  // guarded by mutex_
  int votes_received_ = 0;      // guarded by mutex_; meaningful only while Candidate

  ElectionTimer election_timer_;  // [150ms, 300ms); drives Follower/Candidate timeout
  ElectionTimer heartbeat_timer_; // fixed 50ms; drives Leader's heartbeats

  // Peer id -> stub. Built once at construction, read-only afterwards, so
  // concurrent lookups from multiple RPC threads are safe without a lock.
  std::map<int, std::unique_ptr<Raft::Stub>> stubs_;
};

}  // namespace quorumchaos
