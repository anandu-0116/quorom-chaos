#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace quorumchaos {

// The Raft-required persistent state (currentTerm, votedFor). Nothing else
// is persisted yet -- there is no log.
//
// Every setter that changes term/votedFor writes through to disk
// synchronously (fsync'd, atomic rename) before returning. Callers must
// call Save() (directly, or via SetTermAndVotedFor) before replying to any
// RPC that depends on the new value -- that's what makes "persist before
// responding" hold even though we don't persist a log yet.
class PersistentState {
 public:
  // Loads state from `path` if it exists; otherwise initializes to
  // (term=0, votedFor=nullopt) and writes that out as the starting point.
  explicit PersistentState(std::string path);

  int64_t current_term() const { return current_term_; }
  const std::optional<std::string>& voted_for() const { return voted_for_; }

  // Persists both fields atomically. Blocks until the write (and fsync)
  // completes.
  void SetTermAndVotedFor(int64_t term, std::optional<std::string> voted_for);

 private:
  void Load();
  void Save() const;

  std::string path_;
  int64_t current_term_ = 0;
  std::optional<std::string> voted_for_;
};

}  // namespace quorumchaos
