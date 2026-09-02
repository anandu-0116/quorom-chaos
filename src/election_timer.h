#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <random>
#include <thread>

namespace quorumchaos {

// A resettable, randomized timer used both as the follower/candidate
// election timer ([150ms, 300ms)) and as the leader's heartbeat ticker
// (fixed interval, via min==max). One background thread lives for the
// lifetime of the timer; Reset()/Stop() just bump a generation counter and
// notify the condition variable rather than tearing the thread down, so
// frequent resets (every heartbeat/vote) stay cheap and race-free.
class ElectionTimer {
 public:
  using Callback = std::function<void()>;

  // `min_interval`/`max_interval` bound the random deadline drawn on each
  // Reset(). Pass equal values for a fixed-interval ticker (heartbeats).
  // `seed` should differ per node so timers don't stay in lockstep.
  ElectionTimer(std::chrono::milliseconds min_interval,
                std::chrono::milliseconds max_interval, unsigned seed,
                Callback on_timeout);
  ~ElectionTimer();

  ElectionTimer(const ElectionTimer&) = delete;
  ElectionTimer& operator=(const ElectionTimer&) = delete;

  // Starts the background thread and arms the first deadline. Safe to call
  // once; call Stop() before a second Start() if the timer needs restarting.
  void Start();

  // Draws a new random deadline and arms it, replacing whatever deadline
  // was pending. Call this on every valid RPC from the current leader / on
  // granting a vote / on (re-)entering Candidate.
  void Reset();

  // Permanently stops the background thread. Idempotent; safe to call from
  // the destructor.
  void Stop();

 private:
  void Run();
  std::chrono::milliseconds RandomInterval();

  const std::chrono::milliseconds min_interval_;
  const std::chrono::milliseconds max_interval_;
  std::mt19937 rng_;

  Callback on_timeout_;

  std::mutex mutex_;
  std::condition_variable cv_;
  std::chrono::steady_clock::time_point deadline_;
  uint64_t generation_ = 0;
  bool stopped_ = false;
  bool started_ = false;
  std::thread thread_;
};

}  // namespace quorumchaos
