#include "election_timer.h"

namespace quorumchaos {

ElectionTimer::ElectionTimer(std::chrono::milliseconds min_interval,
                              std::chrono::milliseconds max_interval,
                              unsigned seed, Callback on_timeout)
    : min_interval_(min_interval),
      max_interval_(max_interval),
      rng_(seed),
      on_timeout_(std::move(on_timeout)) {}

ElectionTimer::~ElectionTimer() { Stop(); }

std::chrono::milliseconds ElectionTimer::RandomInterval() {
  if (min_interval_ >= max_interval_) return min_interval_;
  std::uniform_int_distribution<int64_t> dist(min_interval_.count(),
                                               max_interval_.count() - 1);
  return std::chrono::milliseconds(dist(rng_));
}

void ElectionTimer::Start() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (started_) return;
  started_ = true;
  deadline_ = std::chrono::steady_clock::now() + RandomInterval();
  thread_ = std::thread(&ElectionTimer::Run, this);
}

void ElectionTimer::Reset() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (stopped_) return;
  deadline_ = std::chrono::steady_clock::now() + RandomInterval();
  ++generation_;
  cv_.notify_one();
}

void ElectionTimer::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopped_ || !started_) {
      stopped_ = true;
      return;
    }
    stopped_ = true;
    ++generation_;
    cv_.notify_one();
  }
  if (thread_.joinable()) thread_.join();
}

void ElectionTimer::Run() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!stopped_) {
    uint64_t generation_at_wait_start = generation_;
    auto deadline = deadline_;
    // wait_until returns either because the deadline passed or because
    // Reset()/Stop() bumped the generation and notified us; either way we
    // re-check below rather than trusting the return value directly.
    cv_.wait_until(lock, deadline);
    if (stopped_) break;
    if (generation_ != generation_at_wait_start) {
      // Reset() happened during the wait -- loop and wait on the new
      // deadline instead of firing.
      continue;
    }
    if (std::chrono::steady_clock::now() < deadline_) {
      // Spurious wakeup with no actual reset; keep waiting on the same
      // deadline.
      continue;
    }

    // Timeout actually fired. Draw the next deadline now (so, e.g., a
    // candidate whose own election times out immediately gets a fresh
    // window) and invoke the callback without holding mutex_, since the
    // callback (state-transition logic) may call back into Reset()/Stop().
    deadline_ = std::chrono::steady_clock::now() + RandomInterval();
    ++generation_;
    lock.unlock();
    on_timeout_();
    lock.lock();
  }
}

}  // namespace quorumchaos
