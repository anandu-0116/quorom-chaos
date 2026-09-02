#pragma once

#include <chrono>
#include <cstdio>
#include <ctime>
#include <mutex>
#include <string>

namespace quorumchaos {

// Minimal thread-safe stderr logger. Every line is timestamped and tagged
// with the node id so `tail -f logs/*.log` from run_cluster.sh is readable
// when several nodes are logging concurrently.
class Logger {
 public:
  static void Init(int node_id) { NodeId() = node_id; }

  template <typename... Args>
  static void Log(const char* fmt, Args... args) {
    std::lock_guard<std::mutex> lock(Mutex());
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  now.time_since_epoch()) %
              1000;
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm_buf;
    localtime_r(&t, &tm_buf);
    char time_buf[16];
    std::strftime(time_buf, sizeof(time_buf), "%H:%M:%S", &tm_buf);
    std::fprintf(stderr, "%s.%03lld [node%d] ", time_buf,
                 static_cast<long long>(ms.count()), NodeId());
    std::fprintf(stderr, fmt, args...);
    std::fprintf(stderr, "\n");
    std::fflush(stderr);
  }

 private:
  static int& NodeId() {
    static int id = -1;
    return id;
  }
  static std::mutex& Mutex() {
    static std::mutex m;
    return m;
  }
};

#define QC_LOG(...) ::quorumchaos::Logger::Log(__VA_ARGS__)

}  // namespace quorumchaos
