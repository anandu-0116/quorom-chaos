#include "persistent_state.h"

#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <fstream>
#include <sstream>
#include <stdexcept>

#include "logger.h"

namespace quorumchaos {

PersistentState::PersistentState(std::string path) : path_(std::move(path)) {
  Load();
}

void PersistentState::Load() {
  std::ifstream in(path_);
  if (!in.is_open()) {
    // First run for this node: start cold and persist the initial state so
    // the file exists from here on.
    current_term_ = 0;
    voted_for_ = std::nullopt;
    Save();
    return;
  }

  std::string term_line, voted_for_line;
  if (!std::getline(in, term_line)) {
    throw std::runtime_error("persistent state file is corrupt: " + path_);
  }
  std::getline(in, voted_for_line);  // may legitimately be absent/empty

  current_term_ = std::stoll(term_line);
  voted_for_ = voted_for_line.empty() ? std::nullopt
                                       : std::make_optional(voted_for_line);
  QC_LOG("loaded persistent state: term=%lld votedFor=%s",
      static_cast<long long>(current_term_),
      voted_for_.value_or("<none>").c_str());
}

void PersistentState::Save() const {
  const std::string tmp_path = path_ + ".tmp";

  {
    std::ofstream out(tmp_path, std::ios::trunc);
    if (!out.is_open()) {
      throw std::runtime_error("failed to open " + tmp_path +
                                " for writing persistent state");
    }
    out << current_term_ << "\n" << voted_for_.value_or("") << "\n";
    out.flush();
    if (!out.good()) {
      throw std::runtime_error("failed to write persistent state to " +
                                tmp_path);
    }
  }

  // fsync the temp file's contents to disk before the rename, so a crash
  // between fsync and rename can only leave the *old* file intact -- never
  // a half-written one.
  int fd = ::open(tmp_path.c_str(), O_RDONLY);
  if (fd >= 0) {
    ::fsync(fd);
    ::close(fd);
  }

  if (std::rename(tmp_path.c_str(), path_.c_str()) != 0) {
    throw std::runtime_error("failed to atomically replace " + path_);
  }
}

void PersistentState::SetTermAndVotedFor(
    int64_t term, std::optional<std::string> voted_for) {
  current_term_ = term;
  voted_for_ = std::move(voted_for);
  Save();
}

}  // namespace quorumchaos
