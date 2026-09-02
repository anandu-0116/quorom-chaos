#include "config.h"

#include <fstream>
#include <map>
#include <sstream>
#include <stdexcept>

namespace quorumchaos {

namespace {

// Splits "--flag=value" into {"flag", "value"}. Returns false if `arg`
// isn't a "--flag=value" style argument.
bool SplitFlag(const std::string& arg, std::string* flag, std::string* value) {
  if (arg.rfind("--", 0) != 0) return false;
  auto eq = arg.find('=');
  if (eq == std::string::npos) return false;
  *flag = arg.substr(2, eq - 2);
  *value = arg.substr(eq + 1);
  return true;
}

// Parses one "id,host:port" line from a config file.
void ParseConfigLine(const std::string& line, std::map<int, std::string>* out) {
  if (line.empty() || line[0] == '#') return;
  auto comma = line.find(',');
  if (comma == std::string::npos) {
    throw std::runtime_error("malformed config line (expected id,host:port): " +
                              line);
  }
  int id = std::stoi(line.substr(0, comma));
  std::string address = line.substr(comma + 1);
  (*out)[id] = address;
}

// Parses one "--peer=<id>:<host:port>" value.
void ParsePeerFlag(const std::string& value, std::map<int, std::string>* out) {
  auto colon = value.find(':');
  if (colon == std::string::npos) {
    throw std::runtime_error("malformed --peer value (expected id:host:port): " +
                              value);
  }
  int id = std::stoi(value.substr(0, colon));
  std::string address = value.substr(colon + 1);
  (*out)[id] = address;
}

}  // namespace

NodeConfig ParseNodeConfig(int argc, char** argv) {
  NodeConfig config;
  std::map<int, std::string> addresses_by_id;  // includes self, split out later

  for (int i = 1; i < argc; ++i) {
    std::string flag, value;
    if (!SplitFlag(argv[i], &flag, &value)) {
      throw std::runtime_error(std::string("unrecognized argument: ") +
                                argv[i]);
    }

    if (flag == "id") {
      config.self_id = std::stoi(value);
    } else if (flag == "data_dir") {
      config.data_dir = value;
    } else if (flag == "config") {
      std::ifstream file(value);
      if (!file.is_open()) {
        throw std::runtime_error("cannot open config file: " + value);
      }
      std::string line;
      while (std::getline(file, line)) {
        // Trim trailing '\r' in case the file has CRLF line endings.
        if (!line.empty() && line.back() == '\r') line.pop_back();
        ParseConfigLine(line, &addresses_by_id);
      }
    } else if (flag == "peer") {
      ParsePeerFlag(value, &addresses_by_id);
    } else {
      throw std::runtime_error("unrecognized flag: --" + flag);
    }
  }

  if (config.self_id < 0) {
    throw std::runtime_error("--id=<int> is required");
  }
  auto self_it = addresses_by_id.find(config.self_id);
  if (self_it == addresses_by_id.end()) {
    throw std::runtime_error(
        "no address found for --id=" + std::to_string(config.self_id) +
        " (add it via --config or --peer)");
  }
  config.self_address = self_it->second;

  for (const auto& [id, address] : addresses_by_id) {
    if (id != config.self_id) {
      config.peers.push_back(PeerAddress{id, address});
    }
  }
  if (config.peers.empty()) {
    throw std::runtime_error(
        "no peers configured -- a single-node cluster can't hold an "
        "election");
  }

  return config;
}

}  // namespace quorumchaos
