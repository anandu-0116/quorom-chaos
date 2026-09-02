#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace quorumchaos {

struct PeerAddress {
  int id;
  std::string address;  // "host:port"
};

struct NodeConfig {
  int self_id = -1;
  std::string self_address;
  std::vector<PeerAddress> peers;  // everyone except self_id
  std::string data_dir = ".";
};

// Parses command-line args of the form:
//   --id=<int>              (required)
//   --config=<path>          config file, one "id,host:port" line per node
//   --peer=<id>:<host:port>  repeatable; adds/overrides a single peer
//                            without needing a config file
//   --data_dir=<path>        where node<id>.state is written (default ".")
//
// At least one of --config or --peer must supply the node's own id's
// address. Throws std::runtime_error with a human-readable message on any
// parse/validation failure.
NodeConfig ParseNodeConfig(int argc, char** argv);

}  // namespace quorumchaos
