#include <cstdlib>
#include <iostream>

#include "config.h"
#include "raft_node.h"

int main(int argc, char** argv) {
  try {
    quorumchaos::NodeConfig config = quorumchaos::ParseNodeConfig(argc, argv);
    quorumchaos::RaftNode node(std::move(config));
    node.Run();
  } catch (const std::exception& e) {
    std::cerr << "fatal: " << e.what() << "\n\n";
    std::cerr << "usage: raft_node --id=<int> --config=<path> [--data_dir=<dir>]\n"
                 "                  [--peer=<id>:<host:port> ...]\n";
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
