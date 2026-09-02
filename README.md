# quorum-chaos

A C++ Raft implementation using gRPC/Protobuf. Currently implements **leader
election only** -- no log replication, no client-facing API.

## Build

Requires gRPC and Protobuf (on macOS: `brew install grpc`, which pulls in
protobuf).

```sh
cmake -B build
cmake --build build
```

## Run a local cluster

```sh
scripts/run_cluster.sh 5       # starts 5 node processes on 127.0.0.1:50050-50054
tail -f logs/*.log             # watch the election
kill <pid>                     # kill the current leader, watch a re-election
scripts/stop_cluster.sh        # tear the cluster down
```

## Run a single node manually

```sh
build/raft_node --id=0 --config=cluster.conf.example --data_dir=data/node0
```

- `--id=<int>` (required): which line of the config is this node.
- `--config=<path>`: cluster config file, one `id,host:port` line per node.
- `--peer=<id>:<host:port>` (repeatable): add/override a single peer without
  a config file.
- `--data_dir=<path>` (default `.`): where `node<id>.state` (persisted
  `currentTerm`/`votedFor`) is written.

## Scope

Implemented: `RequestVote`/`AppendEntries` (heartbeat-only) RPCs,
Follower/Candidate/Leader transitions, randomized election timeout in
`[150ms, 300ms)`, persistent `currentTerm`/`votedFor` written to disk before
any RPC reply.

Not yet implemented: log entries, log replication, client-facing API,
snapshotting, membership changes.
