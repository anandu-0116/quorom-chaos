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
any RPC reply, and the Pre-Vote extension (see below).

Not yet implemented: log entries, log replication, client-facing API,
snapshotting, membership changes.

## Pre-Vote

Before bumping its term and starting a real election, a node first
canvasses peers with `RequestPreVote` -- a side-effect-free "would you vote
for me?" that never touches `currentTerm`/`votedFor` and never resets
anyone's election timer. A peer grants it only if it hasn't heard a valid
heartbeat from a leader within the last election timeout. Only a Pre-Vote
majority triggers a real election.

This exists to stop a node that's merely *network-partitioned* (not
crashed) from disrupting a healthy cluster. Without it, an isolated node's
election timer keeps firing every ~150-300ms with nobody to hear from, and
each timeout unconditionally increments `currentTerm` -- so a node cut off
for a few minutes can come back holding a term far ahead of the real
leader's. The instant it reaches anyone, Raft's own safety rule ("adopt any
higher term you see") forces the healthy leader to step down, even though
it had a stable quorum the whole time. With Pre-Vote, the isolated node's
peers are all still hearing from the real leader, so they refuse every
canvass -- the isolated node's term never actually advances, and it rejoins
quietly the moment it's reachable again.

Reference: Ongaro's Raft dissertation, §4.2.3 and §9.6 (the same mechanism
etcd/raft and TiKV's raft-rs ship as `PreVote`).
