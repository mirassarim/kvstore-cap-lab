# Consistency Models and the CAP Theorem

Course: CSS4004 - Distributed Systems
Lab: Quorum tuning, CP vs AP behaviour, PACELC trade-offs
Author: Miras Zhanbyr, group 4

A replicated key-value store (Python + gRPC) with three replicas. It shows how the
system behaves under a network partition (CP: strict quorum, AP: sloppy quorum with
Last-Write-Wins) and what linearizable reads cost compared with bounded-staleness
follower reads.

## 1. Setup (Windows, cmd)

```cmd
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

The generated gRPC files (`kvstore_pb2.py`, `kvstore_pb2_grpc.py`) are committed. To
regenerate them after editing `kvstore.proto`:

```cmd
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. kvstore.proto
```

## 2. Run three replicas

One terminal per replica (activate the virtual environment in each one).
Replica A (port 50051) is the leaseholder.

```cmd
python server.py --port 50051 --name replica-A --peers 127.0.0.1:50052,127.0.0.1:50053 --leaseholder 127.0.0.1:50051
python server.py --port 50052 --name replica-B --peers 127.0.0.1:50051,127.0.0.1:50053 --leaseholder 127.0.0.1:50051
python server.py --port 50053 --name replica-C --peers 127.0.0.1:50051,127.0.0.1:50052 --leaseholder 127.0.0.1:50051
```

Stop every replica with Ctrl+C after use; restart all of them after editing `server.py`.

## 3. Use the client

```cmd
python client.py --addr 127.0.0.1:50052 put city Almaty
python client.py --addr 127.0.0.1:50052 put color blue --strategy ap
python client.py --addr 127.0.0.1:50053 get city --mode lin
python client.py --addr 127.0.0.1:50053 get city --mode stale --max-staleness-ms 5000
```

* `put`: `--strategy cp` (default, needs a majority) or `ap` (always accepted);
  `--ts N` sets an explicit timestamp in ms; `--idem KEY` sets the idempotency key.
* `get`: `--mode lin` (linearizable, default) or `stale` (follower read);
  `--max-staleness-ms 0` means "no limit".

## 4. Tests

```cmd
python -m pytest tests/test_cap.py -v
```

10 tests: CP on the majority/minority side, AP on the minority side, LWW merge after
the partition heals, quorum arithmetic (R + W > N), `STALE_READ_EXCEEDED`, an isolated
leaseholder, and a safe retry after a failed quorum. Replicas run inside the test
process on ephemeral ports; no manual steps are needed.

## 5. Benchmark

```cmd
python perf_cap_bench.py            # 1000 reads per scenario
python perf_cap_bench.py --outage   # + experiment with the leaseholder stopped
```

Results are saved to `logs/perf_cap_results.md`.

## 6. Design notes

* **Writes.** The replica that receives a `Put` is the coordinator: it applies the write
  locally, sends it to the peers (`Replicate`) and counts the ACKs.
  `QUORUM_CP` succeeds only with a majority (N//2 + 1 = 2 of 3), otherwise it reports
  `quorum lost`. `ALWAYS_AP` always succeeds; `acks_received` tells how far it travelled.
* **Last-Write-Wins.** Every value is stored with a timestamp; the larger
  `(timestamp, value)` pair wins, so the result does not depend on arrival order.
* **Idempotency.** A retry with the same `idempotency_key` is not applied twice and keeps
  the original timestamp.
* **Linearizable reads.** A follower forwards the request to the leaseholder. The
  leaseholder reads from a majority (R = 2, W = 2, so R + W > N), picks the newest value and
  repairs replicas that answered with something older (read repair). If fewer than a
  majority answer, the read fails with `UNAVAILABLE`.
* **Follower reads.** A follower answers from its own state. The leaseholder sends a
  heartbeat every 0.5 s; if the follower has not heard it for longer than
  `max_staleness_ms`, it returns `STALE_READ_EXCEEDED`.
* **Extra internal RPCs** (beyond `Get` and `Put` from the assignment): `Replicate`,
  `PeerRead`, `Heartbeat`.

## 7. Limitations

* State is in memory; a restarted replica starts empty.
* A `QUORUM_CP` write that fails to reach a majority is still applied on the coordinator
  (no rollback / two-phase commit).
* A heartbeat shows that the leaseholder was reachable, not that the follower has caught up
  on all writes; read repair fixes lagging replicas only for the keys that are read.
* The partition is simulated inside the replicas' peer-RPC layer (`set_blocked`), not with
  a TCP proxy or by killing processes.

## Repository layout

```
kvstore.proto                      service definition
kvstore_pb2.py, kvstore_pb2_grpc.py generated stubs
server.py                          replica (writes, reads, heartbeats, read repair)
client.py                          command-line client and KVClient class
perf_cap_bench.py                  Task 4 benchmark
tests/test_cap.py                  Task 3 partition tests
logs/perf_cap_results.md           recorded benchmark results
Lab_Report_CAP.pdf                 lab report
requirements.txt                   dependencies
```