"""Task 4: PACELC benchmark - linearizable reads vs bounded-staleness follower reads.

    python perf_cap_bench.py               # 1000 reads per scenario
    python perf_cap_bench.py --outage      # + extra experiment: leaseholder is down

Method
  * Three replicas run as separate OS processes on localhost.
    replica-A is the leaseholder; the client is connected to replica-C (a follower).
  * Scenario 1: Get with LINEARIZABLE  -> C forwards to the leaseholder, which does a
                                          majority read (the "remote" path).
    Scenario 2: Get with FOLLOWER_STALE (max_staleness_ms = 5000) -> C answers locally.
  * Latency of one call = time.perf_counter() around client.get().
    Median = statistics.median; p95 = ceil(0.95 * n)-th smallest value (nearest rank).
  * Availability = successful reads / attempted reads.
Results are printed and saved to logs/perf_cap_results.md.
"""
import argparse
import math
import os
import platform
import socket
import statistics
import subprocess
import sys
import time

import grpc

import kvstore_pb2
from client import KVClient

ROOT = os.path.dirname(os.path.abspath(__file__))
KEY = "bench-key"
LIN = kvstore_pb2.LINEARIZABLE
STALE = kvstore_pb2.FOLLOWER_STALE
WARMUP_READS = 50


def p95(values):
    ordered = sorted(values)
    return ordered[math.ceil(0.95 * len(ordered)) - 1]


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_cluster():
    """Start 3 replica processes. Returns (processes, addresses)."""
    ports = [free_port() for _ in range(3)]
    addrs = [f"127.0.0.1:{p}" for p in ports]
    names = ["replica-A", "replica-B", "replica-C"]
    procs = []
    for i in range(3):
        peers = ",".join(a for j, a in enumerate(addrs) if j != i)
        cmd = [
            sys.executable, os.path.join(ROOT, "server.py"),
            "--port", str(ports[i]), "--name", names[i],
            "--peers", peers, "--leaseholder", addrs[0],
        ]
        procs.append(subprocess.Popen(
            cmd, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    for addr in addrs:
        channel = grpc.insecure_channel(addr)
        grpc.channel_ready_future(channel).result(timeout=20)
        channel.close()
    return procs, addrs


def stop_cluster(procs):
    for proc in procs:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=10)


def measure(client, mode, n, max_staleness_ms=0):
    """n sequential reads. Returns (latencies_ms, number_of_successes)."""
    latencies, ok = [], 0
    for _ in range(n):
        start = time.perf_counter()
        try:
            client.get(KEY, mode, max_staleness_ms)
            ok += 1
        except grpc.RpcError:
            pass
        latencies.append((time.perf_counter() - start) * 1000.0)
    return latencies, ok


def row(first, second, result, n):
    latencies, ok = result
    return (f"| {first} | {second} | {statistics.median(latencies):.2f} "
            f"| {p95(latencies):.2f} | {100.0 * ok / n:.1f} |")


def main():
    parser = argparse.ArgumentParser(description="PACELC read benchmark")
    parser.add_argument("--reads", type=int, default=1000,
                        help="reads per scenario")
    parser.add_argument("--outage", action="store_true",
                        help="extra experiment with the leaseholder stopped")
    parser.add_argument("--outage-reads", type=int, default=30,
                        help="reads per scenario in the outage experiment")
    args = parser.parse_args()
    n = args.reads

    procs, addrs = start_cluster()
    client = KVClient(addrs[2])          # the client sits next to follower replica-C
    try:
        seed = client.put(KEY, "value", strategy=kvstore_pb2.QUORUM_CP)
        if not seed.success or seed.acks_received != 3:
            raise SystemExit(f"seed write failed: {seed}")
        time.sleep(1.0)                  # let the first heartbeats arrive

        for _ in range(WARMUP_READS):    # warm-up, not recorded
            client.get(KEY, LIN)
            client.get(KEY, STALE, 5000)

        print(f"-> Linearizable reads ({n}) ...", flush=True)
        lin = measure(client, LIN, n)
        print(f"-> Follower-stale reads ({n}) ...", flush=True)
        stale = measure(client, STALE, n, 5000)

        lines = ["## PACELC: read latency and availability (all replicas healthy)\n",
                 "| Read Mode | Target Node | Median Latency (ms) "
                 "| p95 Latency (ms) | Read Availability (%) |",
                 "|---|---|---|---|---|",
                 row("Linearizable", "Leaseholder (Remote)", lin, n),
                 row("Follower Stale", "Local Follower", stale, n)]

        if args.outage:
            m = args.outage_reads
            print("-> Stopping the leaseholder (replica-A) ...", flush=True)
            procs[0].kill()
            procs[0].wait(timeout=10)
            killed_at = time.perf_counter()

            print(f"-> Follower-stale reads right after the crash ({m}) ...",
                  flush=True)
            out_stale_now = measure(client, STALE, m, 5000)
            print(f"-> Linearizable reads, leaseholder down ({m}) - may take "
                  "about a minute ...", flush=True)
            out_lin = measure(client, LIN, m)
            wait = 6.0 - (time.perf_counter() - killed_at)
            if wait > 0:
                time.sleep(wait)         # now the 5000 ms bound is exceeded
            print(f"-> Follower-stale reads 6 s after the crash ({m}) ...",
                  flush=True)
            out_stale_late = measure(client, STALE, m, 5000)

            lines += ["\n## Extra: leaseholder (replica-A) stopped\n",
                      "Latency here is the time until the answer OR the error.\n",
                      "| Situation | Read Mode | Median (ms) | p95 (ms) "
                      "| Read Availability (%) |",
                      "|---|---|---|---|---|",
                      row("Just after the crash (age < 5000 ms)",
                          "Follower Stale", out_stale_now, m),
                      row("Leaseholder down", "Linearizable", out_lin, m),
                      row("6 s after the crash (age > 5000 ms)",
                          "Follower Stale", out_stale_late, m)]

        lines.append(
            f"\nEnvironment: {platform.system()} {platform.release()}, "
            f"Python {platform.python_version()}, {os.cpu_count()} logical CPUs; "
            f"3 replica processes on 127.0.0.1, client attached to replica-C, "
            f"leaseholder = replica-A, {n} reads per scenario.")
        report = "\n".join(lines)
        print("\n" + report)

        out_dir = os.path.join(ROOT, "logs")
        os.makedirs(out_dir, exist_ok=True)
        with open(os.path.join(out_dir, "perf_cap_results.md"), "w",
                  encoding="utf-8") as f:
            f.write(report + "\n")
        print("\nSaved to logs/perf_cap_results.md")
    finally:
        client.close()
        stop_cluster(procs)


if __name__ == "__main__":
    main()