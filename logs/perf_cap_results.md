## PACELC: read latency and availability (all replicas healthy)

| Read Mode | Target Node | Median Latency (ms) | p95 Latency (ms) | Read Availability (%) |
|---|---|---|---|---|
| Linearizable | Leaseholder (Remote) | 1.13 | 1.18 | 100.0 |
| Follower Stale | Local Follower | 0.31 | 0.34 | 100.0 |

## Extra: leaseholder (replica-A) stopped

Latency here is the time until the answer OR the error.

| Situation | Read Mode | Median (ms) | p95 (ms) | Read Availability (%) |
|---|---|---|---|---|
| Just after the crash (age < 5000 ms) | Follower Stale | 0.36 | 0.65 | 100.0 |
| Leaseholder down | Linearizable | 0.86 | 1503.42 | 0.0 |
| 6 s after the crash (age > 5000 ms) | Follower Stale | 0.56 | 2.68 | 0.0 |

Environment: Windows 10, Python 3.10.9, 12 logical CPUs; 3 replica processes on 127.0.0.1, client attached to replica-C, leaseholder = replica-A, 1000 reads per scenario.
