## PACELC: read latency and availability (all replicas healthy)

| Read Mode | Target Node | Median Latency (ms) | p95 Latency (ms) | Read Availability (%) |
|---|---|---|---|---|
| Linearizable | Leaseholder (Remote) | 4.31 | 6.59 | 100.0 |
| Follower Stale | Local Follower | 1.74 | 2.27 | 100.0 |

Environment: Windows 10, Python 3.10.9, 12 logical CPUs; 3 replica processes on 127.0.0.1, client attached to replica-C, leaseholder = replica-A, 1000 reads per scenario.
