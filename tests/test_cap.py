"""Task 3: CAP behaviour under a simulated network partition.

Three replicas run inside the test process. A partition is simulated by telling
every replica which peers it can no longer reach (set_blocked), i.e. the links
between the groups are cut while every client can still talk to every node.

    DC-1 = replicas A and B  (majority, 2 of 3)
    DC-2 = replica C         (minority, 1 of 3)

Run:  python -m pytest tests/test_cap.py -v
"""
import itertools
import os
import sys
import time

import grpc
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import kvstore_pb2  # noqa: E402
from client import KVClient  # noqa: E402
from server import make_server  # noqa: E402

CP = kvstore_pb2.QUORUM_CP
AP = kvstore_pb2.ALWAYS_AP
STALE = kvstore_pb2.FOLLOWER_STALE

DC1 = {0, 1}   # replicas A, B
DC2 = {2}      # replica C


class Cluster:
    """Three in-process replicas. Replica A (index 0) is the leaseholder."""

    NAMES = ["replica-A", "replica-B", "replica-C"]

    def __init__(self, heartbeat=0.2):
        self.servers, self.servicers, self.addrs = [], [], []
        for name in self.NAMES:
            server, port, servicer = make_server(0, name=name)
            self.servers.append(server)
            self.servicers.append(servicer)
            self.addrs.append(f"127.0.0.1:{port}")
        for i, servicer in enumerate(self.servicers):
            others = [a for j, a in enumerate(self.addrs) if j != i]
            servicer.set_peers(others)
            servicer.set_leaseholder(self.addrs[0])
        for server in self.servers:
            server.start()
        self.servicers[0].start_heartbeats(interval=heartbeat)
        self.clients = [KVClient(addr) for addr in self.addrs]

    def partition(self, *groups):
        """Cut every link between nodes that are in different groups."""
        group_of = {i: g for g, members in enumerate(groups) for i in members}
        for i, servicer in enumerate(self.servicers):
            servicer.set_blocked(
                self.addrs[j] for j in range(3) if group_of[j] != group_of[i])

    def heal(self):
        for servicer in self.servicers:
            servicer.set_blocked([])

    def local_value(self, index, key):
        """What replica `index` holds in its own memory (no limit on staleness)."""
        reply = self.clients[index].get(key, STALE, 0)
        return reply.value if reply.found else None

    def stop(self):
        for servicer in self.servicers:
            servicer.close()
        for client in self.clients:
            client.close()
        for server in self.servers:
            server.stop(grace=0).wait()


@pytest.fixture
def cluster():
    c = Cluster()
    yield c
    c.stop()


# --------------------------------------------------------------------------
# Part A: CP mode (strict quorum)
# --------------------------------------------------------------------------
def test_cp_put_succeeds_on_majority_side(cluster):
    cluster.partition(DC1, DC2)

    reply = cluster.clients[0].put("k", "v", strategy=CP)   # sent to DC-1

    assert reply.success is True
    assert reply.acks_received == 2


def test_cp_put_fails_on_minority_side(cluster):
    cluster.partition(DC1, DC2)

    reply = cluster.clients[2].put("k", "v", strategy=CP)   # sent to DC-2

    assert reply.success is False
    assert reply.acks_received == 1
    assert "quorum" in reply.error_message


# --------------------------------------------------------------------------
# Part B: AP mode (sloppy quorum + Last-Write-Wins)
# --------------------------------------------------------------------------
def test_ap_put_accepted_on_minority_side(cluster):
    cluster.partition(DC1, DC2)

    reply = cluster.clients[2].put("k", "v", strategy=AP)   # sent to DC-2

    assert reply.success is True
    assert reply.acks_received == 1


@pytest.mark.parametrize("ts_dc1, ts_dc2, winner", [
    (1000, 2000, "from-dc2"),
    (3000, 2000, "from-dc1"),
])
def test_lww_merges_divergent_values_after_heal(cluster, ts_dc1, ts_dc2, winner):
    cluster.partition(DC1, DC2)
    cluster.clients[0].put("k", "from-dc1", strategy=AP, timestamp=ts_dc1)
    cluster.clients[2].put("k", "from-dc2", strategy=AP, timestamp=ts_dc2)
    # the two sides now disagree
    assert cluster.local_value(0, "k") == "from-dc1"
    assert cluster.local_value(1, "k") == "from-dc1"
    assert cluster.local_value(2, "k") == "from-dc2"

    cluster.heal()
    reply = cluster.clients[0].get("k")     # LINEARIZABLE: majority read + merge

    assert reply.value == winner            # the larger timestamp wins
    assert [cluster.local_value(i, "k") for i in range(3)] == [winner] * 3


def test_lww_ignores_arrival_order(cluster):
    client = cluster.clients[0]

    client.put("k", "newer", strategy=AP, timestamp=2000)
    client.put("k", "older", strategy=AP, timestamp=1500)   # arrives later, is older

    reply = client.get("k")
    assert reply.value == "newer"
    assert reply.read_timestamp == 2000


# --------------------------------------------------------------------------
# Quorum arithmetic and read paths
# --------------------------------------------------------------------------
def test_read_quorum_intersects_write_quorum(cluster):
    """N=3, W=2, R=2 -> R + W > N, so every read quorum contains a replica that
    holds the newest write. With R=1 (R + W = 3, not > N) a read can miss it."""
    cluster.partition(DC1, DC2)
    reply = cluster.clients[0].put("k", "v1", strategy=CP, timestamp=1000)
    assert reply.success is True and reply.acks_received == 2    # W = {A, B}

    held = {i: cluster.local_value(i, "k") for i in range(3)}
    assert held == {0: "v1", 1: "v1", 2: None}
    for pair in itertools.combinations(range(3), 2):             # every R = 2 read
        assert any(held[i] == "v1" for i in pair)
    assert held[2] is None                                       # R = 1 can miss it


def test_follower_read_rejected_when_too_stale(cluster):
    time.sleep(0.5)                                      # let heartbeats arrive
    fresh = cluster.clients[2].get("k", STALE, 1000)
    assert fresh.is_stale is True                        # follower answer, within the bound

    cluster.partition(DC1, DC2)
    time.sleep(1.5)                                      # C no longer hears the leaseholder

    with pytest.raises(grpc.RpcError) as err:
        cluster.clients[2].get("k", STALE, 1000)
    assert err.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert "STALE_READ_EXCEEDED" in err.value.details()

    still_ok = cluster.clients[1].get("k", STALE, 1000)  # B still hears it
    assert still_ok.found is False


def test_linearizable_read_refused_when_leaseholder_isolated(cluster):
    cluster.partition({0}, {1, 2})                       # leaseholder A is alone
    reply = cluster.clients[1].put("k", "v", strategy=CP)    # B + C = majority
    assert reply.success is True and reply.acks_received == 2

    with pytest.raises(grpc.RpcError) as err:
        cluster.clients[0].get("k")                      # LINEARIZABLE at isolated A
    assert err.value.code() == grpc.StatusCode.UNAVAILABLE

    with pytest.raises(grpc.RpcError) as err2:
        cluster.clients[2].get("k")                      # C cannot reach the leaseholder
    assert err2.value.code() == grpc.StatusCode.UNAVAILABLE

    stale = cluster.clients[0].get("k", STALE, 0)        # available, but misses the write
    assert stale.found is False


def test_retry_after_failed_quorum_applies_once(cluster):
    cluster.partition({0}, {1, 2})
    first = cluster.clients[0].put("k", "v", idempotency_key="op-1", strategy=CP)
    assert first.success is False and first.acks_received == 1
    ts_first = cluster.clients[0].get("k", STALE, 0).read_timestamp

    cluster.heal()
    retry = cluster.clients[0].put("k", "v", idempotency_key="op-1", strategy=CP)

    assert retry.success is True
    assert retry.acks_received == 3
    stamps = {cluster.clients[i].get("k", STALE, 0).read_timestamp for i in range(3)}
    assert stamps == {ts_first}      # one operation, one timestamp, on every replica