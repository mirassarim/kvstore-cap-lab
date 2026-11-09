"""One replica of the key-value store (steps 3-6).

Writes: the receiving replica is the coordinator (applies locally, copies to peers).
Reads:  LINEARIZABLE -> leaseholder does a majority read and repairs stale replicas.
        FOLLOWER_STALE -> any replica answers from its own state, if it is fresh enough.
"""
import argparse
import threading
import time
from concurrent import futures

import grpc

import kvstore_pb2
import kvstore_pb2_grpc


def now_ms():
    return int(time.time() * 1000)


class PeerUnreachable(Exception):
    """The link to a peer is cut (used later to simulate network partitions)."""


class KVStoreServicer(kvstore_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, name="replica-A", peers=(), leaseholder=None,
                 peer_timeout=1.0):
        self.name = name
        self.address = None                   # filled in by make_server
        self._leaseholder = leaseholder       # address of the leaseholder
        self._peer_timeout = peer_timeout
        self._lock = threading.Lock()
        self._data = {}                       # key -> (timestamp_ms, value)
        self._seen = {}                       # idempotency_key -> timestamp
        self._last_heartbeat_ms = now_ms()    # when we last heard the leaseholder
        self._blocked = set()                 # peers we cannot reach (partition)
        self._stop = threading.Event()
        self._peers = []
        self._stubs = {}
        self._pool = None
        self._hb_pool = None
        self.set_peers(peers)

    # ---------------- peers and roles ----------------
    def set_peers(self, peers):
        self._peers = list(peers)
        self._stubs = {
            addr: kvstore_pb2_grpc.KeyValueStoreStub(grpc.insecure_channel(addr))
            for addr in self._peers
        }
        workers = max(1, len(self._peers))
        self._pool = futures.ThreadPoolExecutor(max_workers=workers)
        self._hb_pool = futures.ThreadPoolExecutor(max_workers=workers)

    def set_leaseholder(self, address):
        self._leaseholder = address

    def set_blocked(self, addresses):
        self._blocked = set(addresses)

    def is_leaseholder(self):
        # a node with no leaseholder configured acts as its own leaseholder
        return self._leaseholder is None or self.address == self._leaseholder

    def majority(self):
        return (len(self._peers) + 1) // 2 + 1   # N // 2 + 1

    def _stub_for(self, addr):
        if addr in self._blocked or addr not in self._stubs:
            raise PeerUnreachable(addr)
        return self._stubs[addr]

    def close(self):
        self._stop.set()

    # ---------------- heartbeats ----------------
    def start_heartbeats(self, interval=0.5):
        """The leaseholder tells every follower 'I am alive' twice a second."""
        if not self.is_leaseholder():
            return

        def loop():
            while not self._stop.wait(interval):
                for addr in self._peers:
                    self._hb_pool.submit(self._send_heartbeat, addr)

        threading.Thread(target=loop, daemon=True).start()

    def _send_heartbeat(self, addr):
        try:
            request = kvstore_pb2.HeartbeatRequest(
                leaseholder=self.address or "", timestamp=now_ms())
            self._stub_for(addr).Heartbeat(request, timeout=self._peer_timeout)
        except (grpc.RpcError, PeerUnreachable):
            pass

    # ---------------- talking to peers ----------------
    def _send_to_peer(self, addr, request):
        """True if the peer acknowledged the copy."""
        try:
            reply = self._stub_for(addr).Replicate(request, timeout=self._peer_timeout)
            return reply.success
        except (grpc.RpcError, PeerUnreachable):
            return False

    def _replicate(self, request):
        results = self._pool.map(lambda addr: self._send_to_peer(addr, request),
                                 self._peers)
        return sum(1 for ok in results if ok)

    def _peer_read(self, addr, key):
        """Returns (answered, entry). entry is (timestamp, value) or None."""
        try:
            reply = self._stub_for(addr).PeerRead(
                kvstore_pb2.GetRequest(key=key), timeout=self._peer_timeout)
        except (grpc.RpcError, PeerUnreachable):
            return False, None
        if not reply.found:
            return True, None
        return True, (reply.read_timestamp, reply.value)

    # ---------------- local state ----------------
    def _apply_locked(self, key, value, timestamp):
        """Last-Write-Wins. The caller must already hold self._lock."""
        new = (timestamp, value)
        old = self._data.get(key)
        if old is None or new > old:
            self._data[key] = new
            return True
        return False

    def _apply_once(self, request, timestamp):
        with self._lock:
            key = request.idempotency_key
            if key and key in self._seen:
                return self._seen[key]
            self._apply_locked(request.key, request.value, timestamp)
            if key:
                self._seen[key] = timestamp
            return timestamp

    def _local_entry(self, key):
        with self._lock:
            return self._data.get(key)

    def _local_reply(self, key):
        entry = self._local_entry(key)
        if entry is None:
            return kvstore_pb2.GetReply(found=False)
        timestamp, value = entry
        return kvstore_pb2.GetReply(
            value=value, found=True, read_timestamp=timestamp, is_stale=False)

    # ---------------- write RPCs ----------------
    def Put(self, request, context):
        timestamp = self._apply_once(request, request.timestamp or now_ms())

        copy = kvstore_pb2.PutRequest(
            key=request.key,
            value=request.value,
            idempotency_key=request.idempotency_key,
            strategy=request.strategy,
            timestamp=timestamp,
        )
        acks = 1 + self._replicate(copy)

        if request.strategy == kvstore_pb2.ALWAYS_AP:
            return kvstore_pb2.PutReply(success=True, acks_received=acks)

        needed = self.majority()
        if acks >= needed:
            return kvstore_pb2.PutReply(success=True, acks_received=acks)
        return kvstore_pb2.PutReply(
            success=False,
            acks_received=acks,
            error_message=f"quorum lost: {acks} of {needed} required acks",
        )

    def Replicate(self, request, context):
        self._apply_once(request, request.timestamp or now_ms())
        return kvstore_pb2.PutReply(success=True, acks_received=1)

    # ---------------- read RPCs ----------------
    def Get(self, request, context):
        if request.consistency == kvstore_pb2.FOLLOWER_STALE:
            return self._follower_read(request, context)
        return self._linearizable_read(request, context)

    def PeerRead(self, request, context):
        return self._local_reply(request.key)

    def Heartbeat(self, request, context):
        self._last_heartbeat_ms = now_ms()
        return kvstore_pb2.HeartbeatReply(ok=True)

    def _follower_read(self, request, context):
        leader = self.is_leaseholder()
        age = 0 if leader else now_ms() - self._last_heartbeat_ms
        limit = request.max_staleness_ms          # 0 means "no limit"
        if limit and age > limit:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"STALE_READ_EXCEEDED: last heard from the leaseholder "
                f"{age} ms ago, limit is {limit} ms")
        reply = self._local_reply(request.key)
        reply.is_stale = not leader               # a follower may lag behind
        return reply

    def _linearizable_read(self, request, context):
        if not self.is_leaseholder():
            return self._proxy_to_leaseholder(request, context)

        key = request.key
        answers = [(None, self._local_entry(key))]   # (peer address or None=me, entry)
        results = self._pool.map(lambda addr: (addr, self._peer_read(addr, key)),
                                 self._peers)
        for addr, (answered, entry) in results:
            if answered:
                answers.append((addr, entry))

        needed = self.majority()
        if len(answers) < needed:
            context.abort(
                grpc.StatusCode.UNAVAILABLE,
                f"quorum read failed: {len(answers)} of {needed} required replicas answered")

        entries = [entry for _, entry in answers if entry is not None]
        if not entries:
            return kvstore_pb2.GetReply(found=False)
        winner = max(entries)                        # LWW: highest (timestamp, value)
        self._read_repair(key, winner, answers)
        timestamp, value = winner
        return kvstore_pb2.GetReply(
            value=value, found=True, read_timestamp=timestamp, is_stale=False)

    def _read_repair(self, key, winner, answers):
        """Push the winning value to every replica that answered with something older."""
        timestamp, value = winner
        for addr, entry in answers:
            if entry == winner:
                continue
            if addr is None:
                with self._lock:
                    self._apply_locked(key, value, timestamp)
            else:
                self._send_to_peer(addr, kvstore_pb2.PutRequest(
                    key=key, value=value, timestamp=timestamp))

    def _proxy_to_leaseholder(self, request, context):
        try:
            stub = self._stub_for(self._leaseholder)
            return stub.Get(request, timeout=self._peer_timeout * 1.5)
        except grpc.RpcError as err:
            if err.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                context.abort(grpc.StatusCode.UNAVAILABLE,
                              "leaseholder did not answer in time")
            context.abort(err.code(), f"leaseholder: {err.details()}")
        except PeerUnreachable:
            context.abort(grpc.StatusCode.UNAVAILABLE, "leaseholder unreachable")


def make_server(port=0, name="replica-A", peers=(), leaseholder=None,
                max_workers=8, peer_timeout=1.0):
    servicer = KVStoreServicer(name, peers, leaseholder, peer_timeout)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    kvstore_pb2_grpc.add_KeyValueStoreServicer_to_server(servicer, server)
    bound_port = server.add_insecure_port(f"127.0.0.1:{port}")
    servicer.address = f"127.0.0.1:{bound_port}"
    return server, bound_port, servicer


def main():
    parser = argparse.ArgumentParser(description="KV store replica")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--name", default="replica-A")
    parser.add_argument("--peers", default="",
                        help="comma-separated addresses of the OTHER replicas")
    parser.add_argument("--leaseholder", default="127.0.0.1:50051",
                        help="address of the leaseholder for keyspace [a-z]")
    args = parser.parse_args()
    peers = [p for p in args.peers.split(",") if p]

    server, port, servicer = make_server(
        args.port, args.name, peers, args.leaseholder)
    server.start()
    servicer.start_heartbeats()
    role = "LEASEHOLDER" if servicer.is_leaseholder() else "follower"
    print(f"{args.name} ({role}) listening on 127.0.0.1:{port}, peers={peers}",
          flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        servicer.close()
        server.stop(grace=0)


if __name__ == "__main__":
    main()