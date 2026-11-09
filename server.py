"""One replica of the key-value store.

Step 4: the replica that receives a Put is the coordinator. It applies the
write locally, copies it to its peers (Replicate RPC) and counts the ACKs.
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


class KVStoreServicer(kvstore_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, name="replica-A", peers=(), peer_timeout=1.0):
        self.name = name
        self._peer_timeout = peer_timeout
        self._lock = threading.Lock()
        self._data = {}   # key -> (timestamp_ms, value)
        self._seen = {}   # idempotency_key -> timestamp used for that write
        self._peers = []
        self._stubs = {}
        self._pool = None
        self.set_peers(peers)

    # ---------------- peers ----------------
    def set_peers(self, peers):
        self._peers = list(peers)
        self._stubs = {
            addr: kvstore_pb2_grpc.KeyValueStoreStub(grpc.insecure_channel(addr))
            for addr in self._peers
        }
        self._pool = futures.ThreadPoolExecutor(max_workers=max(1, len(self._peers)))

    def majority(self):
        return (len(self._peers) + 1) // 2 + 1   # N // 2 + 1

    def _send_to_peer(self, addr, request):
        """True if the peer acknowledged. Every call to a peer goes through here."""
        try:
            reply = self._stubs[addr].Replicate(request, timeout=self._peer_timeout)
            return reply.success
        except grpc.RpcError:
            return False

    def _replicate(self, request):
        """Send the write to all peers in parallel. Returns how many said yes."""
        results = self._pool.map(lambda addr: self._send_to_peer(addr, request),
                                 self._peers)
        return sum(1 for ok in results if ok)

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
        """Apply a write at most once per idempotency key.
        Returns the timestamp that belongs to this write."""
        with self._lock:
            key = request.idempotency_key
            if key and key in self._seen:
                return self._seen[key]       # retry: reuse the original timestamp
            self._apply_locked(request.key, request.value, timestamp)
            if key:
                self._seen[key] = timestamp
            return timestamp

    # ---------------- RPCs ----------------
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

    def Get(self, request, context):
        with self._lock:
            entry = self._data.get(request.key)
        if entry is None:
            return kvstore_pb2.GetReply(found=False)
        timestamp, value = entry
        return kvstore_pb2.GetReply(
            value=value, found=True, read_timestamp=timestamp, is_stale=False
        )


def make_server(port=0, name="replica-A", peers=(), max_workers=8, peer_timeout=1.0):
    servicer = KVStoreServicer(name, peers, peer_timeout)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    kvstore_pb2_grpc.add_KeyValueStoreServicer_to_server(servicer, server)
    bound_port = server.add_insecure_port(f"127.0.0.1:{port}")
    return server, bound_port, servicer


def main():
    parser = argparse.ArgumentParser(description="KV store replica")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--name", default="replica-A")
    parser.add_argument("--peers", default="",
                        help="comma-separated addresses of the OTHER replicas")
    args = parser.parse_args()
    peers = [p for p in args.peers.split(",") if p]

    server, port, _ = make_server(args.port, args.name, peers)
    server.start()
    print(f"{args.name} listening on 127.0.0.1:{port}, peers={peers}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=0)


if __name__ == "__main__":
    main()