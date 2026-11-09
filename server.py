"""One replica of the key-value store (step 3: single node, no replication yet)."""
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
    def __init__(self, name="replica-A"):
        self.name = name
        self._lock = threading.Lock()
        self._data = {}   # key -> (timestamp_ms, value)
        self._seen = {}   # idempotency_key -> reply we already gave

    def _apply_locked(self, key, value, timestamp):
        """Last-Write-Wins. The caller must already hold self._lock."""
        new = (timestamp, value)
        old = self._data.get(key)
        if old is None or new > old:
            self._data[key] = new
            return True
        return False

    def Put(self, request, context):
        timestamp = request.timestamp or now_ms()
        with self._lock:
            key = request.idempotency_key
            if key and key in self._seen:
                return self._seen[key]          # retry: do not apply again
            self._apply_locked(request.key, request.value, timestamp)
            reply = kvstore_pb2.PutReply(success=True, acks_received=1)
            if key:
                self._seen[key] = reply
        return reply

    def Get(self, request, context):
        with self._lock:
            entry = self._data.get(request.key)
        if entry is None:
            return kvstore_pb2.GetReply(found=False)
        timestamp, value = entry
        return kvstore_pb2.GetReply(
            value=value, found=True, read_timestamp=timestamp, is_stale=False
        )


def make_server(port=0, name="replica-A", max_workers=8):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    kvstore_pb2_grpc.add_KeyValueStoreServicer_to_server(KVStoreServicer(name), server)
    bound_port = server.add_insecure_port(f"127.0.0.1:{port}")
    return server, bound_port


def main():
    parser = argparse.ArgumentParser(description="KV store replica")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--name", default="replica-A")
    args = parser.parse_args()

    server, port = make_server(args.port, args.name)
    server.start()
    print(f"{args.name} listening on 127.0.0.1:{port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=0)


if __name__ == "__main__":
    main()