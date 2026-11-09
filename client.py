"""Command-line client for the key-value store."""
import argparse
import sys
import uuid
import grpc

import kvstore_pb2
import kvstore_pb2_grpc


class KVClient:
    def __init__(self, target, deadline=3.0):
        self._channel = grpc.insecure_channel(target)
        self._stub = kvstore_pb2_grpc.KeyValueStoreStub(self._channel)
        self._deadline = deadline

    def put(self, key, value, idempotency_key=None,
            strategy=kvstore_pb2.QUORUM_CP, timestamp=0):
        request = kvstore_pb2.PutRequest(
            key=key,
            value=value,
            idempotency_key=idempotency_key or str(uuid.uuid4()),
            strategy=strategy,
            timestamp=timestamp,
        )
        return self._stub.Put(request, timeout=self._deadline)

    def get(self, key, consistency=kvstore_pb2.LINEARIZABLE, max_staleness_ms=0):
        request = kvstore_pb2.GetRequest(
            key=key, consistency=consistency, max_staleness_ms=max_staleness_ms
        )
        return self._stub.Get(request, timeout=self._deadline)

    def close(self):
        self._channel.close()


def main():
    parser = argparse.ArgumentParser(description="KV store client")
    parser.add_argument("--addr", default="127.0.0.1:50051")
    sub = parser.add_subparsers(dest="cmd", required=True)

    put = sub.add_parser("put")
    put.add_argument("key")
    put.add_argument("value")
    put.add_argument("--idem", default=None, help="idempotency key (default: random)")
    put.add_argument("--strategy", choices=["cp", "ap"], default="cp")
    put.add_argument("--ts", type=int, default=0,
                     help="explicit timestamp in ms (default: now)")	

    get = sub.add_parser("get")
    get.add_argument("key")
    get.add_argument("--mode", choices=["lin", "stale"], default="lin")
    get.add_argument("--max-staleness-ms", type=int, default=5000)

    args = parser.parse_args()
    client = KVClient(args.addr)
    try:
        if args.cmd == "put":
            strategy = (kvstore_pb2.QUORUM_CP if args.strategy == "cp"
                        else kvstore_pb2.ALWAYS_AP)
            reply = client.put(args.key, args.value, args.idem, strategy, args.ts)
            if reply.success:
                print(f"OK acks={reply.acks_received}")
            else:
                print(f"FAILED: {reply.error_message}")
                sys.exit(1)
        else:
            mode = (kvstore_pb2.LINEARIZABLE if args.mode == "lin"
                    else kvstore_pb2.FOLLOWER_STALE)
            reply = client.get(args.key, mode, args.max_staleness_ms)
            print(f"found={reply.found} value={reply.value!r} "
                  f"timestamp={reply.read_timestamp} stale={reply.is_stale}")
    except grpc.RpcError as err:
        print(f"RPC error: {err.code().name}: {err.details()}")
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()