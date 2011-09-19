# -*- coding: utf-8 -*-
"""
generic_subscriber.py

simply subscribe to the benchmark PUB socket and dump all messages"
"""
import json
import os
import sys

import zmq

_pub_address = os.environ.get(
    "MOTOBOTO_BENCHMARK_PUB_ADDRESS", 
    "ipc:///tmp/motoboto_benchmark-main-pub/socket"
)
_topic = "" # all

def main():
    """
    main entry point
    """
    global _topic

    context = zmq.Context()
    sub_socket = context.socket(zmq.SUB)
    sub_socket.setsockopt(zmq.SUBSCRIBE, _topic)
    sub_socket.connect(_pub_address)

    while True:
        try:
            _topic = sub_socket.recv()
        except KeyboardInterrupt:
            break

        assert sub_socket.rcvmore()
        message = sub_socket.recv_json()
        print json.dumps(message, sort_keys=True, indent=2)

    sub_socket.close()
    context.term()

if __name__ == "__main__":
    sys.exit(main())

