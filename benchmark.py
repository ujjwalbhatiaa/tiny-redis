#!/usr/bin/env python3
"""Quick throughput benchmark: SET/GET round-trips against a local server."""

import socket
import threading
import time

from tiny_redis import ThreadedServer, Database, _Handler


def main(n=20000):
    server = ThreadedServer(("127.0.0.1", 0), _Handler)
    server.db = Database()
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    sock = socket.create_connection(("127.0.0.1", port))

    def raw(cmd):
        out = b"*%d\r\n" % len(cmd)
        for a in cmd:
            out += b"$%d\r\n%s\r\n" % (len(a), a)
        sock.sendall(out)
        # read one reply (enough for +OK / bulk)
        data = b""
        while not data.endswith(b"\r\n"):
            data += sock.recv(65536)
        return data

    t0 = time.time()
    for i in range(n):
        raw([b"SET", f"k{i}".encode(), b"x" * 16])
    set_s = time.time() - t0

    t0 = time.time()
    for i in range(n):
        raw([b"GET", f"k{i}".encode()])
    get_s = time.time() - t0

    print(f"SET: {n / set_s:,.0f} ops/sec")
    print(f"GET: {n / get_s:,.0f} ops/sec")
    server.shutdown()


if __name__ == "__main__":
    main()
