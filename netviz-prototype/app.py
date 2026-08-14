"""
netviz-prototype
-----------------
A small, self-contained recreation of the "packets that smoke/catch fire
when they're slow" idea (in the spirit of the old NetQoS SuperAgent 3D demo).

Zero third-party dependencies -- everything here is Python standard library
(asyncio for the server, hashlib/struct for the WebSocket handshake/framing).
That's a deliberate choice: it runs the same way in a bare `python:3-slim`
Docker container, in WSL2, or straight on Windows with no `pip install` and
no internet access required at run time.

What's REAL here:
  - Each "server" is an actual TCP listener on localhost.
  - "server_ms" is a genuinely measured client-to-server request/response
    time against that real socket (with an adjustable artificial processing
    delay inside the server, so you can dial in how slow it "feels").

What's SIMULATED (and clearly labeled as such):
  - "network_ms" starts from the real TCP connect time, then adds an
    adjustable synthetic "WAN delay" on top -- because on loopback there's
    no real WAN to measure. Swap this stage for a real passive-capture
    pipeline (see README.md) and nothing else in this app has to change,
    because the WebSocket JSON message schema is the seam between the two.

Run:
    python3 app.py
    open http://localhost:8765
"""
import asyncio
import base64
import hashlib
import json
import random
import struct
import time

HOST = "0.0.0.0"
PORT = 8765
WS_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

# ---------------------------------------------------------------------------
# Topology: edit this to add/remove simulated nodes.
# ---------------------------------------------------------------------------
NODES = {
    "server-fast": {
        "label": "Fast App Server",
        "port": 9101,
        "server_delay_ms": 15,   # real: injected sleep inside the demo backend
        "wan_delay_ms": 5,       # simulated: added on top of real connect time
    },
    "server-db": {
        "label": "Database Tier",
        "port": 9102,
        "server_delay_ms": 40,
        "wan_delay_ms": 10,
    },
    "server-flaky-wan": {
        "label": "Remote Branch Link",
        "port": 9103,
        "server_delay_ms": 20,
        "wan_delay_ms": 15,
    },
}

clients = set()  # connected WSClient instances


# ---------------------------------------------------------------------------
# Demo backends + prober (the "real measurement" half)
# ---------------------------------------------------------------------------
async def demo_backend(node_id: str):
    """A real TCP server standing in for 'the application'. It waits for a
    one-line request, sleeps for the configured processing delay, then
    replies -- so server_ms below is a genuine measurement, not a fake."""
    cfg = NODES[node_id]

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            await reader.readline()
            await asyncio.sleep(cfg["server_delay_ms"] / 1000)
            writer.write(b"OK\n")
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            writer.close()

    return await asyncio.start_server(handle, "127.0.0.1", cfg["port"])


async def prober(node_id: str):
    """Repeatedly measures network_ms and server_ms against one node and
    broadcasts the sample to every connected visualization client."""
    cfg = NODES[node_id]
    while True:
        try:
            t0 = time.perf_counter()
            reader, writer = await asyncio.open_connection("127.0.0.1", cfg["port"])

            # Real loopback connect time is near-zero, so layer a synthetic
            # WAN delay on top -- clearly a stand-in for a real network hop.
            await asyncio.sleep(cfg["wan_delay_ms"] / 1000)
            network_ms = (time.perf_counter() - t0) * 1000

            writer.write(b"GET /ping\n")
            await writer.drain()
            t_sent = time.perf_counter()
            await asyncio.wait_for(reader.readline(), timeout=5)
            server_ms = (time.perf_counter() - t_sent) * 1000
            writer.close()

            await broadcast({
                "type": "sample",
                "node": node_id,
                "label": cfg["label"],
                "network_ms": round(network_ms, 1),
                "server_ms": round(server_ms, 1),
                "ts": time.time(),
            })
        except Exception as exc:
            await broadcast({"type": "error", "node": node_id, "message": str(exc)})

        await asyncio.sleep(0.5 + random.random() * 0.3)


# ---------------------------------------------------------------------------
# Minimal stdlib WebSocket server (RFC 6455) -- no external packages.
# ---------------------------------------------------------------------------
class WSClient:
    def __init__(self, writer: asyncio.StreamWriter):
        self.writer = writer

    async def send_json(self, obj):
        payload = json.dumps(obj).encode()
        header = bytearray([0x81])  # FIN + text opcode
        n = len(payload)
        if n <= 125:
            header.append(n)
        elif n <= 0xFFFF:
            header.append(126)
            header += struct.pack(">H", n)
        else:
            header.append(127)
            header += struct.pack(">Q", n)
        self.writer.write(bytes(header) + payload)
        await self.writer.drain()


async def broadcast(event: dict):
    if not clients:
        return
    dead = []
    for c in list(clients):
        try:
            await c.send_json(event)
        except Exception:
            dead.append(c)
    for c in dead:
        clients.discard(c)


async def read_http_headers(reader: asyncio.StreamReader):
    raw = await reader.readuntil(b"\r\n\r\n")
    lines = raw.decode("iso-8859-1").split("\r\n")
    request_line = lines[0]
    headers = {}
    for line in lines[1:]:
        if not line or ":" not in line:
            continue
        k, v = line.split(":", 1)
        headers[k.strip().lower()] = v.strip()
    parts = request_line.split()
    method, path = (parts[0], parts[1]) if len(parts) >= 2 else ("GET", "/")
    return method, path, headers


async def read_ws_frame(reader: asyncio.StreamReader):
    b1, b2 = await reader.readexactly(2)
    opcode = b1 & 0x0F
    masked = b2 & 0x80
    length = b2 & 0x7F
    if length == 126:
        length = struct.unpack(">H", await reader.readexactly(2))[0]
    elif length == 127:
        length = struct.unpack(">Q", await reader.readexactly(8))[0]
    mask = await reader.readexactly(4) if masked else b""
    payload = await reader.readexactly(length) if length else b""
    if masked:
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return opcode, payload


def handle_client_message(data: dict):
    if data.get("type") != "set_delay":
        return
    node = NODES.get(data.get("node"))
    if not node:
        return
    if "server_delay_ms" in data:
        node["server_delay_ms"] = max(0, float(data["server_delay_ms"]))
    if "wan_delay_ms" in data:
        node["wan_delay_ms"] = max(0, float(data["wan_delay_ms"]))


async def serve_static(writer: asyncio.StreamWriter, path: str):
    if path != "/":
        body = b"not found"
        writer.write(
            b"HTTP/1.1 404 Not Found\r\nContent-Length: " + str(len(body)).encode() +
            b"\r\nConnection: close\r\n\r\n" + body
        )
        await writer.drain()
        return
    with open("viz.html", "rb") as f:
        body = f.read()
    writer.write(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
        b"Content-Length: " + str(len(body)).encode() +
        b"\r\nConnection: close\r\n\r\n" + body
    )
    await writer.drain()


async def handle_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        method, path, headers = await read_http_headers(reader)
    except (asyncio.IncompleteReadError, ConnectionResetError):
        writer.close()
        return

    if path == "/ws" and headers.get("upgrade", "").lower() == "websocket":
        key = headers.get("sec-websocket-key", "")
        accept = base64.b64encode(hashlib.sha1((key + WS_MAGIC).encode()).digest()).decode()
        writer.write(
            b"HTTP/1.1 101 Switching Protocols\r\n"
            b"Upgrade: websocket\r\nConnection: Upgrade\r\n"
            b"Sec-WebSocket-Accept: " + accept.encode() + b"\r\n\r\n"
        )
        await writer.drain()

        client = WSClient(writer)
        clients.add(client)
        await client.send_json({
            "type": "topology",
            "nodes": [{"id": nid, "label": c["label"]} for nid, c in NODES.items()],
        })
        try:
            while True:
                opcode, payload = await read_ws_frame(reader)
                if opcode == 0x8:  # close
                    break
                if opcode == 0x1:  # text
                    try:
                        handle_client_message(json.loads(payload.decode()))
                    except Exception:
                        pass
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            clients.discard(client)
            writer.close()
    else:
        await serve_static(writer, path)
        writer.close()


async def main():
    backend_servers = []
    prober_tasks = []
    for node_id in NODES:
        backend_servers.append(await demo_backend(node_id))
        prober_tasks.append(asyncio.create_task(prober(node_id)))

    server = await asyncio.start_server(handle_connection, HOST, PORT)
    print(f"netviz-prototype listening on http://{HOST}:{PORT}  (open http://localhost:{PORT})")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
