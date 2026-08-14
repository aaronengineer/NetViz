# netviz-prototype

A small recreation of the old NetQoS SuperAgent idea: watch network transit
time and server response time as packets flying between nodes, where slow
ones smoke, and very slow ones catch fire.

This is a **prototype of the visualization concept**, not a production
monitoring tool. It proves out the pipeline (measure -> stream -> render ->
react) with real socket-level timing on localhost, so you can see the smoke
and fire respond to real numbers before investing in a real capture backend.

## What's real vs. simulated

- **`server_ms` is genuinely measured.** Each "server" in the topology is an
  actual TCP listener. The client (the `prober` coroutine in `app.py`)
  connects for real, sends a real request, and times the real response.
  There's an artificial `asyncio.sleep()` inside the demo backend so you can
  control how slow it is, but the round-trip timing around it is real.
- **`network_ms` is simulated.** On localhost there's no real WAN to
  measure, so the app takes the (near-zero) real TCP connect time and adds
  an adjustable synthetic delay on top, clearly so it can be dialed up on
  demand. See "Next steps" below for how to swap this for something real.
- **Zero third-party dependencies.** Everything is Python's standard
  library (`asyncio` for the server and both demo backends, plus a ~60-line
  hand-rolled WebSocket handshake/framing implementation, since this
  environment didn't have the `websockets`/`wsproto` packages available and
  it turned out to be a better answer anyway: no `pip install` needed, no
  internet access needed at run time, and it'll behave identically in any
  plain `python:3-slim` container).

## Running it

**Directly (needs only Python 3, nothing else):**

```
python3 app.py
```

Then open http://localhost:8765 — you'll see three server nodes orbiting a
central client node, with small packet sprites travelling between them. Each
node has two live sliders in the side panel:

- `wan_delay_ms` — raises the simulated network transit time for that path.
  Push it past ~50ms and the packets start trailing light smoke; past
  ~150ms it's heavy smoke; past ~350ms the path is on fire.
- `server_delay_ms` — raises the real, measured server response time. The
  server node itself glows, smokes, or catches fire at the same thresholds.

Both sliders take effect on the very next sample (roughly within a second),
so you can watch a healthy path degrade in real time.

**With Docker** (same pattern as your perkan stack):

```
docker compose up --build -d
```

then open http://localhost:8765 (change `HOST_PORT` in a `.env` file next
to `docker-compose.yml` if 8765 is taken). Note: the Docker build wasn't
testable in the sandbox this was built in (no Docker daemon available
there), but the Dockerfile is about as simple as they come — a slim Python
base image copying two files, no dependencies to install — so it should
build cleanly; flag it back to me if it doesn't and I'll help debug.

## Tuning

Thresholds live at the top of `viz.html` (`const THRESH = { light: 50,
heavy: 150, fire: 350 };`) and the topology/base delays live at the top of
`app.py` (`NODES = {...}`) — add more nodes, rename them, or change their
baseline delays there.

## Next steps: what's possible for free in your own environment

The visualization and streaming layer here (WebSocket -> Three.js) doesn't
need to change at all as you upgrade the data source — only the thing
feeding `broadcast({"type": "sample", ...})` needs to change. A few
realistic upgrade paths, roughly in order of effort:

1. **Active probing against real hosts on your LAN** (least effort, no
   elevated privileges needed). Instead of `prober()` talking to a local
   demo backend, point it at real machines — your perkan Docker host, your
   router, another VM — timing real TCP connects and real HTTP
   request/response cycles. This gets you genuinely real `network_ms` (real
   connect time to a real host) and `server_ms` (real app response time),
   just via active probes rather than a passive tap. This is a very
   reasonable permanent architecture for a homelab, and it's what a lot of
   commercial "synthetic monitoring" tools actually do.

2. **Passive capture via Zeek** (free, does the SYN/SYN-ACK and
   request/first-byte timing math for you). Zeek writes a `conn.log` with
   round-trip-time-ish fields for every connection it sees. Point it at a
   mirrored/SPAN port on a managed switch (many inexpensive "smart" switches
   support port mirroring — this is the one place a small hardware cost
   might show up if you don't already have one) or at a Linux box doing the
   routing, and have a small script tail `conn.log` and re-emit matching
   `sample` events over the same WebSocket schema this prototype already
   uses.

3. **Passive capture via raw `tshark`/`scapy`**, reimplementing the actual
   SYN -> SYN-ACK and PSH -> first-response-byte deltas yourself, if you
   want the exact NetQoS-style math rather than Zeek's own connection
   summary stats. More work, but it's genuinely simple TCP-flag arithmetic
   once you have a capture to feed it.

A practical note for your specific setup: you're running Docker Desktop on
Windows. Docker Desktop's Linux VM makes `--network host` and raw packet
capture from *inside* a container awkward on Windows (unlike native Linux
Docker). If you want to go the passive-capture route, it's easier to run
the capture piece (Zeek, tshark) directly under WSL2 or natively on Windows
with [Npcap](https://npcap.com/) (free for personal use, same driver
Wireshark uses) rather than trying to get a container's virtual interface
to see real LAN traffic. Option 1 above sidesteps all of that entirely,
which is why it's worth trying first.
