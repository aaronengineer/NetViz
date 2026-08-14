"""
nettrace-sankey
---------------
Extends the netviz-prototype idea from raw TCP timing to application-level
call graphs: a small multi-service demo app where each service can call
other services (mirroring a real microservice topology), every hop is
independently and genuinely timed, and the result is streamed to a
Sankey-style flow diagram instead of a 3D scene.

Zero third-party PYTHON dependencies -- just asyncio, hashlib, struct, json.
(This version does talk to one external process over the network: a real,
unmodified OpenTelemetry Collector -- see below.)

Two run modes (NETTRACE_MODE env var):
  demo (default) -- runs the small simulated "webshop" described by
    topology.demo.json (or whatever TOPOLOGY_FILE points at), with a load
    generator producing continuous synthetic traffic. Two data sources feed
    the visualization side by side in this mode:
      1. In-band: the callee reports its own measured handling time back to
         the caller directly in its response, so the caller computes the
         network/server split itself. This is what drives the Sankey's
         colors, widths, and flow animation in demo mode -- it always
         works, no external dependency required.
      2. Real OpenTelemetry traces: every call ALSO emits a real,
         spec-compliant pair of spans (CLIENT span from the caller, SERVER
         span from the callee) as OTLP/HTTP+JSON to a real OpenTelemetry
         Collector. The Collector's `service_graph` connector independently
         re-derives the same client/server timing split and exposes it as
         Prometheus metrics, which this app scrapes and shows alongside the
         in-band numbers as a live "does the real pipeline agree" check.
  otel -- for pointing this at YOUR OWN real services instead of the demo.
    No simulated services or load generator run at all; the app only
    scrapes a Collector's Prometheus /metrics endpoint (fed by the
    `service_graph` connector, fed in turn by spans from your real,
    independently-instrumented services) and renders whatever topology and
    latency it discovers there, live. This is the actual "deploy against a
    real environment" story: you don't run any of this app's Python against
    your services at all -- you just aim a Collector at them the normal
    OpenTelemetry way, and aim this app's OTEL_COLLECTOR_* env vars at that
    Collector. See README.md's "Deploying against real services" section.

Span/metric export and scraping are both best-effort and fire-and-forget:
network failures to the Collector are caught and logged once, never raised.

Run standalone, demo mode (OTel panel will just show "waiting for collector"):
    python3 app.py
    open http://localhost:8766

Run with a real Collector (see otel-collector-config.yaml / docker-compose.yml):
    docker compose up --build -d

Run in otel-only mode against your own services + your own Collector:
    NETTRACE_MODE=otel OTEL_COLLECTOR_HOST=my-collector python3 app.py
"""
import asyncio
import base64
import hashlib
import json
import os
import random
import re
import struct
import time
from collections import deque

HOST = "0.0.0.0"
PORT = 8766
WS_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
WINDOW_SECONDS = 5.0
BROADCAST_INTERVAL = 1.0

# ---------------------------------------------------------------------------
# Run mode. "demo" (default) runs the built-in simulated services; "otel"
# runs nothing locally and only visualizes whatever a real Collector reports
# about your real services. See the module docstring above.
# ---------------------------------------------------------------------------
NETTRACE_MODE = os.environ.get("NETTRACE_MODE", "demo").strip().lower()
if NETTRACE_MODE not in ("demo", "otel"):
    print(f"[nettrace] unrecognized NETTRACE_MODE={NETTRACE_MODE!r}, falling back to 'demo'", flush=True)
    NETTRACE_MODE = "demo"

TOPOLOGY_FILE = os.environ.get("TOPOLOGY_FILE", "topology.demo.json")

# In otel mode, edges with no fresh sample for this long are dropped from
# the live graph (a service that's gone quiet shouldn't linger forever).
OTEL_EDGE_STALE_AFTER_S = 20.0

# ---------------------------------------------------------------------------
# OpenTelemetry Collector connection. Defaults match the docker-compose
# service name ("otel-collector"); override for a bare `python3 app.py` run
# pointed at a Collector running elsewhere, e.g.:
#   OTEL_COLLECTOR_HOST=localhost python3 app.py
# ---------------------------------------------------------------------------
OTEL_COLLECTOR_HOST = os.environ.get("OTEL_COLLECTOR_HOST", "otel-collector")
OTEL_COLLECTOR_HTTP_PORT = int(os.environ.get("OTEL_COLLECTOR_HTTP_PORT", "4318"))
OTEL_COLLECTOR_METRICS_PORT = int(os.environ.get("OTEL_COLLECTOR_METRICS_PORT", "8889"))
OTEL_SCRAPE_INTERVAL = 2.0
OTEL_SERVICE_NAMESPACE = "nettrace-sankey"

# ---------------------------------------------------------------------------
# Demo topology: loaded from an external JSON file (topology.demo.json by
# default) instead of being hardcoded here, so a fork can reshape the demo
# -- add/remove services, change call fan-out, retune delays -- by editing
# JSON, not Python. See topology.demo.json for the schema and comments.
# Unused entirely in NETTRACE_MODE=otel.
# ---------------------------------------------------------------------------
SERVICES = {}
CLIENT_LABEL = "Client Traffic"
ENTRY = {"target": "gateway", "op": "handle_request", "min_interval_s": 0.15, "max_interval_s": 0.40}
ALL_NODE_IDS = []
NODE_LABELS = {}


def load_demo_topology(path):
    global SERVICES, CLIENT_LABEL, ENTRY, ALL_NODE_IDS, NODE_LABELS
    with open(path) as f:
        cfg = json.load(f)
    CLIENT_LABEL = cfg.get("client_label", "Client Traffic")
    ENTRY = cfg.get("entrypoint", ENTRY)
    SERVICES = cfg["services"]
    ALL_NODE_IDS = ["client"] + list(SERVICES.keys())
    NODE_LABELS = {"client": CLIENT_LABEL, **{k: v["label"] for k, v in SERVICES.items()}}


EDGE_DELAY = {}  # "source->target" -> wan_delay_ms (defaults applied lazily), demo mode only
DEFAULT_WAN_DELAY_MS = 6

# rolling per-edge sample history: "source->target" -> deque[(ts, network_ms, server_ms)]
# (demo mode's in-band measurements)
EDGE_SAMPLES = {}

clients = set()  # connected WSClient instances


def edge_key(source, target):
    return f"{source}->{target}"


def get_wan_delay(source, target):
    return EDGE_DELAY.get(edge_key(source, target), DEFAULT_WAN_DELAY_MS)


def record_sample(source, target, network_ms, server_ms):
    key = edge_key(source, target)
    buf = EDGE_SAMPLES.setdefault(key, deque())
    buf.append((time.time(), network_ms, server_ms))


# ---------------------------------------------------------------------------
# Minimal raw-socket HTTP client (POST JSON / GET text) -- same "no
# third-party packages" philosophy as the WebSocket server below, just used
# here as a client instead of a server, to talk to the OTel Collector.
# ---------------------------------------------------------------------------
async def http_post_json(host, port, path, obj, timeout=2.0):
    body = json.dumps(obj).encode()
    request = (
        f"POST {path} HTTP/1.1\r\nHost: {host}\r\n"
        f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\n"
        f"Connection: close\r\n\r\n"
    ).encode() + body
    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(request)
        await writer.drain()
        status_line = await asyncio.wait_for(reader.readline(), timeout=timeout)
        return status_line.decode(errors="replace").strip()
    finally:
        writer.close()


async def http_get_text(host, port, path, timeout=2.0):
    request = f"GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n".encode()
    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(request)
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(), timeout=timeout)
    finally:
        writer.close()
    if b"\r\n\r\n" not in raw:
        return ""
    head, _, body = raw.partition(b"\r\n\r\n")
    # Handle chunked transfer-encoding, which the Collector's Prometheus
    # exporter (and most Go HTTP servers) use by default.
    if b"chunked" in head.lower():
        out = bytearray()
        rest = body
        while rest:
            size_line, _, rest = rest.partition(b"\r\n")
            try:
                size = int(size_line.strip(), 16)
            except ValueError:
                break
            if size == 0:
                break
            out += rest[:size]
            rest = rest[size + 2:]  # skip the chunk's trailing \r\n
        return out.decode(errors="replace")
    return body.decode(errors="replace")


# ---------------------------------------------------------------------------
# Real OpenTelemetry span export -- OTLP/HTTP, JSON encoding, no protobuf
# dependency needed. Field names, hex ID encoding, and the SpanKind /
# StatusCode numeric enums below all match the official OTLP specification
# (https://opentelemetry.io/docs/specs/otlp/,
#  https://github.com/open-telemetry/opentelemetry-proto). Demo mode only --
# in otel mode this app never emits spans, only reads them back out via the
# Collector's Prometheus exporter.
# ---------------------------------------------------------------------------
SPAN_KIND_SERVER = 2
SPAN_KIND_CLIENT = 3
STATUS_CODE_OK = 1

_otel_warned = False


def new_trace_id():
    return os.urandom(16).hex()  # 128-bit, per spec


def new_span_id():
    return os.urandom(8).hex()  # 64-bit, per spec


async def emit_span(service_name, trace_id, span_id, parent_span_id, kind, name, start_perf, end_perf, wall_start):
    """Fire-and-forget OTLP/HTTP export of one span. `start_perf`/`end_perf`
    are time.perf_counter() readings (for an accurate duration); wall_start
    is a time.time() reading close to start_perf, used to anchor perf_counter
    (monotonic, no epoch) to a real wall-clock timestamp for the span."""
    global _otel_warned
    # Anchor to a wall-clock timestamp once, then use the monotonic
    # perf_counter delta for duration (immune to wall-clock adjustments).
    start_unix_ns = int(wall_start * 1e9)
    duration_ns = int(max(0.0, end_perf - start_perf) * 1e9)
    span = {
        "traceId": trace_id,
        "spanId": span_id,
        "name": name,
        "kind": kind,
        "startTimeUnixNano": str(start_unix_ns),
        "endTimeUnixNano": str(start_unix_ns + duration_ns),
        "status": {"code": STATUS_CODE_OK},
    }
    if parent_span_id:
        span["parentSpanId"] = parent_span_id
    payload = {
        "resourceSpans": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": service_name}},
                {"key": "service.namespace", "value": {"stringValue": OTEL_SERVICE_NAMESPACE}},
            ]},
            "scopeSpans": [{
                "scope": {"name": "nettrace-sankey.manual"},
                "spans": [span],
            }],
        }],
    }
    try:
        await http_post_json(OTEL_COLLECTOR_HOST, OTEL_COLLECTOR_HTTP_PORT, "/v1/traces", payload, timeout=1.5)
    except Exception as exc:
        if not _otel_warned:
            _otel_warned = True
            print(f"[otel] couldn't reach collector at {OTEL_COLLECTOR_HOST}:{OTEL_COLLECTOR_HTTP_PORT} "
                  f"({exc!r}) -- span export will keep failing silently; the in-band Sankey view is unaffected.",
                  flush=True)


# ---------------------------------------------------------------------------
# Service call + handler machinery (the "real measurement" half). Demo mode
# only -- none of this runs in NETTRACE_MODE=otel.
# ---------------------------------------------------------------------------
async def call_service(source: str, target: str, op: str, trace_id: str, parent_span_id: str = None):
    """Open a real connection to `target`, time the hop, record a sample,
    and emit a real OTel CLIENT span for this hop (fire-and-forget).

    `parent_span_id` is whatever span logically initiated this call (the
    caller's own SERVER span, if the caller is itself handling a request;
    None for the very first hop, making it the trace root). This call's own
    freshly-generated span id is sent to the callee so its SERVER span can
    be parented to it -- the same client/server pairing a real OTel
    Collector's service_graph connector matches on.
    """
    cfg = SERVICES[target]
    wan_delay_ms = get_wan_delay(source, target)
    client_span_id = new_span_id()

    # t0 starts BEFORE the connect + simulated WAN delay, and total_round_trip_ms
    # below is measured all the way through, so the synthetic delay is actually
    # inside the window this hop's network_ms is derived from.
    t0 = time.perf_counter()
    wall_t0 = time.time()
    reader, writer = await asyncio.open_connection("127.0.0.1", cfg["port"])
    await asyncio.sleep(wan_delay_ms / 1000)  # simulated WAN delay, this hop only

    writer.write(f"CALL {op} {trace_id} {client_span_id}\n".encode())
    await writer.drain()
    line = await asyncio.wait_for(reader.readline(), timeout=5)
    t_end = time.perf_counter()
    total_round_trip_ms = (t_end - t0) * 1000
    writer.close()

    server_ms = total_round_trip_ms  # fallback if parsing fails
    try:
        text = line.decode().strip()
        if "server_ms=" in text:
            server_ms = float(text.split("server_ms=")[1])
    except Exception:
        pass

    # network_ms for this hop = everything the caller waited on (including
    # the simulated WAN delay), minus the time the callee says it actually
    # spent handling the request.
    hop_network_ms = max(0.0, total_round_trip_ms - server_ms)
    record_sample(source, target, hop_network_ms, server_ms)

    asyncio.create_task(emit_span(
        source, trace_id, client_span_id, parent_span_id,
        SPAN_KIND_CLIENT, f"{source} calls {target}.{op}", t0, t_end, wall_t0,
    ))
    return server_ms, client_span_id


def make_service_handler(service_id: str):
    cfg = SERVICES[service_id]

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            line = await reader.readline()
            if not line:
                return
            parts = line.decode().strip().split()
            trace_id = parts[2] if len(parts) >= 3 else new_trace_id()
            # parts[3], if present, is the caller's CLIENT span id for this
            # hop -- this service's own SERVER span is its child.
            incoming_client_span_id = parts[3] if len(parts) >= 4 else None
            my_span_id = new_span_id()

            t_start = time.perf_counter()
            wall_start = time.time()
            await asyncio.sleep(cfg["own_delay_ms"] / 1000)  # this service's own work
            # Independent downstream calls run concurrently, like a real
            # gateway fanning out to services that don't depend on each
            # other. Each one is parented to THIS service's own span, so
            # they nest correctly: server(X) -> client(X calls Y) -> server(Y).
            fanout = [call_service(service_id, c["target"], c["op"], trace_id, parent_span_id=my_span_id)
                      for c in cfg["calls"] if random.random() < c["probability"]]
            if fanout:
                await asyncio.gather(*fanout)
            t_end = time.perf_counter()
            total_ms = (t_end - t_start) * 1000

            writer.write(f"OK server_ms={total_ms:.2f}\n".encode())
            await writer.drain()

            asyncio.create_task(emit_span(
                service_id, trace_id, my_span_id, incoming_client_span_id,
                SPAN_KIND_SERVER, f"{service_id} handles request", t_start, t_end, wall_start,
            ))
        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()

    return handle


async def load_generator():
    """Stands in for real user traffic hitting the entrypoint service. Each
    request is a fresh trace root -- no parent span, since nothing called
    `client`. Entrypoint target/op/cadence come from topology.demo.json."""
    lo, hi = ENTRY["min_interval_s"], ENTRY["max_interval_s"]
    while True:
        trace_id = new_trace_id()
        asyncio.create_task(call_service("client", ENTRY["target"], ENTRY["op"], trace_id))
        await asyncio.sleep(lo + random.random() * (hi - lo))


# ---------------------------------------------------------------------------
# Aggregation: turn rolling per-edge samples into a graph snapshot (demo mode).
# ---------------------------------------------------------------------------
def severity(ms):
    if ms is None:
        return "clean"
    if ms >= 350:
        return "fire"
    if ms >= 150:
        return "heavy"
    if ms >= 50:
        return "light"
    return "clean"


def build_graph_snapshot():
    now = time.time()
    edges = []
    for key, buf in EDGE_SAMPLES.items():
        while buf and now - buf[0][0] > WINDOW_SECONDS:
            buf.popleft()
        if not buf:
            continue
        source, target = key.split("->")
        avg_network = sum(s[1] for s in buf) / len(buf)
        avg_server = sum(s[2] for s in buf) / len(buf)
        rate_per_sec = len(buf) / WINDOW_SECONDS
        otel_info = _otel_latest_by_edge.get(key)
        otel_field = None
        if otel_info and now - otel_info.get("updated_at", 0) < 15:
            otel_field = {k: v for k, v in otel_info.items() if k != "updated_at"}
        edges.append({
            "source": source,
            "target": target,
            "count": len(buf),
            "rate_per_sec": round(rate_per_sec, 2),
            "avg_network_ms": round(avg_network, 1),
            "avg_server_ms": round(avg_server, 1),
            "severity_network": severity(avg_network),
            "severity_server": severity(avg_server),
            "otel": otel_field,  # None until a real Collector has produced data for this edge
        })
    nodes = [{"id": nid, "label": NODE_LABELS[nid]} for nid in ALL_NODE_IDS]
    return {"type": "graph", "mode": "demo", "ts": now, "nodes": nodes, "edges": edges,
            "otel_reachable": _otel_scrape_ok}


# ---------------------------------------------------------------------------
# otel-only mode: the live graph is derived ENTIRELY from what the Collector
# reports via its service_graph connector -- no local services, no in-band
# measurement. `avg_network_ms` here is approximated the same way it's
# conceptually defined in demo mode: the caller's total observed time for
# the hop (the CLIENT span's duration) minus the callee's own reported
# handling time (the SERVER span's duration) -- i.e. avg_client_ms -
# avg_server_ms, both already independently derived by the service_graph
# connector from real spans.
# ---------------------------------------------------------------------------
_otel_known_nodes = set()
_otel_known_edges = set()  # set of edge_key strings currently considered "live"


def build_otel_graph_snapshot():
    now = time.time()
    edges = []
    live_nodes = set()
    for key, info in _otel_latest_by_edge.items():
        if now - info.get("updated_at", 0) > OTEL_EDGE_STALE_AFTER_S:
            continue
        source, target = key.split("->")
        live_nodes.add(source)
        live_nodes.add(target)
        avg_server = info.get("avg_server_ms")
        avg_client = info.get("avg_client_ms")
        avg_network = max(0.0, avg_client - avg_server) if (avg_server is not None and avg_client is not None) else None
        rate = info.get("rate_per_sec", 0.0)
        edges.append({
            "source": source,
            "target": target,
            "count": max(1, round(rate * WINDOW_SECONDS)),
            "rate_per_sec": rate,
            "avg_network_ms": round(avg_network, 1) if avg_network is not None else 0.0,
            "avg_server_ms": round(avg_server, 1) if avg_server is not None else 0.0,
            "severity_network": severity(avg_network),
            "severity_server": severity(avg_server),
            "otel": None,  # this whole snapshot IS otel data; no separate verification badge needed
        })
    nodes = [{"id": nid, "label": nid} for nid in sorted(live_nodes)]
    return live_nodes, edges, {
        "type": "graph", "mode": "otel", "ts": now, "nodes": nodes, "edges": edges,
        "otel_reachable": _otel_scrape_ok,
    }


def otel_topology_message(live_nodes, edges):
    return {
        "type": "topology",
        "mode": "otel",
        "nodes": [{"id": nid, "label": nid} for nid in sorted(live_nodes)],
        "edges": [{"source": e["source"], "target": e["target"]} for e in edges],
    }


async def otel_only_broadcaster():
    """Demo mode uses aggregator() below (in-band driven, on a fixed clock).
    otel mode instead has nothing local to clock against, so this task both
    detects topology changes (new/gone services or edges observed by the
    Collector) and re-broadcasts the graph snapshot on the same cadence."""
    global _otel_known_nodes, _otel_known_edges
    while True:
        await asyncio.sleep(BROADCAST_INTERVAL)
        live_nodes, edges, snapshot = build_otel_graph_snapshot()
        live_edge_keys = {edge_key(e["source"], e["target"]) for e in edges}
        if live_nodes != _otel_known_nodes or live_edge_keys != _otel_known_edges:
            _otel_known_nodes, _otel_known_edges = live_nodes, live_edge_keys
            await broadcast(otel_topology_message(live_nodes, edges))
        await broadcast(snapshot)


async def aggregator():
    while True:
        await asyncio.sleep(BROADCAST_INTERVAL)
        await broadcast(build_graph_snapshot())


# ---------------------------------------------------------------------------
# OTel data source: scrape the real Collector's Prometheus output of the
# service_graph connector, and derive per-edge timing from it.
#
# In demo mode this never drives the visualization -- it's purely a live
# check that the real pipeline agrees with the in-band shortcut. In otel
# mode this IS the entire data source for the visualization.
#
# CAVEAT: the exact metric text below is written from the connector's
# documented metric names (traces_service_graph_request_total /
# _server / _client), but this was validated in development against a
# hand-built mock of the Collector's endpoints, not the real Go binary
# (see README.md's "What's been verified" section for details and
# troubleshooting steps). The metric name matching is deliberately fuzzy
# (prefix/suffix, not exact string) for exactly this reason.
# ---------------------------------------------------------------------------
METRIC_LINE_RE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)\{([^}]*)\}\s+([0-9.eE+\-]+|NaN|\+Inf|-Inf)\s*$')
LABEL_RE = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')

_otel_prev = {}             # edge_key -> {"total":, "server_sum":, "server_count":, "client_sum":, "client_count":, "ts":}
_otel_latest_by_edge = {}   # edge_key -> {"rate_per_sec":, "avg_server_ms":, "avg_client_ms":, "updated_at":}
_otel_scrape_ok = False
_otel_scrape_warned = False


def parse_prometheus_text(text):
    """Minimal, defensive Prometheus exposition-format parser: enough to
    pull out `traces_service_graph_request*` lines. Ignores comments,
    HELP/TYPE lines, and anything else the Collector might also expose."""
    out = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        m = METRIC_LINE_RE.match(line)
        if not m:
            continue
        name, label_str, value_str = m.groups()
        if not name.startswith('traces_service_graph_request'):
            continue
        try:
            value = float(value_str)
        except ValueError:
            continue
        labels = dict(LABEL_RE.findall(label_str))
        out.append((name, labels, value))
    return out


def aggregate_otel_metrics(metrics):
    """Group parsed metric lines into per-edge (client->server) totals.
    Deliberately matches names by prefix/suffix rather than exact string,
    since the Prometheus exporter may append a unit suffix (e.g. `_seconds`)
    to the histogram base names that isn't nailed down without a live run."""
    acc = {}
    for name, labels, value in metrics:
        client, server = labels.get('client'), labels.get('server')
        if not client or not server:
            continue
        key = edge_key(client, server)
        row = acc.setdefault(key, {"total": 0.0, "server_sum": 0.0, "server_count": 0.0,
                                    "client_sum": 0.0, "client_count": 0.0})
        if name == 'traces_service_graph_request_total':
            row["total"] = value
        elif name.startswith('traces_service_graph_request_server') and name.endswith('_sum'):
            row["server_sum"] = value
        elif name.startswith('traces_service_graph_request_server') and name.endswith('_count'):
            row["server_count"] = value
        elif name.startswith('traces_service_graph_request_client') and name.endswith('_sum'):
            row["client_sum"] = value
        elif name.startswith('traces_service_graph_request_client') and name.endswith('_count'):
            row["client_count"] = value
    return acc


async def otel_scrape_poller():
    global _otel_scrape_ok, _otel_scrape_warned
    while True:
        await asyncio.sleep(OTEL_SCRAPE_INTERVAL)
        try:
            text = await http_get_text(OTEL_COLLECTOR_HOST, OTEL_COLLECTOR_METRICS_PORT, "/metrics",
                                        timeout=OTEL_SCRAPE_INTERVAL - 0.3)
            agg = aggregate_otel_metrics(parse_prometheus_text(text))
            _otel_scrape_ok = True
        except Exception as exc:
            _otel_scrape_ok = False
            if not _otel_scrape_warned:
                _otel_scrape_warned = True
                print(f"[otel] couldn't scrape collector metrics at "
                      f"{OTEL_COLLECTOR_HOST}:{OTEL_COLLECTOR_METRICS_PORT}/metrics ({exc!r}) -- "
                      f"{'the live view will just stay empty' if NETTRACE_MODE == 'otel' else 'the OTel-verified panel will just stay empty'}; "
                      + ("this app has no other data source in otel mode." if NETTRACE_MODE == 'otel'
                         else "the in-band Sankey is unaffected."),
                      flush=True)
            continue

        now = time.time()
        for key, row in agg.items():
            prev = _otel_prev.get(key)
            _otel_prev[key] = {**row, "ts": now}
            if not prev:
                continue
            dt = max(0.001, now - prev["ts"])
            d_total = max(0.0, row["total"] - prev["total"])
            d_server_sum = max(0.0, row["server_sum"] - prev["server_sum"])
            d_server_count = max(0.0, row["server_count"] - prev["server_count"])
            d_client_sum = max(0.0, row["client_sum"] - prev["client_sum"])
            d_client_count = max(0.0, row["client_count"] - prev["client_count"])
            if d_server_count <= 0 and d_client_count <= 0 and d_total <= 0:
                continue  # nothing new since last scrape; keep showing the last real value
            entry = _otel_latest_by_edge.setdefault(key, {})
            entry["rate_per_sec"] = round(d_total / dt, 2)
            if d_server_count > 0:
                entry["avg_server_ms"] = round((d_server_sum / d_server_count) * 1000, 1)
            if d_client_count > 0:
                entry["avg_client_ms"] = round((d_client_sum / d_client_count) * 1000, 1)
            entry["updated_at"] = now


# ---------------------------------------------------------------------------
# Minimal stdlib WebSocket server (RFC 6455) -- same approach as netviz-prototype.
# ---------------------------------------------------------------------------
class WSClient:
    def __init__(self, writer: asyncio.StreamWriter):
        self.writer = writer

    async def send_json(self, obj):
        payload = json.dumps(obj).encode()
        header = bytearray([0x81])
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
    if NETTRACE_MODE != "demo":
        return  # no controls in otel mode -- nothing local to tune
    mtype = data.get("type")
    if mtype == "set_node_delay":
        cfg = SERVICES.get(data.get("node"))
        if cfg and "own_delay_ms" in data:
            cfg["own_delay_ms"] = max(0, float(data["own_delay_ms"]))
    elif mtype == "set_edge_delay":
        source, target = data.get("source"), data.get("target")
        if source and target and "wan_delay_ms" in data:
            EDGE_DELAY[edge_key(source, target)] = max(0, float(data["wan_delay_ms"]))


async def serve_static(writer: asyncio.StreamWriter, path: str):
    if path != "/":
        body = b"not found"
        writer.write(
            b"HTTP/1.1 404 Not Found\r\nContent-Length: " + str(len(body)).encode() +
            b"\r\nConnection: close\r\n\r\n" + body
        )
        await writer.drain()
        return
    with open("sankey.html", "rb") as f:
        body = f.read()
    writer.write(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
        b"Content-Length: " + str(len(body)).encode() +
        b"\r\nConnection: close\r\n\r\n" + body
    )
    await writer.drain()


def demo_topology_message():
    return {
        "type": "topology",
        "mode": "demo",
        "nodes": [{"id": nid, "label": NODE_LABELS[nid]} for nid in ALL_NODE_IDS],
        "entry": {"source": "client", "target": ENTRY["target"]},
        "services": {sid: {"own_delay_ms": cfg["own_delay_ms"],
                            "calls": [c["target"] for c in cfg["calls"]]}
                     for sid, cfg in SERVICES.items()},
    }


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
        if NETTRACE_MODE == "demo":
            await client.send_json(demo_topology_message())
            await client.send_json(build_graph_snapshot())
        else:
            live_nodes, edges, snapshot = build_otel_graph_snapshot()
            await client.send_json(otel_topology_message(live_nodes, edges))
            await client.send_json(snapshot)
        try:
            while True:
                opcode, payload = await read_ws_frame(reader)
                if opcode == 0x8:
                    break
                if opcode == 0x1:
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
    if NETTRACE_MODE == "demo":
        load_demo_topology(TOPOLOGY_FILE)
        for service_id in SERVICES:
            cfg = SERVICES[service_id]
            await asyncio.start_server(make_service_handler(service_id), "127.0.0.1", cfg["port"])
        asyncio.create_task(load_generator())
        asyncio.create_task(aggregator())
    else:
        asyncio.create_task(otel_only_broadcaster())

    asyncio.create_task(otel_scrape_poller())

    server = await asyncio.start_server(handle_connection, HOST, PORT)
    print(f"nettrace-sankey listening on http://{HOST}:{PORT}  (open http://localhost:{PORT})  mode={NETTRACE_MODE}")
    if NETTRACE_MODE == "demo":
        print(f"[otel] will export spans to http://{OTEL_COLLECTOR_HOST}:{OTEL_COLLECTOR_HTTP_PORT}/v1/traces "
              f"and scrape metrics from http://{OTEL_COLLECTOR_HOST}:{OTEL_COLLECTOR_METRICS_PORT}/metrics", flush=True)
    else:
        print(f"[otel] live mode: scraping metrics ONLY from "
              f"http://{OTEL_COLLECTOR_HOST}:{OTEL_COLLECTOR_METRICS_PORT}/metrics -- "
              f"no local services, this app emits nothing.", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
