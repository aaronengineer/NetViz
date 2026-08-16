"""
netprobe
--------
The network-device latency layer: real TCP-connect timing against real,
dynamic targets drawn from inventory_store's network_endpoints (populated
manually via /api/endpoints and/or by discovery.py). Adapted from
netviz-prototype/app.py's prober() (real time.perf_counter() timing around
a genuine asyncio.open_connection()) -- same honest "this is a real
measurement" approach, just pointed at real remote hosts instead of local
demo listeners, and against a target set that changes at runtime instead
of a hardcoded dict.

IMPORTANT HONESTY NOTE (matches this repo's existing "what's real vs.
simulated" transparency style): this measures latency from the nettrace
HOST to each discovered device -- a single vantage point -- not true
hop-by-hop latency between two arbitrary devices on the wire. The device
graph below is therefore a star: one edge per endpoint, from a synthetic
"nettrace-host" node to that endpoint, carrying real measured
avg_network_ms. LLDP/CDP/ARP-discovered device-to-device adjacency (which
DOES capture who's physically next to whom) is stored separately in
inventory_store's network_edges and exposed read-only via /api/endpoints
data, but is not itself timed -- there's no agent running on every
discovered device to measure device-to-device latency directly.

Stdlib-only. No knowledge of SNMP/SSH/pysnmp/paramiko.
"""
import asyncio
import time
from collections import deque

import inventory_store
from netcommon import edge_key, severity

OBSERVER_ID = "nettrace-host"
WINDOW_SECONDS = 5.0
PROBE_INTERVAL_S = 2.0
PROBE_TIMEOUT_S = 3.0
BROADCAST_INTERVAL = 1.0

_samples = {}   # endpoint_id -> deque[(ts, network_ms or None)]
_tasks = {}     # endpoint_id -> asyncio.Task
_known_node_ids = set()


async def _probe_loop(endpoint_id: str, ip: str, port: int):
    buf = _samples.setdefault(endpoint_id, deque())
    while True:
        t0 = time.perf_counter()
        network_ms = None
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(ip, port), timeout=PROBE_TIMEOUT_S)
            network_ms = (time.perf_counter() - t0) * 1000
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
        except (OSError, asyncio.TimeoutError):
            pass  # unreachable this round -- recorded as None, not raised
        buf.append((time.time(), network_ms))
        await asyncio.sleep(PROBE_INTERVAL_S)


def reconcile():
    """Diff running probe tasks against inventory_store's current endpoint
    set: start a prober for each new endpoint, cancel one for each removed
    endpoint. Called periodically by prober_manager() and right after any
    /api/endpoints mutation. Returns the current set of endpoint ids."""
    current = {e["id"]: e for e in inventory_store.list_endpoints()}

    for endpoint_id, rec in current.items():
        task = _tasks.get(endpoint_id)
        if task is None or task.done():
            port = rec.get("probe_port", 22)
            _tasks[endpoint_id] = asyncio.create_task(_probe_loop(endpoint_id, rec["ip"], port))

    for endpoint_id in list(_tasks):
        if endpoint_id not in current:
            _tasks.pop(endpoint_id).cancel()
            _samples.pop(endpoint_id, None)

    return set(current.keys())


def build_device_graph_snapshot():
    now = time.time()
    endpoints = {e["id"]: e for e in inventory_store.list_endpoints()}
    edges = []
    for endpoint_id, buf in list(_samples.items()):
        while buf and now - buf[0][0] > WINDOW_SECONDS:
            buf.popleft()
        if not buf or endpoint_id not in endpoints:
            continue
        ok_samples = [v for (_, v) in buf if v is not None]
        avg_network = sum(ok_samples) / len(ok_samples) if ok_samples else None
        edges.append({
            "source": OBSERVER_ID,
            "target": endpoint_id,
            "count": len(buf),
            "rate_per_sec": round(len(buf) / WINDOW_SECONDS, 2),
            "avg_network_ms": round(avg_network, 1) if avg_network is not None else None,
            "severity_network": severity(avg_network),
            "reachable": avg_network is not None,
        })
    return {"type": "device_graph", "ts": now, "edges": edges}


def device_topology_message():
    endpoints = inventory_store.list_endpoints()
    nodes = [{"id": OBSERVER_ID, "label": "nettrace host (observer)"}]
    for e in endpoints:
        nodes.append({"id": e["id"], "label": e.get("hostname") or e["ip"], "device_type": e.get("device_type")})
    edges = [{"source": OBSERVER_ID, "target": e["id"]} for e in endpoints]
    return {"type": "device_topology", "nodes": nodes, "edges": edges,
            "adjacency": [{"source": a["source"], "target": a["target"], "discovery_method": a["discovery_method"]}
                          for a in inventory_store.list_edges()]}


async def prober_manager(broadcast_fn):
    """Long-running task (started once from app.py's main()). Reconciles
    the probe task set against the inventory on a fixed cadence, detects
    structural changes (endpoint added/removed) to know when to re-send
    device_topology, and broadcasts device_graph every tick regardless --
    same shape as app.py's own aggregator()/otel_only_broadcaster()."""
    global _known_node_ids
    while True:
        await asyncio.sleep(BROADCAST_INTERVAL)
        current_ids = reconcile()
        if current_ids != _known_node_ids:
            _known_node_ids = current_ids
            await broadcast_fn(device_topology_message())
        await broadcast_fn(build_device_graph_snapshot())
