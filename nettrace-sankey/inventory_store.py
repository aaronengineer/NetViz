"""
inventory_store
----------------
Pure data layer: the persistent inventory of network endpoints, discovered
adjacency edges, and applications. No networking, no HTTP, no SNMP/SSH --
that separation is what keeps this module stdlib-only and independently
testable (see nettrace-sankey/README.md's "Network layer & inventory API"
section for how this fits into the rest of the app).

Backed by a single JSON file (default data/inventory.json), read fully into
memory on init() and written back atomically (netcommon.atomic_write_json)
on every mutation, guarded by an asyncio.Lock so concurrent API writes and
a discovery crawl's writes can't interleave and corrupt the file.

`applications` is seeded once, on first run (empty store), from
topology.demo.json -- after that the store is authoritative and
topology.demo.json is left untouched, still checked into git as the
shipped example/reset point.

Credentials are NEVER stored here -- see discovery.credentials.json.example
and README.md's Security section.
"""
import asyncio
import json
import os
import time

from netcommon import atomic_write_json, edge_key

SCHEMA_VERSION = 1

_data = None
_lock = asyncio.Lock()
_data_path = None


def _empty_store():
    return {"_meta": {"version": SCHEMA_VERSION}, "network_endpoints": {}, "network_edges": {}, "applications": {}}


def _seed_applications_from_topology(topology_file: str):
    apps = {}
    if not os.path.exists(topology_file):
        return apps
    with open(topology_file) as f:
        cfg = json.load(f)
    for sid, svc in cfg.get("services", {}).items():
        apps[sid] = {
            "id": sid,
            "label": svc.get("label", sid),
            "port": svc["port"],
            "own_delay_ms": svc.get("own_delay_ms", 0),
            "calls": svc.get("calls", []),
            "source": "seed",
        }
    return apps


def init(data_dir: str = "data", topology_file: str = "topology.demo.json"):
    """Load data/inventory.json, creating it (and seeding `applications`
    from topology_file) if it doesn't exist yet. Synchronous and called
    once, before any concurrent tasks that touch the store are started."""
    global _data, _data_path
    _data_path = os.path.join(data_dir, "inventory.json")
    if os.path.exists(_data_path):
        with open(_data_path) as f:
            _data = json.load(f)
        _data.setdefault("network_endpoints", {})
        _data.setdefault("network_edges", {})
        _data.setdefault("applications", {})
        return
    _data = _empty_store()
    _data["applications"] = _seed_applications_from_topology(topology_file)
    atomic_write_json(_data_path, _data)


def _save_locked():
    atomic_write_json(_data_path, _data)


# ---------------------------------------------------------------------------
# network_endpoints
# ---------------------------------------------------------------------------
def list_endpoints():
    return list(_data["network_endpoints"].values())


def get_endpoint(endpoint_id: str):
    return _data["network_endpoints"].get(endpoint_id)


async def upsert_endpoint(rec: dict, overwrite_manual: bool = True):
    """Create or update a network endpoint. `rec` must include `id`.
    If overwrite_manual is False, a call that would touch an existing
    record whose source is "manual" is a no-op (used by discovery, which
    must never clobber a manually-entered endpoint) -- returns the
    existing record unchanged in that case."""
    endpoint_id = rec["id"]
    async with _lock:
        existing = _data["network_endpoints"].get(endpoint_id)
        if existing and existing.get("source") == "manual" and not overwrite_manual:
            return existing
        now = time.time()
        merged = {**(existing or {}), **rec}
        merged.setdefault("first_seen", now)
        merged["last_seen"] = now
        _data["network_endpoints"][endpoint_id] = merged
        _save_locked()
        return merged


async def delete_endpoint(endpoint_id: str) -> bool:
    async with _lock:
        existed = _data["network_endpoints"].pop(endpoint_id, None) is not None
        if existed:
            stale = [k for k, e in _data["network_edges"].items()
                     if e["source"] == endpoint_id or e["target"] == endpoint_id]
            for k in stale:
                _data["network_edges"].pop(k, None)
            _save_locked()
        return existed


# ---------------------------------------------------------------------------
# network_edges (discovered adjacency)
# ---------------------------------------------------------------------------
def list_edges():
    return list(_data["network_edges"].values())


async def upsert_edge(source: str, target: str, discovery_method: str):
    key = edge_key(source, target)
    async with _lock:
        _data["network_edges"][key] = {
            "source": source, "target": target,
            "discovery_method": discovery_method, "last_seen": time.time(),
        }
        _save_locked()
        return _data["network_edges"][key]


# ---------------------------------------------------------------------------
# applications
# ---------------------------------------------------------------------------
def list_applications():
    return list(_data["applications"].values())


def get_application(app_id: str):
    return _data["applications"].get(app_id)


async def upsert_application(rec: dict):
    app_id = rec["id"]
    async with _lock:
        existing = _data["applications"].get(app_id)
        merged = {**(existing or {}), **rec}
        merged.setdefault("source", "api")
        _data["applications"][app_id] = merged
        _save_locked()
        return merged


async def delete_application(app_id: str) -> bool:
    async with _lock:
        existed = _data["applications"].pop(app_id, None) is not None
        if existed:
            _save_locked()
        return existed
