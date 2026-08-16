"""
discovery
---------
Bounded, seeded autodiscovery: starting from configured seed devices,
walk outward via LLDP -> CDP -> ARP (SNMP) -> SSH CLI fallback, up to a
configured hop limit, restricted to an explicit CIDR allowlist. Only
imported when NETTRACE_DISCOVERY_ENABLED=true (see app.py's main()) -- a
plain demo/otel run never touches this module, snmp_client, or
ssh_client.

Config lives in discovery.config.json (seeds/max_hops/allowed_cidrs/
poll_interval_s) -- a file, not the API, per this feature's design:
discovery targets real infrastructure and shouldn't be reachable/
reconfigurable through the same REST surface used for everyday inventory
edits. Credentials (SNMP community, SSH user/pass) live separately in
discovery.credentials.json / env vars -- see discovery.credentials.json.example
-- and are never read by inventory_store or rest_api.

Discovered endpoints/edges are written into inventory_store tagged
source="discovered"; a record already tagged source="manual" is never
overwritten (overwrite_manual=False on every write here). netprobe.py's
own reconcile loop picks up newly discovered endpoints on its next tick
(within BROADCAST_INTERVAL, ~1s) -- no direct coupling needed here.
"""
import asyncio
import ipaddress
import json
import os

import inventory_store
import snmp_client
import ssh_client

DEFAULT_CONFIG_PATH = "discovery.config.json"
DEFAULT_CREDENTIALS_PATH = "discovery.credentials.json"

_warned = set()


def _warn_once(key, msg):
    if key not in _warned:
        _warned.add(key)
        print(f"[discovery] {msg}", flush=True)


def load_config(path: str = None):
    path = path or os.environ.get("NETTRACE_DISCOVERY_CONFIG", DEFAULT_CONFIG_PATH)
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"discovery config not found at {path!r} -- copy discovery.config.json.example "
            f"to {DEFAULT_CONFIG_PATH} and edit it (seeds + allowed_cidrs are required)")
    with open(path) as f:
        cfg = json.load(f)
    cfg.setdefault("seeds", [])
    cfg.setdefault("max_hops", 2)
    cfg.setdefault("allowed_cidrs", [])
    cfg.setdefault("poll_interval_s", 300)
    cfg.setdefault("per_device_delay_s", 0.3)
    return cfg


def load_credentials(path: str = None):
    path = path or os.environ.get("NETTRACE_DISCOVERY_CREDENTIALS", DEFAULT_CREDENTIALS_PATH)
    creds = {"default": {}, "by_cidr": []}
    if os.path.exists(path):
        with open(path) as f:
            creds.update(json.load(f))
    creds["default"].setdefault("snmp_community", os.environ.get("NETTRACE_SNMP_COMMUNITY", "public"))
    creds["default"].setdefault("ssh_username", os.environ.get("NETTRACE_SSH_USER"))
    creds["default"].setdefault("ssh_password", os.environ.get("NETTRACE_SSH_PASS"))
    return creds


def credentials_for(ip: str, creds: dict) -> dict:
    """Per-CIDR credential overrides take precedence over the default --
    a real network commonly has different community strings/SSH creds per
    subnet or per vendor."""
    try:
        ip_obj = ipaddress.ip_address(ip)
    except ValueError:
        return creds["default"]
    for entry in creds.get("by_cidr", []):
        try:
            if ip_obj in ipaddress.ip_network(entry["cidr"]):
                return {**creds["default"], **entry}
        except ValueError:
            continue
    return creds["default"]


def is_allowed(ip: str, allowed_cidrs: list) -> bool:
    """Fail closed: an empty allowlist means nothing is allowed, not
    everything -- discovery has to be explicitly opted into a range."""
    if not allowed_cidrs:
        return False
    try:
        ip_obj = ipaddress.ip_address(ip)
    except ValueError:
        return False
    for cidr in allowed_cidrs:
        try:
            if ip_obj in ipaddress.ip_network(cidr):
                return True
        except ValueError:
            continue
    return False


async def _neighbors_for(ip: str, creds: dict):
    dev_creds = credentials_for(ip, creds)
    community = dev_creds.get("snmp_community") or "public"

    for fn in (snmp_client.get_lldp_neighbors, snmp_client.get_cdp_neighbors, snmp_client.get_arp_table):
        neighbors = await fn(ip, community)
        if neighbors:
            return neighbors

    username, password = dev_creds.get("ssh_username"), dev_creds.get("ssh_password")
    if username and password:
        return await ssh_client.get_neighbors_via_ssh(ip, username, password)
    return []


async def crawl_once(cfg: dict, creds: dict):
    """One full bounded BFS pass from cfg['seeds']."""
    allowed_cidrs = cfg["allowed_cidrs"]
    max_hops = cfg["max_hops"]
    delay = cfg["per_device_delay_s"]

    seeds = [s for s in cfg["seeds"] if is_allowed(s, allowed_cidrs)]
    for s in cfg["seeds"]:
        if s not in seeds:
            _warn_once(f"seed-rejected:{s}", f"seed {s!r} is outside allowed_cidrs -- ignored")

    visited = set()
    queue = [(s, 0) for s in seeds]
    for s in seeds:
        await inventory_store.upsert_endpoint(
            {"id": s, "ip": s, "source": "discovered", "discovery_method": None},
            overwrite_manual=False)

    while queue:
        ip, depth = queue.pop(0)
        if ip in visited:
            continue
        visited.add(ip)

        neighbors = await _neighbors_for(ip, creds)
        next_depth = depth + 1
        for n in neighbors:
            n_ip = n["neighbor_ip"]
            if not is_allowed(n_ip, allowed_cidrs):
                _warn_once(f"dropped:{n_ip}", f"{n_ip} (neighbor of {ip} via {n['method']}) is outside allowed_cidrs -- dropped")
                continue
            if next_depth > max_hops:
                continue  # beyond the configured hop limit -- not recorded at all, not just not crawled further
            await inventory_store.upsert_endpoint(
                {"id": n_ip, "ip": n_ip, "hostname": n.get("neighbor_hostname"),
                 "source": "discovered", "discovery_method": n["method"]},
                overwrite_manual=False)
            await inventory_store.upsert_edge(ip, n_ip, n["method"])
            if n_ip not in visited:
                queue.append((n_ip, next_depth))

        await asyncio.sleep(delay)  # rate-limit: don't hammer a large network


async def discovery_loop():
    """Long-running task started from app.py's main() only when
    NETTRACE_DISCOVERY_ENABLED=true. Runs crawl_once() immediately, then
    on cfg['poll_interval_s'] cadence, forever. A crawl failure (bad
    config, every seed unreachable, etc.) is logged and retried next
    interval rather than crashing the whole app."""
    try:
        cfg = load_config()
        creds = load_credentials()
    except FileNotFoundError as exc:
        # Missing config is a setup step, not a crash -- log once, cleanly,
        # and let this task end; the rest of the app (demo/otel viz, REST
        # API, network-latency probing) is unaffected either way.
        print(f"[discovery] disabled: {exc}", flush=True)
        return
    print(f"[discovery] enabled: seeds={cfg['seeds']} max_hops={cfg['max_hops']} "
          f"allowed_cidrs={cfg['allowed_cidrs']} poll_interval_s={cfg['poll_interval_s']}", flush=True)
    while True:
        try:
            await crawl_once(cfg, creds)
        except Exception as exc:
            print(f"[discovery] crawl_once failed: {exc!r}", flush=True)
        await asyncio.sleep(cfg["poll_interval_s"])
