"""
snmp_client
-----------
Thin, normalized wrapper around pysnmp for the three tables discovery.py
needs: LLDP-MIB's lldpRemTable, Cisco's CDP-MIB cdpCacheTable, and the
standard MIB-II ipNetToMediaTable (ARP). Only imported when
NETTRACE_DISCOVERY_ENABLED=true (see app.py's main()) -- a plain demo/otel
run never touches this module or pysnmp.

Uses pysnmp's asyncio-native v3arch API (pysnmp>=6), so these coroutines
run directly on the same event loop as the rest of the app -- no
run_in_executor needed here (unlike ssh_client.py, which wraps paramiko's
blocking API).

Every function returns a normalized list of
{"neighbor_ip", "neighbor_hostname", "local_port", "method"} dicts (ARP
entries have neighbor_hostname=None -- ARP has no hostname, just an IP/MAC
pairing) so discovery.py doesn't need to know MIB-specific shapes. Errors
(timeout, unreachable, no such object) are caught and result in an empty
list, never raised -- matches this repo's existing "best-effort, log once"
philosophy (see app.py's _otel_warned pattern).
"""
import time

from pysnmp.hlapi.v3arch.asyncio import (
    CommunityData, ContextData, ObjectIdentity, ObjectType,
    SnmpEngine, UdpTransportTarget, walk_cmd,
)

SNMP_TIMEOUT_S = 3.0
SNMP_RETRIES = 1

# LLDP-MIB::lldpRemTable columns we need (indexed by
# lldpRemTimeMark.lldpRemLocalPortNum.lldpRemIndex):
LLDP_REM_SYS_NAME = "1.0.8802.1.1.2.1.4.1.1.9"     # lldpRemSysName
LLDP_REM_MGMT_ADDR = "1.0.8802.1.1.2.1.4.2.1.4"    # lldpRemManAddrIfSubtype-indexed table; address is part of the index

# Cisco CDP-MIB::cdpCacheTable
CDP_CACHE_ADDRESS = "1.3.6.1.4.1.9.9.23.1.2.1.1.4"       # cdpCacheAddress (raw, usually 4-byte IPv4)
CDP_CACHE_DEVICE_ID = "1.3.6.1.4.1.9.9.23.1.2.1.1.6"     # cdpCacheDeviceId

# RFC 1213 MIB-II ipNetToMediaTable (ARP)
IP_NET_TO_MEDIA_NET_ADDRESS = "1.3.6.1.2.1.4.22.1.3"  # ipNetToMediaNetAddress

_warned_devices = set()


def _warn_once(ip, msg):
    key = (ip, msg)
    if key not in _warned_devices:
        _warned_devices.add(key)
        print(f"[snmp] {ip}: {msg}", flush=True)


async def _walk(ip: str, community: str, base_oid: str, port: int = 161):
    """Yield (oid_str, value) pairs for everything under base_oid, or
    nothing at all on any error (timeout, unreachable, bad community)."""
    engine = SnmpEngine()
    try:
        target = await UdpTransportTarget.create(
            (ip, port), timeout=SNMP_TIMEOUT_S, retries=SNMP_RETRIES)
    except Exception as exc:
        _warn_once(ip, f"couldn't create SNMP transport ({exc!r})")
        return

    try:
        async for error_indication, error_status, error_index, var_binds in walk_cmd(
            engine, CommunityData(community, mpModel=1), target, ContextData(),
            ObjectType(ObjectIdentity(base_oid)),
        ):
            if error_indication:
                _warn_once(ip, f"SNMP walk of {base_oid} failed: {error_indication}")
                return
            if error_status:
                _warn_once(ip, f"SNMP walk of {base_oid} error status: {error_status.prettyPrint()}")
                return
            for name, value in var_binds:
                oid_str = str(name)
                if not oid_str.startswith(base_oid):
                    return  # walked past the subtree we asked for
                yield oid_str, value
    except Exception as exc:
        _warn_once(ip, f"SNMP walk of {base_oid} raised {exc!r}")
        return


def _decode_ip_octets(value) -> str:
    """pysnmp OctetString values for IPv4-shaped data (raw 4-byte
    addresses, as used by CDP's cdpCacheAddress and similar) come back as
    bytes; render as dotted-quad."""
    raw = bytes(value)
    if len(raw) == 4:
        return ".".join(str(b) for b in raw)
    return raw.hex()


async def get_arp_table(ip: str, community: str, port: int = 161):
    """ipNetToMediaTable is indexed by ifIndex.ipAddress -- the ARP entry's
    IP is the trailing 4 sub-identifiers of the OID itself, not the value
    (ipNetToMediaNetAddress's *value* duplicates it, which is what we walk
    for simplicity: one column, IP already decoded by pysnmp as an
    IpAddress type)."""
    out = []
    async for oid_str, value in _walk(ip, community, IP_NET_TO_MEDIA_NET_ADDRESS, port):
        neighbor_ip = value.prettyPrint()
        if neighbor_ip in ("0.0.0.0", "255.255.255.255"):
            continue
        out.append({"neighbor_ip": neighbor_ip, "neighbor_hostname": None,
                     "local_port": None, "method": "arp"})
    return out


async def get_lldp_neighbors(ip: str, community: str, port: int = 161):
    """lldpRemManAddrTable's index encodes the neighbor's management
    address as the trailing OID sub-identifiers (after subtype+length);
    lldpRemSysName gives the hostname, matched by the shared
    lldpRemLocalPortNum.lldpRemIndex prefix of the index."""
    sys_names = {}  # "<timemark>.<localport>.<index>" -> hostname
    async for oid_str, value in _walk(ip, community, LLDP_REM_SYS_NAME, port):
        key = oid_str[len(LLDP_REM_SYS_NAME) + 1:]
        sys_names[key] = value.prettyPrint()

    out = []
    async for oid_str, value in _walk(ip, community, LLDP_REM_MGMT_ADDR, port):
        # Index shape: <timemark>.<localport>.<index>.<addrSubtype>.<addrLen>.<addr...>
        rest = oid_str[len(LLDP_REM_MGMT_ADDR) + 1:].split(".")
        if len(rest) < 6:
            continue
        timemark, local_port, lldp_index, addr_subtype, addr_len = rest[:5]
        addr_octets = rest[5:5 + int(addr_len)]
        if addr_subtype != "1" or int(addr_len) != 4:  # subtype 1 = IPv4
            continue
        neighbor_ip = ".".join(addr_octets)
        key = f"{timemark}.{local_port}.{lldp_index}"
        out.append({"neighbor_ip": neighbor_ip, "neighbor_hostname": sys_names.get(key),
                     "local_port": local_port, "method": "lldp"})
    return out


async def get_cdp_neighbors(ip: str, community: str, port: int = 161):
    device_ids = {}  # "<ifIndex>.<cacheIndex>" -> device id string
    async for oid_str, value in _walk(ip, community, CDP_CACHE_DEVICE_ID, port):
        key = oid_str[len(CDP_CACHE_DEVICE_ID) + 1:]
        device_ids[key] = value.prettyPrint()

    out = []
    async for oid_str, value in _walk(ip, community, CDP_CACHE_ADDRESS, port):
        key = oid_str[len(CDP_CACHE_ADDRESS) + 1:]
        local_port = key.split(".")[0] if "." in key else key
        try:
            neighbor_ip = _decode_ip_octets(value)
        except Exception:
            continue
        out.append({"neighbor_ip": neighbor_ip, "neighbor_hostname": device_ids.get(key),
                     "local_port": local_port, "method": "cdp"})
    return out
