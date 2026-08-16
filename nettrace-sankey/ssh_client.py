"""
ssh_client
----------
SSH CLI fallback for devices without usable SNMP (see discovery.py's
LLDP -> CDP -> ARP -> SSH fallback chain). Only imported when
NETTRACE_DISCOVERY_ENABLED=true -- a plain demo/otel run never touches
this module or paramiko.

paramiko is a BLOCKING library (its own socket I/O, not asyncio-aware), so
every call in here is wrapped in loop.run_in_executor() -- this is the
single most important integration detail: without it, one slow or
unreachable SSH device would stall the entire asyncio event loop and every
other probe/broadcast task riding on it.

Command syntax genuinely varies by vendor and there's no way to know a
given device's exact dialect in advance, so this tries a short list of
common commands per vendor family and regex-parses whichever one succeeds
-- best-effort, same spirit as snmp_client.py's defensive OID walking.
UNVERIFIED AGAINST REAL HARDWARE (no devices available in this environment
-- see README.md's Security/Verification notes): the command list and
regexes are written from documented CLI output formats and should be
spot-checked against your actual devices.

Host key handling: AutoAddPolicy (accept-and-trust-on-first-use, no
interactive prompt) is used deliberately -- this runs unattended against
internal devices on a network you already control, and paramiko has no
non-interactive "prompt the user" option. This is a real MITM-on-first-
connection tradeoff for the convenience of a fully automated crawl; if
that's not acceptable for your environment, don't enable SSH discovery.
"""
import asyncio
import re

import paramiko

SSH_TIMEOUT_S = 5.0

# (command, parser) pairs, tried in order until one returns any neighbors.
# LLDP/CDP are tried before ARP because they carry real adjacency
# (who's physically next to whom); ARP only tells us "this IP was seen",
# with no adjacency information at all.
_COMMANDS = [
    ("show lldp neighbors detail", "lldp"),
    ("show cdp neighbors detail", "cdp"),
    ("show ip arp", "arp"),
    ("show arp", "arp"),
    ("ip neigh", "arp"),
]

_IPV4_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")


def _parse_lldp_detail(text: str):
    out = []
    for block in re.split(r"\n(?=-{2,}|Local Intf:)", text):
        sys_name = re.search(r"System Name:\s*(\S+)", block)
        mgmt_addr = re.search(r"Management Address:\s*(" + _IPV4_RE.pattern + r")", block)
        local_port = re.search(r"Local Intf:\s*(\S+)", block)
        if mgmt_addr:
            out.append({"neighbor_ip": mgmt_addr.group(1),
                        "neighbor_hostname": sys_name.group(1) if sys_name else None,
                        "local_port": local_port.group(1) if local_port else None, "method": "lldp"})
    return out


def _parse_cdp_detail(text: str):
    out = []
    for block in re.split(r"\n(?=-{2,}|Device ID:)", text):
        device_id = re.search(r"Device ID:\s*(\S+)", block)
        ip_addr = re.search(r"IP address:\s*(" + _IPV4_RE.pattern + r")", block)
        local_port = re.search(r"Interface:\s*([\w/.]+)", block)
        if ip_addr:
            out.append({"neighbor_ip": ip_addr.group(1),
                        "neighbor_hostname": device_id.group(1) if device_id else None,
                        "local_port": local_port.group(1) if local_port else None, "method": "cdp"})
    return out


def _parse_arp_generic(text: str):
    """No single ARP output format across vendors/OSes -- fall back to
    pulling every IPv4-looking token out of each non-empty line. No
    hostname or port info available from ARP alone."""
    out = []
    seen = set()
    for line in text.splitlines():
        for ip in _IPV4_RE.findall(line):
            if ip in seen or ip in ("0.0.0.0", "255.255.255.255"):
                continue
            seen.add(ip)
            out.append({"neighbor_ip": ip, "neighbor_hostname": None, "local_port": None, "method": "arp"})
    return out


_PARSERS = {"lldp": _parse_lldp_detail, "cdp": _parse_cdp_detail, "arp": _parse_arp_generic}


def _run_blocking(ip: str, username: str, password: str, port: int):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(ip, port=port, username=username, password=password,
                        timeout=SSH_TIMEOUT_S, banner_timeout=SSH_TIMEOUT_S,
                        auth_timeout=SSH_TIMEOUT_S, allow_agent=False, look_for_keys=False)
    except Exception as exc:
        print(f"[ssh] {ip}: connect failed ({exc!r})", flush=True)
        return []

    try:
        for command, method in _COMMANDS:
            try:
                _stdin, stdout, stderr = client.exec_command(command, timeout=SSH_TIMEOUT_S)
                out_text = stdout.read().decode(errors="replace")
                err_text = stderr.read().decode(errors="replace")
            except Exception:
                continue
            if not out_text.strip() or "% Invalid" in out_text or "command not found" in err_text:
                continue
            neighbors = _PARSERS[method](out_text)
            if neighbors:
                return neighbors
        return []
    finally:
        client.close()


async def get_neighbors_via_ssh(ip: str, username: str, password: str, port: int = 22):
    """Runs the blocking paramiko session in a thread executor so it can
    never stall the event loop (see module docstring)."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _run_blocking, ip, username, password, port)
