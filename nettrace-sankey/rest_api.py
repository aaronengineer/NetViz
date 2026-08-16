"""
rest_api
--------
Routes /api/endpoints[/{id}] and /api/applications[/{id}] (GET/POST/PUT/
DELETE) to inventory_store. Stdlib-only -- no knowledge of SNMP/SSH/
pysnmp/paramiko at all, and no direct import of app.py (to avoid a
circular import); demo-mode live add/remove of applications is wired
through set_application_hooks() instead, called by app.py at startup.

Auth: if NETTRACE_API_TOKEN is set in the environment, mutating verbs
(POST/PUT/DELETE) require a matching `X-NetTrace-Token` header. GET stays
open either way, matching the rest of this prototype's already-open
visualization -- see README.md's Security section for the reasoning.
This is a basic deterrent, not production auth.
"""
import json
import os

import inventory_store
from netcommon import send_json_response

_on_application_create = None
_on_application_delete = None


def set_application_hooks(on_create, on_delete):
    """Registered by app.py at startup. Both must be async callables --
    they're awaited here. In demo mode, on_create/on_delete start/stop a
    real local simulated service listener so a POST/DELETE against
    /api/applications takes live effect instead of only being inventory
    bookkeeping. In otel mode there's nothing local to start, so app.py
    doesn't register hooks there at all (left as None -- no-op)."""
    global _on_application_create, _on_application_delete
    _on_application_create = on_create
    _on_application_delete = on_delete


def _authorized(method: str, headers: dict) -> bool:
    token = os.environ.get("NETTRACE_API_TOKEN", "").strip()
    if not token:
        return True
    if method == "GET":
        return True
    return headers.get("x-nettrace-token", "") == token


def _parse_path(path: str):
    """'/api/endpoints/1.2.3.4' -> ('endpoints', '1.2.3.4'); '/api/endpoints' -> ('endpoints', None)."""
    parts = [p for p in path.split("/") if p]  # drop leading/trailing empties
    # parts[0] == "api"
    resource = parts[1] if len(parts) >= 2 else None
    item_id = "/".join(parts[2:]) if len(parts) >= 3 else None
    return resource, (item_id or None)


async def handle_api_request(method: str, path: str, headers: dict, body: bytes, writer):
    if not _authorized(method, headers):
        send_json_response(writer, 401, {"error": "missing or invalid X-NetTrace-Token"})
        return

    resource, item_id = _parse_path(path)
    if resource == "endpoints":
        await _handle_endpoints(method, item_id, body, writer)
    elif resource == "applications":
        await _handle_applications(method, item_id, body, writer)
    else:
        send_json_response(writer, 404, {"error": f"unknown resource {resource!r}"})


def _parse_body(body: bytes):
    if not body:
        return {}
    try:
        return json.loads(body.decode())
    except (ValueError, UnicodeDecodeError):
        return None


# ---------------------------------------------------------------------------
# /api/endpoints
# ---------------------------------------------------------------------------
async def _handle_endpoints(method, item_id, body, writer):
    if method == "GET":
        if item_id is None:
            send_json_response(writer, 200, {"endpoints": inventory_store.list_endpoints()})
            return
        rec = inventory_store.get_endpoint(item_id)
        if rec is None:
            send_json_response(writer, 404, {"error": f"no endpoint {item_id!r}"})
            return
        send_json_response(writer, 200, rec)
        return

    if method in ("POST", "PUT"):
        data = _parse_body(body)
        if data is None:
            send_json_response(writer, 400, {"error": "invalid JSON body"})
            return
        rec_id = item_id or data.get("id") or data.get("ip")
        if not rec_id or "ip" not in data:
            send_json_response(writer, 400, {"error": "endpoint requires at least 'ip' (and 'id' if not derivable from it)"})
            return
        data["id"] = rec_id
        data.setdefault("source", "manual")
        data.setdefault("probe_port", 22)
        saved = await inventory_store.upsert_endpoint(data, overwrite_manual=True)
        send_json_response(writer, 201 if method == "POST" else 200, saved)
        return

    if method == "DELETE":
        if not item_id:
            send_json_response(writer, 400, {"error": "DELETE requires an endpoint id in the path"})
            return
        deleted = await inventory_store.delete_endpoint(item_id)
        if not deleted:
            send_json_response(writer, 404, {"error": f"no endpoint {item_id!r}"})
            return
        send_json_response(writer, 204, {})
        return

    send_json_response(writer, 405, {"error": f"method {method} not allowed on /api/endpoints"})


# ---------------------------------------------------------------------------
# /api/applications
# ---------------------------------------------------------------------------
async def _handle_applications(method, item_id, body, writer):
    if method == "GET":
        if item_id is None:
            send_json_response(writer, 200, {"applications": inventory_store.list_applications()})
            return
        rec = inventory_store.get_application(item_id)
        if rec is None:
            send_json_response(writer, 404, {"error": f"no application {item_id!r}"})
            return
        send_json_response(writer, 200, rec)
        return

    if method in ("POST", "PUT"):
        data = _parse_body(body)
        if data is None:
            send_json_response(writer, 400, {"error": "invalid JSON body"})
            return
        app_id = item_id or data.get("id")
        if not app_id or "port" not in data:
            send_json_response(writer, 400, {"error": "application requires 'id' and 'port'"})
            return
        data["id"] = app_id
        data.setdefault("label", app_id)
        data.setdefault("own_delay_ms", 0)
        data.setdefault("calls", [])
        is_new = inventory_store.get_application(app_id) is None
        saved = await inventory_store.upsert_application(data)
        if is_new and _on_application_create:
            await _on_application_create(saved)
        send_json_response(writer, 201 if method == "POST" else 200, saved)
        return

    if method == "DELETE":
        if not item_id:
            send_json_response(writer, 400, {"error": "DELETE requires an application id in the path"})
            return
        deleted = await inventory_store.delete_application(item_id)
        if not deleted:
            send_json_response(writer, 404, {"error": f"no application {item_id!r}"})
            return
        if _on_application_delete:
            await _on_application_delete(item_id)
        send_json_response(writer, 204, {})
        return

    send_json_response(writer, 405, {"error": f"method {method} not allowed on /api/applications"})
