"""
netcommon
---------
Small stdlib-only helpers shared across nettrace-sankey's modules
(app.py, inventory_store.py, rest_api.py, netprobe.py). Kept separate so
these modules don't have to import from each other in a circle, and so
the discovery-only modules (which DO pull in third-party deps) never need
to import app.py directly.

severity()/edge_key() below are moved out of app.py unchanged -- same
thresholds as sankey.html's THRESH/SEVERITY_COLOR (kept in sync by hand,
same as the existing JS/Python duplication that file's comments already
call out).
"""
import json
import os


def edge_key(source, target):
    return f"{source}->{target}"


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


def atomic_write_json(path, obj):
    """Write JSON to `path` atomically: write to a sibling .tmp file, then
    os.replace() over the original. Avoids a reader ever seeing a partially
    written file if the process is killed mid-write."""
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(obj, f, indent=2)
        f.write("\n")
    os.replace(tmp_path, path)


def send_json_response(writer, status: int, obj, extra_headers: dict = None):
    """Write a full HTTP response with a JSON body, in the same manual,
    hand-rolled style as app.py's serve_static()/handle_connection()."""
    body = json.dumps(obj).encode()
    status_text = {200: "OK", 201: "Created", 204: "No Content", 400: "Bad Request",
                   401: "Unauthorized", 404: "Not Found", 405: "Method Not Allowed",
                   500: "Internal Server Error"}.get(status, "OK")
    header_lines = [
        f"HTTP/1.1 {status} {status_text}",
        "Content-Type: application/json; charset=utf-8",
        f"Content-Length: {len(body)}",
        "Connection: close",
    ]
    for k, v in (extra_headers or {}).items():
        header_lines.append(f"{k}: {v}")
    writer.write(("\r\n".join(header_lines) + "\r\n\r\n").encode() + body)
