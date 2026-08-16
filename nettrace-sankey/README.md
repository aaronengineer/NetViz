# nettrace-sankey

The application-trace follow-up to `netviz-prototype`: instead of one fixed
TCP topology probed from a single point, this renders a live application
call graph -- who calls whom, how often, and how slow each hop is -- as a
Sankey-style flow diagram, with real OpenTelemetry underneath it.

It runs in one of two modes:

- **`demo`** (default) -- a small built-in simulated "webshop" (gateway ->
  auth / catalog / orders -> payments) with continuous synthetic traffic,
  so there's always something to look at with zero setup.
- **`otel`** -- no simulation at all. Point it at a real OpenTelemetry
  Collector that's already watching your own real, independently
  instrumented services, and it renders whatever topology and latency the
  Collector reports, live. This is the "deploy it against a real
  environment" mode -- see [Deploying against real services](#deploying-against-real-services)
  below.

New here? [CONTRIBUTING.md](CONTRIBUTING.md) has the fastest path to
running each mode and a short code tour.

On top of the application call graph above, there's also an independent
**network layer** -- real devices, real TCP-connect latency, optional
LLDP/CDP/ARP autodiscovery, and a REST API for managing both layers'
inventory. See [Network layer, autodiscovery & inventory API](#network-layer-autodiscovery--inventory-api)
below.

## Reading the visualization (legend)

The app itself has a collapsible **Legend** panel (bottom-right corner)
that explains this live and can't drift out of sync with the code, since
it's generated from the same color/threshold constants the rendering uses.
The short version:

- **Link (tube) color** = that hop's average network transit time:
  clean (blue, under 50ms) -> light delay/smoke (yellow, 50-150ms) ->
  heavy delay (orange, 150-350ms) -> fire (red, 350ms+). These are the
  same thresholds and the same clean/smoke/fire idea as `netviz-prototype`.
- **Node color** = the average *server-side* handling time for requests
  arriving at that service, same color scale.
- **Tube width** = call volume -- how many requests crossed that link in
  the trailing 5-second window. Width and color are independent: a wide,
  blue tube is high traffic that's still fast; a thin, red tube is low
  traffic that's slow.
- **The moving dots inside each tube** ("bars" traveling through it) are
  individual calls in flight, one per real request. They're spawned at
  that link's actual measured rate -- a busier link emits dots faster --
  and each dot's color is a snapshot of that link's severity color at the
  moment it was spawned. They're an activity indicator, not a literal
  physics simulation: their travel time across the tube is a fixed
  ~1 second for readability, not the hop's real latency (the real latency
  is the number printed on the link and the link's color).
- **Node bar height** = that service's relative throughput (its busier
  side, incoming or outgoing) -- taller bar, busier service.
- **The `OTel ✓` badge** under an edge (demo mode only) is a live
  cross-check: the same latency numbers, independently re-derived by a
  real OpenTelemetry Collector from real spans, shown side by side with
  the app's own measurement to prove the two agree. In `otel` mode there's
  no separate badge because the whole graph already **is** that data.

## Topology (demo mode)

```
client -> gateway -> auth              (always)
                   -> catalog          (usually, 70%)
                   -> orders           (sometimes, 40%) -> payments (always, if orders ran)
```

This -- along with every service's simulated processing delay and each
call's probability -- is defined in [`topology.demo.json`](topology.demo.json),
not hardcoded in `app.py`. Edit that file to reshape the demo (add a
service, change fan-out, retune delays) without touching Python; see
"Tuning / extending" below for the schema. A load generator fires synthetic
requests into the configured entrypoint (`gateway` by default) continuously,
so there's always live traffic to watch.

## What's real vs. simulated (demo mode)

- **Every hop is a real TCP call and a real measurement.** Each service is
  an actual `asyncio` listener; a service's `server_ms` is its own genuine
  wall-clock handling time -- including time spent waiting on its own
  downstream calls, exactly like a parent span's duration naturally
  includes its children's durations in real distributed tracing. Try
  cranking `gateway`'s `own_delay_ms` slider and you'll see it cascade
  into every edge downstream of it, live.
- **`network_ms` per hop is simulated**, same as before: real connect time
  plus an adjustable synthetic delay, so you can dial in how bad a
  specific hop looks without needing a real WAN.
- **Traffic volume is synthetic** -- the load generator's request rate and
  branching probabilities, not real users.
- **Zero third-party dependencies**, same philosophy as `netviz-prototype`:
  the backend is plain `asyncio` with a hand-rolled WebSocket
  implementation, and the frontend is hand-rolled SVG with no charting
  library at all (deliberately -- see "Why no D3" below).

None of the above applies in `otel` mode -- there, every number on screen
comes straight from your Collector's real data. See below.

## Real OTel Collector integration

`app.py` emits genuine OTLP/HTTP spans for every hop: each service sends a
CLIENT span for the call it makes and a SERVER span for the call it
handles, with proper `trace_id`/`span_id`/`parent_span_id` linkage, straight
from the stdlib (`os.urandom` for IDs, a hand-rolled asyncio HTTP client for
the POST -- no `opentelemetry-*` packages, keeping the zero-dependency
philosophy). Point it at a real OpenTelemetry Collector running the
[service_graph connector](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/connector/servicegraphconnector/README.md)
(config in `otel-collector-config.yaml`) and it independently reconstructs
the same call graph, purely from the spans, with its own latency
histograms -- exported as Prometheus metrics that `app.py` scrapes and
parses back out.

In demo mode, the frontend shows this as a second, independent readout
next to each edge's normal numbers (`OTel ✓ 4/s · 12.2ms srv · 19.7ms
client`), plus a banner confirming whether the Collector is currently
reachable. If the Collector is down or misconfigured, that badge just says
so and everything else keeps working unaffected -- the in-band metrics
never depend on it.

**Why keep both instead of switching over fully (in demo mode):** the OTel
path depends on a second container staying up and configured correctly,
and pairing independently-reported spans into edges takes a few seconds
longer to "warm up" than the in-band measurement (which is exact and
instant since the callee just tells the caller directly). Keeping the
in-band metrics as the default and the OTel numbers as a verification
badge means you get to watch them agree -- which is the actual point: it
demonstrates that a real OTel pipeline reconstructs the same truth an app
could just report about itself, which is the whole value proposition of
distributed tracing. `otel` mode (below) is what it looks like to trust
that pipeline fully, once you do.

### Wiring it to `demo-net`

`docker-compose.yml` starts the app *and* an
`otel/opentelemetry-collector-contrib` container, both attached to the
external `demo-net` bridge network (the same one your other services --
convertx, flask-hub, linux-cli, perkan, watchtower -- run on), instead of
an isolated per-project network:

```
docker network create demo-net   # if it doesn't already exist
docker compose up --build -d
```

`nettrace` talks to the collector by its service name (`otel-collector`) on
the Docker network, so no ports need to be published between them. The
Collector's OTLP intake (4318) and Prometheus output (8889) are also
published to the host, in case you want to point other tools at either one
directly (`curl http://localhost:8889/metrics` to see the raw
service-graph metrics, for instance).

### What's been verified vs. what to check on your end

Everything on the `app.py` side of this -- OTLP span construction and
export, and the Prometheus scrape + parse-back path, in both modes -- was
tested against hand-built mocks of the Collector's endpoints (the
sandbox this was built in has no Docker daemon, so the real
`otel/opentelemetry-collector-contrib` binary couldn't be run there
directly). Demo mode's test pushed real traffic through all six services,
validated 254 spans with 0 structural errors (well-formed trace/span IDs,
valid span kinds, sane timestamps, correct parent/child linkage), and
confirmed the OTel-derived numbers matched the in-band ones closely (e.g.
in-band `server_ms` of 12.2 lined up exactly with the OTel-scraped
`avg_server_ms`). `otel` mode's test simulated an entirely different,
growing topology (a `frontend -> api -> db` / `api -> cache` graph, unrelated
to the demo's own services) purely via Prometheus scrapes, and confirmed
the app correctly discovered new services/edges live and derived matching
network/server timing splits.

What that testing *can't* confirm is the real Collector binary's exact
Prometheus output -- specifically, whether `service_graph`'s emitted
metric names carry a `_seconds` unit suffix or not, and the precise label
set, since that depends on the installed Collector version. `app.py`'s
parser (`aggregate_otel_metrics()`) is written defensively with fuzzy
prefix/suffix matching to tolerate small naming differences, but if the
OTel badge (demo mode) or the whole graph (otel mode) stays stuck on
"collector not reachable" or empty after the Collector is confirmed up,
check in this order:

1. `docker compose logs otel-collector` (or however you're running your
   own Collector) -- confirm it started cleanly and is listening on both
   its OTLP port and its Prometheus port (a bad collector config or an
   image without the `service_graph` connector -- i.e. using
   `otel/opentelemetry-collector` instead of the `-contrib` image -- will
   show up here).
2. `docker compose logs nettrace` -- look for `[otel] couldn't reach
   collector` or `[otel] couldn't scrape collector metrics` lines, which
   include the actual connection error.
3. `curl http://<collector-host>:<prometheus-port>/metrics` directly -- if
   this returns data but the app still shows nothing, the metric
   names/labels probably don't match what `aggregate_otel_metrics()`
   expects; that function in `app.py` is the place to adjust the matching.

## Deploying against real services

This is `NETTRACE_MODE=otel`: no simulated services, no load generator --
`app.py` only scrapes a Collector's Prometheus endpoint and renders
whatever it finds. Getting real value out of it assumes you already have
(or are willing to stand up) the normal OpenTelemetry pieces; this app
doesn't replace any of them, it's just a viewer for what a Collector's
`service_graph` connector already produces.

**What you need first**, all standard OpenTelemetry, nothing specific to
this repo:

1. Your services emitting OTLP spans -- via `opentelemetry-instrument` for
   near-zero-code auto-instrumentation in most languages/frameworks, or a
   manually wired SDK. Every hop needs a CLIENT span from the caller and a
   SERVER span from the callee with matching trace context, same as any
   distributed tracing setup.
2. An OpenTelemetry Collector (the `-contrib` distribution, since
   `service_graph` isn't in the core build) receiving those spans, with a
   pipeline like `otel-collector-config.yaml` in this repo: `otlp` receiver
   -> `service_graph` connector -> `prometheus` exporter. You likely
   already have a Collector in a real environment for your normal
   tracing backend (Jaeger, Tempo, a vendor); `service_graph` can usually
   just be added to its existing pipeline rather than standing up a
   second Collector.

**Then run nettrace-sankey pointed at it:**

```
NETTRACE_MODE=otel \
OTEL_COLLECTOR_HOST=<your-collector-host> \
OTEL_COLLECTOR_METRICS_PORT=<your-collector-prometheus-port> \
python3 app.py
```

or in Docker, on whatever network can reach your Collector:

```
docker run -p 8766:8766 \
  -e NETTRACE_MODE=otel \
  -e OTEL_COLLECTOR_HOST=<your-collector-host> \
  -e OTEL_COLLECTOR_METRICS_PORT=<your-collector-prometheus-port> \
  --network <your-network> \
  <this-image>
```

`OTEL_COLLECTOR_HTTP_PORT` (the OTLP intake port) doesn't matter in this
mode -- `app.py` never sends spans, only reads `/metrics` back.

**What to expect:** the graph starts empty ("waiting for the Collector to
report service_graph data") and fills in as real traffic happens --
services and edges appear as your Collector's `service_graph` connector
first reports them, with no fixed topology assumed up front. Depth/column
layout is computed from the discovered edges themselves (root services --
the ones nothing else calls -- become column 0), not from any config file,
so this genuinely adapts to whatever shape your real system has. An edge
that's gone quiet for 20 seconds drops off the graph rather than lingering
forever with stale numbers.

**Known real-world wrinkle to expect:** depending on your Collector
version and `service_graph` config, external/root traffic (a request that
arrives with no parent span, from outside anything OTel-instrumented) may
show up with an empty or synthetic `client` label -- this is normal
`service_graph` connector behavior, not a bug in this app, and it'll just
render as its own node.

## Running it (demo mode)

```
python3 app.py
```

then open http://localhost:8766. Or with Docker:

```
docker network create demo-net   # if needed
docker compose up --build -d
```

New to this repo? [CONTRIBUTING.md](CONTRIBUTING.md) walks through all
three ways to run it (demo standalone, demo + real Collector, otel-only
against real services) in order of setup effort.

## Using it (demo mode)

The side panel has two groups of live controls:

- **Service processing time** -- one slider per service, controls that
  service's own `own_delay_ms`. Raising `orders`, for instance, will show
  up in `gateway -> orders`'s `server_ms` immediately, and (since gateway
  waits on all its downstream calls before replying) will cascade into
  `client -> gateway`'s `server_ms` too, within a few seconds.
- **Link (WAN) delay** -- one slider per edge, controls that hop's
  simulated network delay. Push `gateway -> orders` up and watch that
  link's color shift from clean, through smoke, to fire, and its flow dots
  keep moving at the same rate (volume didn't change) while the link
  itself goes red.

`otel` mode has no sliders (there's nothing local to tune -- these are
someone's real services) -- the same panel instead lists whatever edges
have been discovered, read-only, growing on its own as the Collector
reports more.

Numbers update on roughly a 1-second cadence, aggregated over a trailing
5-second rolling window per edge -- so a slider change takes a few seconds
to fully "turn over" the average, rather than jumping instantly. If you
want a snappier feel, shorten `WINDOW_SECONDS` in `app.py`.

## Why no D3 / no charting library

The first prototype's THREE.js came from a CDN and worked fine in a real
browser, but it meant the sandbox this was built in couldn't fully test the
frontend without a workaround (stubbing the library to catch errors
headlessly). For this one, the Sankey layout is hand-rolled vanilla SVG/JS
instead -- a deliberate choice to keep the zero-dependency philosophy
consistent front-to-back, and it meant the whole frontend could be tested
headlessly end-to-end here, including actually dragging a slider in a real
(headless) browser and confirming the readout and link color updated
correctly, before this was ever sent to you.

The layout logic (`computeLayout()` in `sankey.html`) is a simplified
Sankey: nodes are grouped into columns by BFS depth from the entrypoint
(demo mode) or from whatever root services the discovered graph implies
(otel mode), each column is stacked and centered, and a single global
pixels-per-unit scale (calibrated against the busiest column) sizes every
node and link consistently -- deliberately, since an earlier version scaled
each column independently to fill the full canvas height, which made a
column with only one node balloon to enormous size and turned its links
into giant blobs rather than proportional ribbons.

## Tuning / extending

- **Demo topology, per-service delays, and call probabilities:**
  [`topology.demo.json`](topology.demo.json) -- no Python changes needed.
  Schema: `client_label` (string), `entrypoint` (`target`/`op`/
  `min_interval_s`/`max_interval_s` for the load generator), and `services`
  -- a dict keyed by service id, each with a `label`, a unique `port`, an
  `own_delay_ms`, and a `calls` list of `{target, probability, op}`. To add
  a service: add an entry with a unique port, and reference it in another
  service's `calls` (or set it as the `entrypoint.target` if it's the front
  door). The frontend discovers topology entirely from the WebSocket
  `"topology"` message, so no frontend changes are needed for a bigger
  graph -- only the layout margins may need adjusting for a very wide one.
  Override the file path with `TOPOLOGY_FILE=/path/to/other.json`.
- **Run mode:** `NETTRACE_MODE=demo` (default) or `NETTRACE_MODE=otel` --
  see "Deploying against real services" above.
- **Rolling window and broadcast cadence:** `WINDOW_SECONDS` /
  `BROADCAST_INTERVAL` in `app.py`.
- **Severity thresholds and colors:** `THRESH` / `SEVERITY_COLOR` in
  `sankey.html` (kept at the same 50 / 150 / 350ms bands as
  `netviz-prototype` for consistency; the in-app Legend is generated from
  these same constants, so changing them updates the legend automatically).

## Network layer, autodiscovery & inventory API

A second, independent layer alongside the application call graph: real
network devices (routers, switches, hosts) as nodes, real TCP-connect
latency as edges. Toggle between them with the **Application Calls /
Network Latency** buttons at the top of the side panel -- the two layers
share nothing (no merged nodes, no shared severity data), just the same
color scale and diagram style.

**What it measures, honestly:** this is latency from *this host* to each
monitored device -- a single vantage point -- not true hop-by-hop latency
between two arbitrary devices on the wire. There's no agent running on
every discovered device, so device-to-device latency genuinely isn't
something this can measure. Device-to-device *adjacency* (who's physically
next to whom), when known from LLDP/CDP/ARP discovery, is stored in the
inventory and returned by the API, but isn't itself timed. The in-app
Legend explains this every time you switch to the layer.

### Managing endpoints & applications: the REST API

```
GET    /api/endpoints            list all monitored network endpoints
GET    /api/endpoints/{id}       fetch one
POST   /api/endpoints            create (body needs at least "ip"; "id" defaults to "ip")
PUT    /api/endpoints/{id}       update
DELETE /api/endpoints/{id}       remove

GET    /api/applications         list all applications (demo-mode services)
GET    /api/applications/{id}    fetch one
POST   /api/applications         create (body needs "id" and "port")
PUT    /api/applications/{id}    update
DELETE /api/applications/{id}    remove
```

Example -- add a device to the network layer:

```
curl -X POST http://localhost:8766/api/endpoints \
  -H 'Content-Type: application/json' \
  -d '{"id": "core-sw1", "ip": "192.168.1.1", "hostname": "core-sw1", "device_type": "switch", "probe_port": 22}'
```

`probe_port` is whatever TCP port that device will actually answer on for
the latency probe (defaults to 22) -- the probe is a bare TCP connect, not
a protocol handshake, so any consistently-open port works.

Everything persists to `data/inventory.json` (created on first run,
gitignored) and survives a restart. In `demo` mode, `POST`/`DELETE` on
`/api/applications` takes live effect immediately -- it starts or stops a
real local simulated service listener, visible in the app-layer Sankey on
the next tick, not just written to the inventory file. `topology.demo.json`
is only ever read once, to seed the store on its very first run; after
that the store is authoritative and the JSON file is left untouched.

### Autodiscovery

Optional, off by default (`NETTRACE_DISCOVERY_ENABLED=true`). Starting
from configured seed device(s), it walks outward via **LLDP, falling back
to CDP, falling back to ARP** (all via SNMP) -- and if SNMP is entirely
unreachable on a device, falls back further to an **SSH CLI** attempt --
up to a configured hop limit, restricted to an explicit CIDR allowlist.
Discovered endpoints/edges are written into the same inventory as the API,
tagged `source: "discovered"`; a record you added manually (`source:
"manual"`) is never overwritten by discovery.

**This adds two dependencies** (`pysnmp`, `paramiko`) -- a deliberate,
scoped exception to this repo's zero-dependency philosophy, isolated to
`discovery.py`/`snmp_client.py`/`ssh_client.py` and lazily imported only
when discovery is actually turned on. A plain `demo`/`otel` run, the REST
API, and the network-latency probing layer itself never import them and
don't need them installed.

**Setup:**

1. `pip install -r requirements.txt` (or use `Dockerfile.discovery`
   instead of the default `Dockerfile` -- see the note in
   `docker-compose.yml` about why discovery needs real LAN access that the
   default compose network doesn't have).
2. Copy `discovery.config.json.example` to `discovery.config.json` and set
   real `seeds` and `allowed_cidrs` -- **`allowed_cidrs` fails closed: an
   empty list means nothing is allowed to be discovered, not everything.**
   `max_hops` bounds how far the crawl walks outward from your seeds;
   `poll_interval_s` is how often it re-crawls.
3. Credentials (SNMP community string, SSH user/pass) go in **env vars**
   (`NETTRACE_SNMP_COMMUNITY`, `NETTRACE_SSH_USER`, `NETTRACE_SSH_PASS`)
   for the common single-credential case, or in a gitignored
   `discovery.credentials.json` (copy `discovery.credentials.json.example`)
   if different subnets/vendors need different credentials. **Credentials
   are never accepted or returned by the REST API and never written to
   `data/inventory.json`** -- only `discovery.py`/`snmp_client.py`/
   `ssh_client.py` ever read them.
4. `NETTRACE_DISCOVERY_ENABLED=true python3 app.py`

**What's verified vs. what to check on your end** (same spirit as the OTel
section above -- no real network hardware was available while building
this): the SNMP walk mechanics (`snmp_client.py`'s async pysnmp usage,
OID-table parsing) were validated against a real, unmodified `net-snmp`
`snmpd`'s `ipNetToMediaTable` (ARP) -- confirmed byte-for-byte matching
`snmpwalk`'s own output. LLDP-MIB and CDP-MIB walks use the identical
walk/parse mechanics against different, well-documented OIDs, but weren't
run against a real LLDP- or CDP-speaking device (spot-check these first
against your actual switches). The bounded-crawl algorithm itself (hop
limit, CIDR allowlist enforcement, never overwriting manual records) was
verified with mocked neighbor data forming a small multi-hop topology. The
SSH CLI fallback's command list and output-parsing regexes were verified
against realistic sample output for the commands in `ssh_client.py`, but
real vendor CLI output varies -- treat it as a best-effort fallback for
devices SNMP can't reach, and expect to tune the regexes for your gear.
SSH host keys are auto-accepted (trust-on-first-connect, no interactive
prompt) since this runs unattended; don't enable SSH discovery if that
tradeoff isn't acceptable for your network.

### Security

This prototype has never had authentication anywhere (`HOST=0.0.0.0`, no
login) -- consistent with `CONTRIBUTING.md`'s "not production software"
stance. The inventory API and discovery change that calculus a little: the
API can now create/modify a persisted list of real device IPs/hostnames,
and discovery holds live credentials for your network gear. Two basic
deterrents, both opt-in and both off by default so nothing changes unless
you turn them on:

- Set `NETTRACE_API_TOKEN` to require an `X-NetTrace-Token: <token>`
  header on mutating `/api/*` requests (`POST`/`PUT`/`DELETE`). `GET`
  stays open either way, matching the fact that the visualization itself
  has always been open.
- When `NETTRACE_DISCOVERY_ENABLED=true`, `HOST` defaults to `127.0.0.1`
  instead of `0.0.0.0` unless you explicitly override it -- the
  credential-bearing, scan-capable mode shouldn't default to listening on
  every interface.

These are basic deterrents, not production auth -- there's still no user
model, no TLS, no rate limiting. Don't expose this to an untrusted network.

## Next steps toward the real thing

The real-OTel-pipeline step and the "point it at real services" step are
both done. From here, further steps in the same direction: swap the
hand-rolled span export for the actual `opentelemetry-sdk`/
`opentelemetry-exporter-otlp` packages (this build avoided them only to
stay zero-dependency); add the W3C `traceparent` header so demo-mode calls
are correlated the standard way rather than via this app's own wire
protocol; point a real trace backend (Jaeger, Tempo, or similar) at the
same Collector alongside `service_graph` so you can click an edge and see
actual example traces, not just aggregate latency; or add a small
persistence layer so `otel` mode's discovered topology survives a restart
instead of rebuilding from scratch on reconnect.
