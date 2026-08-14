# Testing / contributing to nettrace-sankey

Thanks for taking a look. This is a small prototype, not production
software, so "contributing" mostly means: run it, poke at it, and report
back what broke or what was confusing. Here's the fastest path to each.

## 1. Just see it work (30 seconds, no Docker, no dependencies)

```
git clone <this repo>
cd nettrace-sankey
python3 app.py
```

Open http://localhost:8766. That's the whole install -- `app.py` is plain
Python 3 standard library, nothing to `pip install`. You'll see the
built-in simulated "webshop" demo (client → gateway → auth/catalog/orders →
payments), with live sliders to crank up delay on any service or link and
watch the diagram react. The **Legend** panel (bottom-right, collapsible)
explains what every color and shape means.

The OTel Collector badge will say "not reachable" in this mode -- that's
expected, there's no Collector running yet, and the demo works fully
without one (see "What's real vs. simulated" in README.md).

## 2. See the full pipeline, including a real OpenTelemetry Collector

```
docker network create demo-net   # skip if you already have a network you want to use
docker compose up --build -d
open http://localhost:8766
```

Now the OTel banner should turn green within a few seconds, and each
edge's readout shows a live `OTel ✓` line -- numbers independently
re-derived by a real, unmodified `otel/opentelemetry-collector-contrib`
container from real spans this app exported to it. If it doesn't turn
green, `docker compose logs otel-collector` and `docker compose logs
nettrace` are the first things to check (README.md has a longer
troubleshooting list).

## 3. Point it at your own real services instead of the demo

This is the interesting one if you're evaluating whether this pattern is
useful beyond the toy demo. See README.md's "Deploying against real
services" section for the full walkthrough; the short version is:

```
NETTRACE_MODE=otel OTEL_COLLECTOR_HOST=<your-collector-host> python3 app.py
```

pointed at a Collector that's already receiving OTLP spans from your own
services and running the `service_graph` connector. No code from this repo
runs against your services at all -- it only ever reads your Collector's
Prometheus output.

## What's most useful to hear back about

- Does the demo mode's legend actually make the visualization readable on
  first look, or is something still unclear?
- If you tried mode 3 against a real system: did topology discovery behave
  sensibly (right nodes, right edges, latency numbers that match what you
  already know about that system)?
- Anything in README.md that was wrong, missing, or assumed context you
  didn't have.
- Whether the zero-dependency approach (hand-rolled WebSocket server,
  hand-rolled OTLP export, hand-rolled SVG Sankey layout) is worth keeping
  vs. just depending on real libraries -- it was a deliberate choice for
  this prototype, not a permanent constraint.

## Code tour

| File | What it is |
|---|---|
| `app.py` | The whole backend: demo services, load generator, OTLP span export, Prometheus scrape/parse, hand-rolled WebSocket server. Start here; it's one file, read top to bottom, heavily commented. |
| `sankey.html` | The whole frontend: SVG Sankey layout, legend, controls, WebSocket client. Also one file, no build step. |
| `topology.demo.json` | The demo's topology, delays, and call-fanout probabilities -- edit this to reshape the demo without touching Python. |
| `otel-collector-config.yaml` | Config for the real Collector container: OTLP receiver, `service_graph` connector, Prometheus exporter. |
| `docker-compose.yml` | Runs `nettrace` + `otel-collector` together on the external `demo-net` network. |

If you change something, there's no test suite (small prototype, remember)
-- just run it and watch it work, in both demo and (if you can) otel mode.
