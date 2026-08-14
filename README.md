# NetViz

A small line of prototypes exploring whether the old NetQoS SuperAgent idea
-- a live 3D topology where slow packets visibly smoke and catch fire --
could be rebuilt today, for free, self-hosted. Each folder is a step
further than the last:

## [`netviz-prototype/`](netviz-prototype/)

The first sketch: a single-vantage-point network topology (one client, a
few servers) rendered in 3D with Three.js. Packets travel visibly between
nodes; slow network transit time or slow server response time makes a
packet's link smoke, then catch fire, echoing the original SuperAgent
visual language. Zero third-party Python dependencies -- a hand-rolled
`asyncio` WebSocket server -- and the whole frontend is one HTML file.

## [`nettrace-sankey/`](nettrace-sankey/)

The next step: from raw network timing to *application* call graphs.
Instead of one fixed topology, this is a small multi-service demo (a toy
"webshop") where every service-to-service call is genuinely timed and
rendered as a live Sankey-style flow diagram -- link color is latency
severity (the same clean/smoke/fire idea), link width is traffic volume,
and moving dots are individual calls in flight. It also wires up a *real*
OpenTelemetry Collector pipeline (the `service_graph` connector) alongside
its own in-band measurement as a live cross-check, and can run in a mode
that drops the simulated demo entirely and renders live topology/latency
discovered from your own real, independently-instrumented services. See
its own README for the full story, an in-app legend explaining every color
and shape, and a [CONTRIBUTING.md](nettrace-sankey/CONTRIBUTING.md) with
the fastest path to trying each mode.

## Why two folders instead of one

Each prototype deliberately kept the zero-dependency, single-file-frontend
philosophy of the one before it, so each is fully runnable and testable
top to bottom (down to headless-browser tests of real DOM interaction)
without a build step or an external service. `nettrace-sankey` is the more
current and more capable of the two; `netviz-prototype` is kept as-is
since it's a genuinely different visual approach (3D scene vs. Sankey
diagram) that might still be the more legible fit for some topologies.
