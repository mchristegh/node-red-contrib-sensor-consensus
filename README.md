# node-red-contrib-sensor-consensus

A Node-RED sensor-fusion / consensus node. Readings from multiple
sensors (keyed by `msg.topic`) are fused into a single trusted
aggregate, and a trigger latches when that aggregate crosses a
configured threshold — with **freshness** (staleness windows),
**quorum**, and **disagreement detection** as first-class features
rather than afterthoughts.

Averaging three healthy humidity sensors is easy. What existing
aggregation nodes don't answer is what happens when one of them goes
quiet, sticks, or drifts: a dead sensor's last reading silently skews
the average forever, and nothing tells you a trigger didn't fire
because trust was lost rather than because the value was fine. This
node makes every one of those decisions observable.

## Highlights

- **Two type modes** (per node instance): _Numeric_ — mean, median,
  min, max, or trimmed mean over the latest value from each source —
  or _Boolean_ — vote rules (Any / Majority / All / At least N) over
  the fraction of sources reporting true, with configurable true/false
  value lists.
- **Trust built in**: per-source staleness windows, fresh-source
  quorum, minority reports when a sensor disagrees with its peers, and
  a configurable Hold/Release policy for quorum loss while triggered.
- **Latched threshold trigger** with direction (above/below) and
  hysteresis, plus runtime commands to move thresholds, adjust the
  staleness window, disable/enable decisions, and remove or reset
  sources.
- **Four purpose-built outputs** — Trigger, Release, Query, Events —
  where outputs 1 and 2 carry only what genuinely happened, and every
  blocked, rejected, or suppressed action is observable on the Events
  output with `msg.ignored = true`.
- **Heartbeat** snapshots for watchdogs, and optional **persistence**
  across deploys/restarts with wall-clock staleness re-evaluation on
  restore.

## Install

From the Node-RED palette manager, or:

```
cd ~/.node-red
npm install node-red-contrib-sensor-consensus
```

## Documentation

The embedded node help (in the editor sidebar) is a complete
reference. Detailed documentation, examples, and troubleshooting live
in the [project wiki](https://github.com/mchristegh/node-red-contrib-sensor-consensus/wiki).
The authoritative record of design decisions is
[`sensor-consensus-design.md`](./sensor-consensus-design.md) in this
repository.

## Sibling nodes

This node shares its event envelope (`msg.ignored`, `msg.source`),
command conventions, and output-exclusivity guarantees with
[node-red-contrib-timer-events](https://github.com/mchristegh/node-red-contrib-timer-events)
and
[node-red-contrib-timer-threshold](https://github.com/mchristegh/node-red-contrib-timer-threshold)
— downstream flows can process events from all three with the same
code.

## Testing

```
npm test
```

runs the standalone assertion harness (no framework dependencies;
144 checks across 8 suites at the time of writing). Publishing is
gated on the same harness via `prepublishOnly`.

## License

Apache-2.0. Includes Douglas Crockford's public-domain `cycle.js` for
persistence serialization.
