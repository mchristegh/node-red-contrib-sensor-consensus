# sensor-consensus — Design Document

## Overview

`sensor-consensus` is a Node-RED node that fuses readings from multiple
sensors (keyed by `msg.topic`) into a single trusted aggregate, and
latches a trigger when that aggregate crosses a configured threshold.
It is not a statistics/history node — it holds exactly one *latest*
value per source and answers the question "what do my sensors, taken
together, say right now, and can I trust it?" Freshness (staleness
windows), quorum, and disagreement detection are first-class features,
not afterthoughts.

Sibling of `node-red-contrib-timer-events` and
`node-red-contrib-timer-threshold` — same event envelope philosophy
(`ignored`, `source`), same command conventions, same output
exclusivity guarantees, so downstream flows can process all three with
the same code.

The node has **1 input** and **4 outputs**:

| # | Label   | Fires on |
|---|---------|----------|
| 1 | Trigger | A genuine `untriggered` → `triggered` transition. Nothing else. |
| 2 | Release | A genuine `triggered` → `untriggered` transition (threshold re-crossed, or quorum lost under the Release policy). Nothing else. |
| 3 | Query   | An incoming `query` message, or a Heartbeat tick. Nothing else. |
| 4 | Events  | Every other event, plus a duplicate copy of every Trigger/Release event. The only output where `msg.ignored` can be `true`. |

Outputs 1 and 2 **never** carry a blocked/suppressed/ignored message —
anything that didn't truly happen only appears on output 4.

---

## Type Modes

The node operates in exactly one of two modes, selected at the node
level in configuration — never inferred per message:

- **Numeric** — payloads are coerced with `Number(payload)`; `NaN`
  rejects the reading. The aggregate is the configured aggregation
  function over the fresh sources' latest values.
- **Boolean** — payloads are coerced against configurable true/false
  value lists (see Boolean Coercion below). The aggregate is the
  **fraction of fresh sources reporting true** (0.0–1.0), and the
  trigger threshold is expressed as a vote rule preset that maps to a
  fraction internally.

**Key design decision — one node instance = one type mode = one group
of like sensors.** Mixing humidity readings and door contacts in one
instance has no coherent semantics; deploy one instance per group.

### Boolean Coercion

Coercion is case-insensitive and whitespace-trimmed, against two
configurable comma-separated lists:

| List | Default values |
|---|---|
| True values  | `true, on, yes, 1` |
| False values | `false, off, no, 0` |

Native booleans `true`/`false` and numbers `1`/`0` always coerce
regardless of the lists. Any payload matching neither list is a
rejected reading (`reading`/`ignored:true`). Application-specific
vocabularies (`open`/`closed`, `wet`/`dry`, …) are handled by editing
the lists, not by hardcoded guesses.

### Command precedence

A recognized command string in `msg.payload` (any case) is **always**
interpreted as a command, in both modes, before any coercion is
attempted. In numeric mode this costs nothing (command strings are
`NaN` anyway); in boolean mode it means a command word can never be
added to the true/false lists — the command interpretation wins and
this is documented as a hard rule.

---

## Sources, Freshness, and Quorum

- **Identity:** sources are keyed by `msg.topic`. A reading with an
  empty/missing topic is rejected (`reading`/`ignored:true`).
- **Registration:** sources are learned dynamically on first message
  (`sourceadded`). Optionally, an **Expected sources** config list
  (comma-separated topics) declares the roster up front — expected
  sources appear in every snapshot from deploy, marked as never-seen
  until their first reading, making "sensor 3 never came back after
  the power cut" visible instead of silent.
- **Latest-value-per-source:** each source holds exactly one value and
  its `lastSeen` timestamp. There is no history buffer.
- **Staleness:** a per-node staleness window (duration + units,
  `0` = disabled). A source whose `lastSeen` ages past the window is
  marked stale, excluded from the aggregate, and announced with a
  `sourcestale` event — which requires the node's **own internal
  staleness clock**, since a sensor going quiet is precisely the event
  that produces no message. Staleness expiry recomputes the aggregate
  and re-evaluates the trigger immediately, `source:"internal"`. With
  the window at `0`, last values are held indefinitely (the
  zero-config, all-sensors-healthy case). A reading from a stale
  source marks it fresh again (`sourcerecovered`).
- **Quorum:** a configured minimum number of *fresh* sources
  (`0` = quorum disabled). Below quorum, the aggregate is still
  computed and reported (marked `quorum:false`), but trigger/release
  evaluation is suspended. Crossing the quorum boundary emits
  `quorumlost`/`quorumregained`.

**Key design decision — quorum loss while latched.** Configurable,
because it is genuinely application-dependent:

| Policy | Behavior |
|---|---|
| **Hold (default)** | The latch keeps its last consensus; state stays `triggered` with `quorum:false` until data returns and re-evaluation resumes. |
| Release | Quorum loss immediately releases the latch — a normal Release event (output 2 + 4) with `releaseReason:"quorumlost"`. |

---

## Consensus States (`msg.consensusState`)

```
                    first evaluation with quorum satisfied
        waiting ────────────────────────────────────────► untriggered
           ▲                                              │        ▲
           │                             aggregate crosses │        │ aggregate re-crosses
         reset                            trigger threshold│        │ release threshold, or
      (any state)                                          ▼        │ quorumlost (Release policy)
           │                                            triggered ──┘
```

- `waiting` — no evaluation has ever succeeded (no readings yet, or
  quorum never met since deploy/reset)
- `untriggered` — evaluated, aggregate on the release side of the
  thresholds
- `triggered` — latched; the trigger threshold was crossed

`disabled` and `quorum` are **flags in the envelope**, not states —
mirroring how the timer siblings treat `disabled`.

---

## Trigger / Release Semantics

- **Trigger direction (numeric mode):** configurable — trigger when the
  aggregate rises **Above** (≥ trigger value) or falls **Below**
  (≤ trigger value). Covers both "humidity too high" and "freezer too
  warm... temperature too low" style automations.
- **Hysteresis:** a separate release value on the opposite side of the
  trigger value (Above-mode requires release ≤ trigger; Below-mode
  requires release ≥ trigger; enforced via `setrelease` at runtime).
  Default: release equals trigger (no hysteresis). Editor-side
  cross-field validators are unreliable in Node-RED (they see pre-edit
  values), so an **inverted configured pair is sanitized at node
  construction** instead: the release is ignored (falls back to
  same-as-trigger) with a single `node.warn` — the node's only warn,
  reserved for configuration defects; runtime commands surface as
  `ignored:true` events per house convention.
- **Release-follows-trigger:** when no *distinct* release exists (the
  configured release equals the configured trigger and no `setrelease`
  override has been applied), the effective release is defined as "same
  as the effective trigger" *dynamically* — the numeric analog of
  boolean mode's `sameastrigger` preset — so `settrigger` moves both
  thresholds together and a no-hysteresis threshold can be moved freely
  at runtime. Once a distinct release exists, `settrigger` and
  `setrelease` are guarded **symmetrically**: either command is rejected
  (`ignored:true`, attempted value included) if it would put the pair on
  the wrong side of each other, because an inverted pair flaps
  Trigger/Release on every reading in the band — exactly the failure
  hysteresis exists to prevent. Consequence for users moving *both*
  thresholds of a hysteresis pair at runtime: move them in the order
  that keeps the pair valid at each step (Above-mode moving down:
  `setrelease` first, then `settrigger`). This ordering note belongs in
  the embedded help and the wiki's Input Messages / setrelease coverage.
- **Boolean mode:** the trigger rule is a preset over the fraction of
  fresh sources reporting true — **Any** (> 0), **Majority** (≥ 0.5),
  **All** (= 1.0), or **At least N** (N a config count). The
  denominator is the *fresh* source count. The release rule is its own
  preset, defaulting to "Same as trigger" (release the moment the rule
  stops being satisfied); choosing a looser release rule (e.g. trigger
  on All, release below Majority) is the boolean analog of hysteresis
  and the built-in flap suppressor.
- **Latching:** Trigger fires once per `untriggered` → `triggered`
  transition; while latched, further trigger-side evaluations are
  silent (the aggregate keeps flowing on output 4). Release re-arms it.

---

## Message Envelope

Every output message is a clone of the message that triggered it (see
"Baseline message lineage" below), with these properties layered on top:

| Property | Description |
|---|---|
| `msg.consensusEvent` | The event type (see taxonomy below) |
| `msg.consensusState` | `waiting`, `untriggered`, or `triggered` |
| `msg.aggregate` | Current aggregate value (numeric value, or fraction-true in boolean mode); `null` while `waiting` with no data |
| `msg.aggregation` | The function actually used (`mean`, `median`, `min`, `max`, `trimmedmean`, `mean(fallback)`, `fraction`) |
| `msg.quorum` | Whether quorum is currently satisfied (boolean; `true` when quorum is disabled) |
| `msg.freshCount` | Number of fresh sources |
| `msg.sourceCount` | Number of known sources (learned + expected) |
| `msg.sources` | Per-source breakdown: `{ <topic>: { value, lastSeen (ISO 8601 or null), stale (bool), seen (bool) } }` |
| `msg.disabled` | Current disabled state (boolean) |
| `msg.ignored` | `true` if this message was received but did not change node state. Always `false` on outputs 1, 2, 3. |
| `msg.source` | `"external"` (live incoming message) or `"internal"` (staleness expiry, heartbeat tick, persisted restore) |

Event-specific extras: `releaseReason` (`"threshold"` \| `"quorumlost"`),
`rejectedValue`, `staleTopic`, `recoveredTopic`, `addedTopic`,
`removedTopic`, `minorityTopics`, `triggerSet`, `releaseSet`,
`staleSet` — attached only to the relevant event types, and included
even on *rejected* attempts so downstream consumers can see what was
refused.

**Key design decision — envelopes reflect the instant of dispatch.**
Events fire in causal order, and each envelope is a snapshot of the
world *at the moment that event is dispatched* — earlier envelopes are
never re-stamped with knowledge of the later events they cause. Two
observable consequences, both deliberate: (1) a `quorumregained` that
enables the first-ever evaluation reports `consensusState:"waiting"`,
because the silent waiting → untriggered bookkeeping happens after it
(entering evaluation is not itself an event); (2) a `sourcestale` whose
expiry causes quorum loss reports the pre-update `quorum:true` — the
`quorumlost` event follows immediately as its own message and is the
correction.

---

## Event Type Taxonomy (`msg.consensusEvent`)

| Event | Output(s) | Can be `ignored:true`? | `source` values |
|---|---|---|---|
| `triggered` | 1 + 4 | Yes, on 4 only (transition suppressed while disabled) | external, internal |
| `released` | 2 + 4 | Yes, on 4 only (transition suppressed while disabled) | external, internal |
| `reading` | 4 only | Yes (unparseable/uncoercible payload, missing topic) | external |
| `sourceadded` | 4 only | No | external |
| `sourcerecovered` | 4 only | No | external |
| `sourcestale` | 4 only | No | external (via `setstale`), internal |
| `sourceremoved` | 4 only | Yes (unknown topic) | external |
| `quorumlost` | 4 only | No | external, internal |
| `quorumregained` | 4 only | No | external, internal |
| `minorityreport` | 4 only | No — deliberate notification | external, internal |
| `disabled` | 4 only | Yes (redundant) | external |
| `enabled` | 4 only | Yes (redundant) | external |
| `reset` | 4 only | No | external |
| `triggerset` | 4 only | Yes (invalid value) | external |
| `releaseset` | 4 only | Yes (invalid / wrong side of trigger) | external |
| `staleset` | 4 only | Yes (negative value) | external |
| `query` | 3 only | No | external, internal |

**Key design decision — one message, one event.** A single incoming
reading emits exactly one event on output 4, the most specific one that
applies: a first-ever reading from a topic emits `sourceadded` (not
`sourceadded` + `reading`); a reading from a stale source emits
`sourcerecovered`. All three reading-class events carry the full
recomputed aggregate, so nothing is lost by the substitution. Trigger/
Release/quorum transitions caused by that same reading are separate
events (they are transitions, not readings) and fire in addition, in
causal order: reading-class event → quorum event → trigger/release
event.

**Key design decision — `ignored` is a modifier, not a category.** A
rejected payload is still a `reading` (`ignored:true`, with
`rejectedValue`); a suppressed transition while disabled is still
`triggered`/`released` (`ignored:true`). The event names what was
attempted; `ignored` says whether it took effect.

### `reading` chattiness — Emit aggregate on change only

A checkbox, **default checked**: accepted-reading events (`reading`
only — not `sourceadded`/`sourcerecovered`, which are always
noteworthy) are emitted only when the reading changed the aggregate
value. Unchecked, every accepted reading emits. Rejected readings
(`ignored:true`) always emit regardless — they are diagnostics, and
suppressing them would reintroduce silent drops.

### `minorityreport` (boolean mode only, v1)

When a consensus exists (quorum satisfied and the fraction is not
exactly split), any fresh source disagreeing with the consensus value
is listed in `minorityTopics`. The event fires when the minority set
*changes* (not on every reading), so a persistently stuck sensor
produces one report, not a stream. Numeric-mode divergence detection is
deferred (see Deliberately Dropped / Deferred).

---

## Control Commands (`msg.payload`, case-insensitive)

| Command | Effect |
|---|---|
| `query` | Full snapshot on output 3, no side effects |
| `disable` | Suppresses Trigger/Release transitions (see below). Readings still accepted and accumulated. Redundant → `ignored:true`. |
| `enable` | Re-allows transitions and immediately re-evaluates (see below). Redundant → `ignored:true`. |
| `reset` | Clears all learned sources, values, and the latch; returns to `waiting`. Expected sources remain on the roster (never-seen). Works in any state. |
| `remove` | Drops one source by `msg.removetopic` (decommissioned sensor); recomputes and re-evaluates. Unknown topic → `ignored:true`. |
| `settrigger` | Sets the trigger value at runtime — `msg.settrigger` (numeric mode: a number; boolean mode: a preset name or fraction; `atleastn` takes its count from `msg.settriggern`). Numeric mode is guarded symmetrically with `setrelease` when a distinct release exists (see Release-follows-trigger under Trigger / Release Semantics). Invalid or wrong-side → `ignored:true` with the attempted value. |
| `setrelease` | Sets the release value, validated against the trigger direction (boolean mode: preset — incl. `sameastrigger` — or fraction; `atleastn` count from `msg.setreleasen`). Invalid/wrong side → `ignored:true` with the attempted value. |
| `setstale` | Sets the staleness window in ms (`msg.setstale`, optional `msg.setstaleunits`); `0` disables staleness. Negative → `ignored:true`. Takes effect immediately: every seen source is re-evaluated against the new window **in both directions** — a shrunk window can fire `sourcestale` on the spot, a widened/disabled one can fire `sourcerecovered` for sources whose data sits back inside it. These flips carry the command's `source` (the one path where `sourcestale` is externally sourced). |

Runtime threshold/staleness changes trigger an immediate re-evaluation.
No `node.warn()` for rejected commands — the `ignored:true` output-4
event is the sole surfacing mechanism, per house convention.

**Key design decision — `disable` suppresses decisions, not data.**
While disabled, readings continue to be accepted, sources stay fresh,
staleness/quorum tracking continues, and the aggregate keeps flowing on
output 4 — only the latch is frozen. A threshold crossing while
disabled emits `triggered`/`released` with `ignored:true` on output 4
(the transition that *would* have happened). On `enable`, the node
immediately re-evaluates: if the aggregate sits on the other side of
the latch's threshold, the genuine transition fires then (outputs
1/2 + 4, `source:"external"` — the enable command is its live trigger).
This preserves the sibling principle that disable never blinds the
node, only stays its hand.

**Key design decision — no `pause`/`resume`.** Pausing a consensus node
is ambiguous (do readings still accumulate? does staleness advance?)
and every concrete use case examined reduces to `disable`. Deliberately
omitted; see Deliberately Dropped / Deferred.

---

## Blocking / Gating Rules

Evaluated in this order for every incoming message:

1. **Command gate** — recognized command strings are handled as
   commands, always, in both type modes.
2. **Topic gate** — a non-command message with a missing/empty
   `msg.topic` is a rejected `reading` (`ignored:true`).
3. **Coercion gate** — payload must coerce under the node's type mode;
   failure is a rejected `reading` (`ignored:true`, `rejectedValue`).
4. **Accepted** — the source's latest value updates, the aggregate
   recomputes, and evaluation proceeds:
   - **Quorum gate** — below quorum, trigger/release evaluation is
     suspended (aggregate still reported, `quorum:false`).
   - **Disabled gate** — a warranted transition is suppressed to
     output 4 as `ignored:true`.

Heartbeat, staleness expiry, and restore are internally-sourced and
enter at step 4's recompute/evaluate stage directly.

---

## Feature: Heartbeat

- Configurable fixed-interval tick (interval + units, `0` = disabled)
  producing a Query-output snapshot (output 3, `source:"internal"`).
- **Runs continuously from deploy** — unlike the timer siblings, this
  node has no bounded "run" to scope the heartbeat to; it is a
  monitoring node and its idle state (`waiting`, stale sources, lost
  quorum) is precisely what a watchdog wants to see. Independent
  `setInterval`, unaffected by any command or evaluation.
- After a persisted restore, restarts fresh rather than resuming the
  original schedule.

## Feature: Aggregation Functions (numeric mode)

| Function | Notes |
|---|---|
| Mean (default) | Arithmetic mean of fresh values |
| Median | Robust default recommendation for 3+ noisy sensors |
| Min / Max | For worst-case style automations |
| Trimmed mean | Drops the single highest and lowest fresh value; requires ≥ 3 fresh sources, otherwise **falls back to mean** and reports `aggregation:"mean(fallback)"` in the envelope rather than failing or going silent |

Boolean mode always uses `fraction` (fraction of fresh sources true).

---

## Baseline Message Lineage

The analog of the timer siblings' `originalMsg`: internally-sourced
events (staleness expiry, heartbeat ticks, quorum changes caused by
staleness, restore) have no live triggering message, so they clone the
**most recent accepted reading's message** as their payload base.

- **Set/overwritten by:** every accepted reading (`reading`,
  `sourceadded`, `sourcerecovered`).
- **Read/cloned by:** `sourcestale`, heartbeat `query`, internally
  caused `quorumlost`/`quorumregained`, internally caused
  `triggered`/`released`, `minorityreport` when internally caused.
- **Untouched by:** all commands — these clone their own triggering
  message.

---

## Persistence (`Resume state on deploy/restart`)

Disabled by default. When enabled, state is written to
`<userDir>/sensorconsensus-state/<node-id>` on every meaningful change
and restored on startup:

- Persisted: per-source latest values and `lastSeen` timestamps,
  learned source roster, `consensusState` (latch), `disabled`, and
  runtime overrides (`settrigger`/`setrelease`/`setstale` values).
- On restore, every stored source's staleness is **re-evaluated against
  wall-clock time** — a long outage naturally restores into "all
  sources stale, quorum lost" rather than pretending old readings are
  fresh. The resulting `sourcestale`/`quorumlost` events fire with
  `source:"internal"`.
- The latch restores to its saved state silently — **restore never
  fires Trigger or Release by itself**; the first genuine transition
  after restore comes from live evaluation.
- **Deviation from the timer siblings, to be flagged in a code
  comment:** no 3–8 s randomization on restore. That mechanism exists
  to stagger *scheduled firings* after mass restore; this node has no
  scheduled firing to stagger, and restore is transition-silent by the
  rule above.
- Unrelated to Node-RED's built-in "Persistent Context."

---

## Status Label

| State | Indicator | Text |
|---|---|---|
| `waiting` | grey dot | `Waiting (fresh/quorum: F/Q)` |
| `untriggered`, quorum OK | green dot | `<aggregate> (F/S fresh)` |
| `triggered` | blue dot | `Triggered: <aggregate> (F/S fresh)` |
| Quorum lost | yellow ring | `No quorum (F/Q) | <last aggregate>` — with `Triggered |` prefix if latched under Hold |
| Disabled | grey ring | `Disabled | ` + normal text |

Purely cosmetic, per house convention — the status label never produces
output messages. Numeric aggregate rendered to a sensible fixed
precision; boolean mode shows the fraction as `T/F of N` style counts.

---

## What Was Deliberately Dropped / Deferred

- **`pause`/`resume`** — ambiguous semantics for a consensus node;
  `disable` covers the real use cases. Dropped.
- **Enum/string consensus (mode / most-common-value)** — fits the
  architecture but triples the coercion and disagreement rules;
  deferred to a future version, recorded here so the door stays open.
- **Weighted averages, standard deviation** — deferred; median +
  trimmed mean cover the v1 robustness need.
- **Numeric-mode divergence events** — trimmed mean already blunts
  outliers silently; a numeric `minorityreport` analog needs a
  deviation-threshold design of its own. Deferred.
- **Per-source staleness windows** — one node-level window keeps v1
  configuration honest; per-source overrides deferred.
- **Time-window / history statistics** — out of scope by design; this
  node is latest-value-per-source consensus. `node-red-contrib-aggregator`
  and `node-red-contrib-statistics` own that space.
- **Per-output event filtering** — a downstream `switch` node handles
  this transparently, per house convention.

---

## Configuration Reference

| Field | Default | Notes |
|---|---|---|
| Type mode | Numeric | Numeric / Boolean — node-level, never per message |
| Aggregation | Mean | Numeric mode: mean / median / min / max / trimmed mean |
| Trigger direction | Above | Numeric mode: Above (≥) / Below (≤) |
| Trigger value | — (required) | Numeric mode threshold. Blank parses as *unset* (`null`) — the node accumulates and reports but never triggers — not as a phantom `0` |
| Release value | = trigger | Numeric mode; blank = same as trigger (release-follows-trigger). Validated to the opposite side of trigger when distinct |
| Boolean trigger rule | Majority | Any / Majority / All / At least N (+ N field) |
| Boolean release rule | Same as trigger | Optional looser rule = flap suppression |
| True values / False values | `true,on,yes,1` / `false,off,no,0` | Boolean coercion lists |
| Expected sources | (empty) | Comma-separated topics; empty = learn dynamically |
| Quorum (min fresh sources) | 0 | `0` = quorum disabled |
| Staleness window / units | 0 / Second | `0` = staleness disabled (hold values forever) |
| On quorum lost while latched | Hold | Hold / Release |
| Emit aggregate on change only | Checked | Suppresses unchanged-aggregate `reading` events |
| Heartbeat interval / units | 0 / Second | `0` disables; runs continuously from deploy |
| Resume state on deploy/restart | Off | Persistence |
| Name | (empty) | Suppresses the generated label |

---

## Repository Structure

```
node-red-contrib-sensor-consensus/    (repo root = npm package root)
├── package.json                       # node-red registration, test scripts
├── README.md                          # overview; details live in the wiki
├── sensor-consensus-design.md         # this document
├── examples/                          # importable flow JSON
├── sensor-consensus/
│   ├── sensor-consensus.js            # runtime logic
│   ├── sensor-consensus.html          # editor UI + embedded help
│   └── cycle.js                       # persistence serialization (must sit
│                                      #   next to the .js for require('./cycle.js'))
└── test-scripts/
    └── test-harness.js                # standalone assertion harness
```

Same conventions as the siblings: constants blocks at module level, one
envelope builder + one dispatcher encoding the output-exclusivity
rules, independent timer handles (staleness clock vs. heartbeat),
`npm test` and `prepublishOnly` both running the harness.

---

## Testing

Standalone harness (`test-scripts/test-harness.js`) stubbing the RED
runtime — no framework dependencies, counted PASS/FAIL checks, non-zero
exit on any failure (verified: a deliberately broken check exits 1, so
the `prepublishOnly` gate is real). Module resolution via `__dirname`
with a flat-layout fallback; if `cycle.js` is absent next to the source,
the harness runs from a temp copy with an identity stub and prints a
NOTE.

**Current inventory: 144 checks across 8 suites** —

1. Output routing & envelope shape (exclusivity per output, full
   envelope field set, clone integrity, rejection paths)
2. Aggregation functions (mean / median odd+even / min / max / trimmed
   mean incl. the named `mean(fallback)`)
3. Boolean mode (default + custom coercion lists, majority latch and
   the exact-0.5 edge, minority report on-change-only incl. resolution
   to empty, `atleastn` preset, command precedence over coercion)
4. Commands (disable/enable suppression cycle, redundancy, remove,
   reset roster + override survival, setstale validation + units,
   emit-on-change both settings, the documented `quorumregained`-shows-
   `waiting` nuance)
5. Threshold set-commands (release-follows-trigger free move, symmetric
   wrong-side guards in both directions, ordered pair move, boolean
   overrides incl. the `atleastn` count requirement and fraction
   release, blank-config parsing regressions — blank release =
   same-as-trigger, blank trigger = never triggers, no phantom `0` —
   and inverted-configured-pair sanitization incl. the no-flap check)
6. Status labels (all five label states, boolean T/F counts, held-latch
   prefix on quorum loss)
7. Staleness / quorum / heartbeat, async with real ~300 ms windows
   (Hold vs Release policies, the pre-update-quorum envelope nuance,
   staggered independent expiries, disabled-node tracking + suppressed
   release firing on enable, setstale both directions with external
   source, heartbeat continuous-from-deploy through stop-on-close)
8. Persistence (event-silent quick restore, discriminating override
   survival, outage restore firing `sourcestale`/`quorumlost` with the
   pre-update-quorum regression check, transition-silence, held-latch
   status after restore, removal cleanup, persist-off cleanup)

Real-environment items the mock cannot verify (to exercise before
release): editor status rendering, restore timing in a live runtime.
