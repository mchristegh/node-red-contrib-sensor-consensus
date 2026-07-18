# Conventions

Standards for this repository and its sibling `node-red-contrib-*`
projects. The audience is the maintainer and any AI-assisted coding
session: apply these conventions to all new and modified code. Where a
judgment call arises, match the surrounding code — consistency with what
exists beats any rule here.

Formatting and correctness are owned by tooling (Prettier and ESLint —
see `eslint.config.js` and `.prettierrc`) and are not restated here.
This document covers what tools cannot check: what comments should say,
what tests should look like, and where each kind of documentation lives.

## Documentation architecture — what lives where

Each fact should have exactly one home. When behavior changes, update
the home, then check the other layers for stale echoes.

| Layer                                                   | Owns                                                                                                                | Does not own                                       |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| **Code comments**                                       | Design rationale, invariants, non-obvious decisions and their reasons                                               | Usage instructions, restating the code             |
| **Editor help panel** (`.html`, `data-help-name` block) | Everyday usage: outputs, commands, config fields, message properties                                                | Deep behavioral detail, changelog, troubleshooting |
| **README.md**                                           | What the node is, why it exists, feature overview, install, pointer to the wiki                                     | Full reference material                            |
| **Project wiki**                                        | The complete reference: configuration, event/command taxonomy, persistence behavior, example flows, troubleshooting | Nothing exclusive to code internals                |
| **Test check labels**                                   | The exact guaranteed behavior, including what the old broken behavior was                                           | —                                                  |

## Code comments

The prime directive: **comments explain why, not what.** The code
already says what it does; a comment earns its place by recording the
reasoning, the invariant, or the rejected alternative that the code
cannot express. If a decision was debated and settled, say so — future
readers (including future AI sessions) must be able to distinguish a
deliberate behavior from an accident.

Good, from `timer-events.js` — records a deliberate omission and its
reason:

```js
// Deliberately short, no ignored-count/last-ignored detail here -
// ignored messages during cooldown aren't actionable the way they
// are while running, so surfacing them would just add clutter.
```

Good — records a settled decision so it is never "fixed":

```js
// ... including adjusttime: 0 (accepted per explicit decision) ...
```

Avoid comments that restate the line below them. `// increment counter`
above `count++` is noise; delete it.

### File headers

Every source file opens with a block comment containing, in order: the
node name; a short prose description of what the file implements
(for the main runtime file, include a summary of the output model or
other core architecture); provenance (what the code is derived from,
if anything); copyright lines, newest first; and the Apache 2.0 license
notice. See the top of `timer-events.js` for the canonical example.

### Section banners

Source files are organized into titled sections using this exact form
(full-width dashes to column 79):

```js
// -------------------------------------------------------------------------
// Runtime state variables
// -------------------------------------------------------------------------
```

Test files use `=` banners for suite boundaries:

```js
// ============================================================================
// SUITE: test-restore
// ============================================================================
```

Section titles are short noun phrases. New code goes into the section
where it belongs; if no section fits, add one rather than appending to
the end of the file.

### Function documentation

Significant functions carry a `/** ... */` block written as prose, not
as strict tag-based JSDoc. The block states what the function
guarantees, its role in the wider design, and any behavioral subtleties
a caller must know. Enumerations of modes or cases are welcome inside
the prose. Example shape (from `buildEventMessage` and
`getElapsedTime`): a one-sentence contract, then the state-by-state or
case-by-case detail. `@param`/`@returns` tags are optional and used
only when parameter semantics are not obvious from names.

Small helpers need no block when their name and a one-line inline
comment suffice.

### Inline comments

Inline comments annotate the non-obvious: why a guard exists, what a
magic value means, which historical bug a line prevents. Trailing
comments on the same line are fine for short annotations
(`// ignored (locked) -> ignoredCount 1`); longer rationale goes on its
own line(s) above the code.

## Test conventions

Tests live in `test-scripts/` as a single self-contained battery with
zero dependencies, runnable by plain Node (`npm test`). The harness
stubs the Node-RED runtime rather than importing it; the stub
(`makeRED`) is duplicated per suite so each suite remains standalone
and copy-paste extractable.

Structure and style:

- One suite per fix or feature area, registered with
  `__defineSuite(name, fn)` and returning its failure count.
- Every assertion goes through `check(label, cond, detail)` and prints
  `PASS`/`FAIL` with the label; the process exits 0 only on a fully
  clean run. The publish gate (`prepublishOnly`) depends on this exit
  code.
- **Check IDs**: labels start with a suite letter and number plus a
  lowercase step letter — `T1a`, `V3b`, `Z1c` — unique across the
  battery so a failure is findable by its ID alone.
- **"was:" annotations**: when a check guards a fixed bug, the label
  names the old broken behavior in parentheses:
  `"T2 query at t+4s reports ~6000ms (was: 10000ms)"`. This is the
  project's changelog-in-tests and is required for regression checks.
- Regression checks that protect still-correct old behavior are
  labeled as such: `"X4a regression - genuine stop still ..."` style.
- Wall-clock assertions use `near(actual, expected, tol)` with
  tolerances of 300–800 ms. Do not tighten tolerances below 300 ms;
  CI and Codespaces runners jitter. A lone timing failure on a shared
  runner warrants a rerun before investigation.
- Comments in tests explain scenario intent ("engineered so remaining
  is a messy fraction") the same way source comments explain design.

## Wiki approach

The GitHub wiki is the single full reference. Its standing page set:
configuration reference, event taxonomy, command details, persistence
behavior, example flows, and troubleshooting. Behaviors that look like
bugs but are deliberate (redundant-command handling, threshold scoping,
cooldown semantics) are documented on the Troubleshooting page — the
CONTRIBUTING file points issue reporters there first.

When a change touches commands, events, the message envelope,
configuration fields, or persistence format, updating the affected wiki
pages is part of the change, not a follow-up. Checklist before
publishing a release:

1. Help panel (`.html`) reflects any command/output/config change.
2. Wiki pages for the affected areas are updated.
3. README feature list still accurate (it is an overview — only touch
   it for genuinely new capabilities).
4. Full battery passes (`npm test`).

Links from repo files to the wiki use the relative `../../wiki` form so
they survive forks and renames.

## Repository furniture

Each repo carries the same set: `README.md` in the established shape
(what/why, outputs table, features at a glance, install, documentation
pointer, license with provenance chain), `CONTRIBUTING.md` (issues
welcome, PRs not accepted), `LICENSE` (Apache 2.0), issue templates,
`examples/` with importable flows, and the shared lint/format configs.
Vendored third-party files (e.g. `cycle.js`) are kept byte-identical to
upstream and are excluded from linting and formatting — never edit
them; replace them wholesale from upstream if an update is needed.
