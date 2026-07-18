#!/usr/bin/env node
/**
 * test-harness.js - standalone assertion harness for
 * node-red-contrib-sensor-consensus.
 *
 * House conventions (see the development guidelines / sibling nodes):
 *   - No framework dependencies: stubs just enough of the RED runtime to
 *     instantiate the real node and drive it with real messages.
 *   - Counted PASS/FAIL checks with a summary line; process exits
 *     non-zero on any failure (this is what makes the prepublishOnly
 *     gate real).
 *   - Module resolution via __dirname, never the working directory.
 *   - If cycle.js is absent next to the source, the node is copied to a
 *     temp dir with a four-line identity stub (with a printed NOTE).
 *
 * Suites:
 *   1. Output routing & envelope shape
 *   2. Aggregation functions (numeric)
 *   3. Boolean mode: coercion, vote rules, minority reports
 *   4. Commands (sync paths)
 *   5. Threshold set-commands & release-follows-trigger (regressions)
 *   6. Status labels
 *   7. Staleness, quorum policies, heartbeat (async, real short timers)
 *   8. Persistence (write / quick restore / outage restore / cleanup)
 *
 * Known limits (real-environment items the stub cannot verify): editor
 * status rendering, restore timing in a live Node-RED runtime.
 */
"use strict";

const fs = require("fs");
const path = require("path");
const os = require("os");

// ---------------------------------------------------------------------------
// Locate and load the node under test
// ---------------------------------------------------------------------------

const CANDIDATES = [
  path.join(__dirname, "..", "sensor-consensus", "sensor-consensus.js"),
  path.join(__dirname, "sensor-consensus.js"),
  path.join(__dirname, "..", "sensor-consensus.js"),
];

const sourcePath = CANDIDATES.find(function (p) {
  return fs.existsSync(p);
});
if (!sourcePath) {
  console.error(
    "FATAL: cannot locate sensor-consensus.js (tried:\n  " +
      CANDIDATES.join("\n  ") +
      ")",
  );
  process.exit(1);
}

let loadPath = sourcePath;
if (!fs.existsSync(path.join(path.dirname(sourcePath), "cycle.js"))) {
  const tmpSrcDir = fs.mkdtempSync(path.join(os.tmpdir(), "sc-harness-src-"));
  fs.copyFileSync(sourcePath, path.join(tmpSrcDir, "sensor-consensus.js"));
  fs.writeFileSync(
    path.join(tmpSrcDir, "cycle.js"),
    'if (typeof JSON.decycle !== "function") { JSON.decycle = function (o) { return o; }; }\n' +
      'if (typeof JSON.retrocycle !== "function") { JSON.retrocycle = function (o) { return o; }; }\n',
  );
  loadPath = path.join(tmpSrcDir, "sensor-consensus.js");
  console.log(
    "NOTE: cycle.js not found next to source; running from temp copy with identity stub\n      (" +
      tmpSrcDir +
      ")",
  );
}

const USER_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "sc-harness-userdir-"));

// ---------------------------------------------------------------------------
// Minimal RED stub
// ---------------------------------------------------------------------------

let registeredCtor = null;

const RED = {
  nodes: {
    createNode: function (node, n) {
      node.id = n.id;
    },
    registerType: function (name, ctor) {
      registeredCtor = ctor;
    },
  },
  util: {
    cloneMessage: function (m) {
      return JSON.parse(JSON.stringify(m || {}));
    },
  },
  settings: { userDir: USER_DIR },
};

require(loadPath)(RED);
if (typeof registeredCtor !== "function") {
  console.error("FATAL: node did not register a constructor");
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Assertion plumbing
// ---------------------------------------------------------------------------

let passCount = 0;
let failCount = 0;

function suite(name) {
  console.log("\n=== " + name + " ===");
}

function check(desc, cond) {
  if (cond) {
    passCount++;
    console.log("  PASS: " + desc);
  } else {
    failCount++;
    console.log("  FAIL: " + desc);
  }
}

function checkEq(desc, actual, expected) {
  const ok = actual === expected;
  if (!ok)
    desc +=
      "  [expected " +
      JSON.stringify(expected) +
      ", got " +
      JSON.stringify(actual) +
      "]";
  check(desc, ok);
}

const sleep = function (ms) {
  return new Promise(function (r) {
    setTimeout(r, ms);
  });
};

// ---------------------------------------------------------------------------
// Node instantiation helper
// ---------------------------------------------------------------------------

let idCounter = 0;

function mkNode(cfg) {
  if (!cfg.id) cfg.id = "test-" + ++idCounter;
  const batches = []; // every node.send([o1,o2,o3,o4])
  const statusLog = [];
  const errors = [];
  const warns = [];
  const handlers = {};
  const node = {};

  node.status = function (s) {
    statusLog.push(s);
  };
  node.error = function (e) {
    errors.push(String(e));
  };
  node.warn = function (w) {
    warns.push(String(w));
  };
  node.on = function (ev, fn) {
    handlers[ev] = fn;
  };
  node.send = function (arr) {
    batches.push(arr);
  };
  RED.nodes.createNode(node, cfg);
  registeredCtor.call(node, cfg);

  return {
    node: node,
    errors: errors,
    warns: warns,
    statusLog: statusLog,
    lastStatus: function () {
      return statusLog[statusLog.length - 1];
    },
    input: function (msg) {
      handlers.input(msg);
    },
    drain: function () {
      const o = batches.slice();
      batches.length = 0;
      return o;
    },
    close: function (removed) {
      return new Promise(function (res) {
        handlers.close(removed === true, res);
      });
    },
  };
}

// Batch helpers. Each dispatched event is one send([out1,out2,out3,out4]).
function firstMsg(batch) {
  return batch[0] || batch[1] || batch[2] || batch[3];
}
function eventsOf(batches) {
  return batches.map(firstMsg);
}
function findEvent(batches, evt) {
  return (
    eventsOf(batches).find(function (m) {
      return m && m.consensusEvent === evt;
    }) || null
  );
}
function findBatch(batches, evt) {
  return (
    batches.find(function (b) {
      const m = firstMsg(b);
      return m && m.consensusEvent === evt;
    }) || null
  );
}
function eventNames(batches) {
  return eventsOf(batches)
    .map(function (m) {
      return m.consensusEvent + (m.ignored ? "!" : "");
    })
    .join(",");
}
function stateFileFor(id) {
  return path.join(USER_DIR, "sensorconsensus-state", id);
}

const NUM_BASE = {
  typemode: "numeric",
  aggregation: "mean",
  triggerdir: "above",
  triggervalue: "60",
  releasevalue: "55",
  quorum: "0",
  stalewindow: "0",
  quorumlostpolicy: "hold",
  emitonchange: true,
  heartbeatinterval: "0",
  persist: false,
};

function numCfg(over) {
  return Object.assign({}, NUM_BASE, over || {});
}

// ---------------------------------------------------------------------------
// Suite 1: Output routing & envelope shape
// ---------------------------------------------------------------------------

function suite1() {
  suite("Suite 1: Output routing & envelope shape");
  const h = mkNode(numCfg());
  h.drain();

  h.input({ topic: "s1", payload: 70 });
  let b = h.drain();
  checkEq(
    "first reading emits sourceadded then triggered",
    eventNames(b),
    "sourceadded,triggered",
  );

  const saB = findBatch(b, "sourceadded");
  check(
    "sourceadded routes to output 4 only",
    saB[3] !== null && !saB[0] && !saB[1] && !saB[2],
  );

  const trB = findBatch(b, "triggered");
  check(
    "triggered routes to outputs 1 AND 4",
    trB[0] !== null && trB[3] !== null && !trB[1] && !trB[2],
  );
  checkEq("output-1 copy is not ignored", trB[0].ignored, false);

  // Envelope shape on the output-1 message
  const env = trB[0];
  [
    "consensusEvent",
    "consensusState",
    "aggregate",
    "aggregation",
    "quorum",
    "freshCount",
    "sourceCount",
    "sources",
    "disabled",
    "ignored",
    "source",
  ].forEach(function (f) {
    check("envelope has ." + f, env[f] !== undefined);
  });
  checkEq("envelope clones triggering msg payload", env.payload, 70);
  checkEq("envelope clones triggering msg topic", env.topic, "s1");
  checkEq("envelope source is external", env.source, "external");
  checkEq("per-source breakdown carries value", env.sources.s1.value, 70);

  h.input({ payload: "query" });
  b = h.drain();
  check(
    "query routes to output 3 only",
    b.length === 1 && b[0][2] !== null && !b[0][0] && !b[0][1] && !b[0][3],
  );
  checkEq("query event name", b[0][2].consensusEvent, "query");

  h.input({ topic: "s1", payload: 40 });
  b = h.drain();
  const relB = findBatch(b, "released");
  check(
    "released routes to outputs 2 AND 4",
    relB && relB[1] !== null && relB[3] !== null && !relB[0] && !relB[2],
  );
  checkEq("release reason is threshold", relB[1].releaseReason, "threshold");

  h.input({ topic: "s1", payload: "abc" });
  b = h.drain();
  checkEq(
    "unparseable payload is reading ignored:true",
    eventNames(b),
    "reading!",
  );
  check(
    "rejected reading routes to output 4 only",
    b[0][3] !== null && !b[0][0] && !b[0][1] && !b[0][2],
  );
  checkEq("rejectedValue carries the attempt", b[0][3].rejectedValue, "abc");

  h.input({ payload: 41 });
  b = h.drain();
  checkEq("missing topic is reading ignored:true", eventNames(b), "reading!");
}

// ---------------------------------------------------------------------------
// Suite 2: Aggregation functions (numeric)
// ---------------------------------------------------------------------------

function suite2() {
  suite("Suite 2: Aggregation functions");

  function aggOf(aggregation, values) {
    const h = mkNode(
      numCfg({ aggregation: aggregation, triggervalue: "10000" }),
    );
    h.drain();
    values.forEach(function (v, i) {
      h.input({ topic: "s" + i, payload: v });
    });
    h.drain();
    h.input({ payload: "query" });
    const q = h.drain()[0][2];
    return q;
  }

  checkEq("mean of 10,20,30", aggOf("mean", [10, 20, 30]).aggregate, 20);
  checkEq(
    "median odd count 10,90,20",
    aggOf("median", [10, 90, 20]).aggregate,
    20,
  );
  checkEq(
    "median even count 10,20,30,40",
    aggOf("median", [10, 20, 30, 40]).aggregate,
    25,
  );
  checkEq("min of 7,3,9", aggOf("min", [7, 3, 9]).aggregate, 3);
  checkEq("max of 7,3,9", aggOf("max", [7, 3, 9]).aggregate, 9);

  let q = aggOf("trimmedmean", [10, 50, 90]);
  checkEq("trimmed mean drops high+low (10,50,90 -> 50)", q.aggregate, 50);
  checkEq("trimmed mean reports its name", q.aggregation, "trimmedmean");

  q = aggOf("trimmedmean", [10, 50]);
  checkEq("trimmed mean <3 sources falls back to mean", q.aggregate, 30);
  checkEq("fallback is named in the envelope", q.aggregation, "mean(fallback)");
}

// ---------------------------------------------------------------------------
// Suite 3: Boolean mode
// ---------------------------------------------------------------------------

function suite3() {
  suite("Suite 3: Boolean mode");

  const boolCfg = {
    typemode: "boolean",
    boolrule: "majority",
    boolreleaserule: "sameastrigger",
    quorum: "0",
    stalewindow: "0",
    emitonchange: true,
    heartbeatinterval: "0",
    persist: false,
  };

  // Coercion defaults
  let h = mkNode(Object.assign({}, boolCfg));
  h.drain();
  [
    ["d1", true],
    ["d2", "ON"],
    ["d3", "yes"],
    ["d4", "1"],
    ["d5", 1],
  ].forEach(function (p) {
    h.input({ topic: p[0], payload: p[1] });
  });
  h.drain();
  h.input({ payload: "query" });
  let q = h.drain()[0][2];
  checkEq(
    "default true-list coerces true/ON/yes/'1'/1 (fraction 1.0)",
    q.aggregate,
    1,
  );
  h.input({ topic: "d1", payload: "OFF" });
  h.input({ topic: "d2", payload: "no" });
  h.input({ topic: "d3", payload: 0 });
  h.drain();
  h.input({ payload: "query" });
  q = h.drain()[0][2];
  checkEq(
    "default false-list coerces OFF/no/0 (fraction 2/5)",
    q.aggregate,
    2 / 5,
  );
  h.input({ topic: "d1", payload: "weird" });
  checkEq("unlisted value rejected", eventNames(h.drain()), "reading!");

  // Custom lists
  h = mkNode(
    Object.assign({}, boolCfg, { truevalues: "open", falsevalues: "closed" }),
  );
  h.drain();
  h.input({ topic: "w1", payload: "OPEN" });
  let b = h.drain();
  check(
    "custom true value 'OPEN' accepted and triggers (any/majority of 1)",
    findEvent(b, "triggered") !== null,
  );
  h.input({ topic: "w1", payload: "true" });
  checkEq(
    "default 'true' rejected when custom lists replace defaults",
    eventNames(h.drain()),
    "reading!",
  );

  // Majority latch behavior + minority reports
  h = mkNode(Object.assign({}, boolCfg));
  h.drain();
  h.input({ topic: "d1", payload: true });
  b = h.drain();
  check("1/1 majority triggers", findEvent(b, "triggered") !== null);
  h.input({ topic: "d2", payload: false });
  b = h.drain();
  check(
    "exact 0.5 stays latched (majority >= 0.5)",
    findEvent(b, "released") === null,
  );
  check(
    "exact split produces no minority report",
    findEvent(b, "minorityreport") === null,
  );
  h.input({ topic: "d3", payload: false });
  b = h.drain();
  check("1/3 releases under sameastrigger", findEvent(b, "released") !== null);
  const mr = findEvent(b, "minorityreport");
  check(
    "minority report fires with [d1]",
    mr !== null &&
      mr.minorityTopics.length === 1 &&
      mr.minorityTopics[0] === "d1",
  );
  h.input({ topic: "d3", payload: false });
  b = h.drain();
  check(
    "unchanged minority set does not re-report",
    findEvent(b, "minorityreport") === null,
  );
  h.input({ topic: "d1", payload: false });
  b = h.drain();
  const mr2 = findEvent(b, "minorityreport");
  check(
    "minority resolution (set -> empty) reports once",
    mr2 !== null && mr2.minorityTopics.length === 0,
  );

  // At-least-N config preset
  h = mkNode(
    Object.assign({}, boolCfg, { boolrule: "atleastn", boolrulen: "2" }),
  );
  h.drain();
  h.input({ topic: "d1", payload: true });
  h.input({ topic: "d2", payload: false });
  b = h.drain();
  check(
    "atleastn(2): 1 true does not trigger",
    findEvent(b, "triggered") === null,
  );
  h.input({ topic: "d3", payload: true });
  b = h.drain();
  check("atleastn(2): 2 true triggers", findEvent(b, "triggered") !== null);

  // Command precedence over coercion
  h = mkNode(Object.assign({}, boolCfg));
  h.drain();
  h.input({ topic: "d1", payload: "RESET" });
  b = h.drain();
  checkEq(
    "command string wins over coercion even with a topic",
    eventNames(b),
    "reset",
  );
}

// ---------------------------------------------------------------------------
// Suite 4: Commands (sync paths)
// ---------------------------------------------------------------------------

function suite4() {
  suite("Suite 4: Commands");

  // disable / enable suppression cycle
  let h = mkNode(numCfg({ quorum: "2" }));
  h.drain();
  h.input({ topic: "s1", payload: 50 });
  h.input({ topic: "s2", payload: 52 });
  h.drain();
  h.input({ payload: "DISABLE" });
  let b = h.drain();
  checkEq(
    "disable (case-insensitive) emits disabled",
    eventNames(b),
    "disabled",
  );
  h.input({ payload: "disable" });
  checkEq(
    "redundant disable is ignored:true",
    eventNames(h.drain()),
    "disabled!",
  );
  h.input({ topic: "s2", payload: 80 }); // mean 65 >= 60
  b = h.drain();
  const supp = findBatch(b, "triggered");
  check(
    "warranted trigger while disabled is suppressed to output 4 (ignored:true)",
    supp !== null && supp[3].ignored === true && !supp[0],
  );
  checkEq(
    "reading still accepted while disabled (data keeps flowing)",
    findEvent(b, "reading") !== null,
    true,
  );
  h.input({ payload: "enable" });
  b = h.drain();
  const gen = findBatch(b, "triggered");
  check(
    "enable re-evaluates: genuine trigger fires on outputs 1+4",
    gen !== null && gen[0] !== null && gen[0].ignored === false,
  );
  checkEq(
    "genuine post-enable trigger is externally sourced",
    gen[0].source,
    "external",
  );
  h.input({ payload: "enable" });
  checkEq(
    "redundant enable is ignored:true",
    eventNames(h.drain()),
    "enabled!",
  );

  // remove
  h.input({ payload: "remove", removetopic: "nope" });
  b = h.drain();
  check(
    "remove unknown topic is ignored:true with attempted topic",
    firstMsg(b[0]).ignored === true && firstMsg(b[0]).removedTopic === "nope",
  );
  h.input({ payload: "remove", removetopic: "s1" });
  b = h.drain();
  checkEq(
    "remove known topic emits sourceremoved then quorumlost",
    eventNames(b),
    "sourceremoved,quorumlost",
  );

  // reset: expected roster survives, overrides survive
  h = mkNode(
    numCfg({
      expectedsources: "e1,e2",
      triggervalue: "60",
      releasevalue: "60",
    }),
  );
  h.drain();
  h.input({ payload: "query" });
  let q = h.drain()[0][2];
  checkEq("expected sources pre-registered (sourceCount 2)", q.sourceCount, 2);
  checkEq("expected source visible as never-seen", q.sources.e1.seen, false);
  h.input({ topic: "e1", payload: 30 });
  b = h.drain();
  check(
    "first reading from an expected source is sourceadded",
    findEvent(b, "sourceadded") !== null,
  );
  h.input({ payload: "settrigger", settrigger: 70 }); // free move (no distinct release)
  h.drain();
  h.input({ payload: "reset" });
  b = h.drain();
  checkEq(
    "reset emits reset in post-reset waiting state",
    firstMsg(b[0]).consensusState,
    "waiting",
  );
  h.input({ payload: "query" });
  q = h.drain()[0][2];
  checkEq("reset keeps expected roster", q.sourceCount, 2);
  checkEq("reset clears aggregate", q.aggregate, null);
  h.input({ topic: "e1", payload: 65 }); // >= config 60 but < surviving override 70
  b = h.drain();
  check(
    "settrigger override SURVIVES reset (65 vs override 70 must not trigger)",
    findEvent(b, "triggered") === null,
  );
  h.input({ topic: "e1", payload: 71 });
  b = h.drain();
  check(
    "...and 71 vs override 70 triggers",
    findEvent(b, "triggered") !== null,
  );

  // setstale sync paths
  h = mkNode(numCfg({ quorum: "2" }));
  h.drain();
  h.input({ topic: "s1", payload: 50 });
  h.input({ topic: "s2", payload: 50 });
  h.drain();
  h.input({ payload: "setstale", setstale: -5 });
  b = h.drain();
  check(
    "negative setstale is ignored:true",
    firstMsg(b[0]).ignored === true && firstMsg(b[0]).staleSet === -5,
  );
  h.input({ payload: "setstale", setstale: 1, setstaleunits: "seconds" });
  b = h.drain();
  checkEq(
    "setstale units conversion (1 second -> 1000ms)",
    findEvent(b, "staleset").staleSet,
    1000,
  );

  // emit-on-change-only
  h = mkNode(numCfg());
  h.drain();
  h.input({ topic: "s1", payload: 50 });
  h.input({ topic: "s2", payload: 50 });
  h.drain();
  h.input({ topic: "s1", payload: 50 });
  checkEq(
    "unchanged-aggregate reading suppressed when emitonchange checked",
    h.drain().length,
    0,
  );
  h = mkNode(numCfg({ emitonchange: false }));
  h.drain();
  h.input({ topic: "s1", payload: 50 });
  h.input({ topic: "s2", payload: 50 });
  h.drain();
  h.input({ topic: "s1", payload: 50 });
  checkEq(
    "unchanged-aggregate reading emitted when emitonchange unchecked",
    eventNames(h.drain()),
    "reading",
  );

  // quorumregained-before-first-evaluation nuance (documented)
  h = mkNode(numCfg({ quorum: "2" }));
  h.drain();
  h.input({ topic: "s1", payload: 50 });
  h.input({ topic: "s2", payload: 50 });
  b = h.drain();
  const qr = findEvent(b, "quorumregained");
  checkEq(
    "documented nuance: quorumregained enabling first evaluation reports waiting",
    qr.consensusState,
    "waiting",
  );
}

// ---------------------------------------------------------------------------
// Suite 5: Threshold set-commands & release-follows-trigger
// ---------------------------------------------------------------------------

function suite5() {
  suite("Suite 5: Threshold set-commands & release-follows-trigger");

  // No-hysteresis: settrigger moves freely, release follows
  let h = mkNode(numCfg({ triggervalue: "60", releasevalue: "60" }));
  h.drain();
  h.input({ topic: "s1", payload: 45 });
  h.drain();
  h.input({ payload: "settrigger", settrigger: 40 });
  let b = h.drain();
  check(
    "no-hysteresis settrigger 40 applies (release-follows-trigger)",
    findEvent(b, "triggerset") !== null &&
      findEvent(b, "triggerset").ignored === false,
  );
  check(
    "set-command re-evaluates in place (45 >= 40 triggers immediately)",
    findEvent(b, "triggered") !== null,
  );
  h.input({ topic: "s1", payload: 39 });
  b = h.drain();
  check(
    "release FOLLOWED the trigger (39 < 40 releases)",
    findEvent(b, "released") !== null,
  );

  // Hysteresis pair: symmetric guard
  h = mkNode(numCfg({ triggervalue: "60", releasevalue: "55" }));
  h.drain();
  h.input({ payload: "settrigger", settrigger: "high" });
  b = h.drain();
  check(
    "non-numeric settrigger rejected with attempted value",
    firstMsg(b[0]).ignored === true && firstMsg(b[0]).triggerSet === "high",
  );
  h.input({ payload: "settrigger", settrigger: 50 });
  b = h.drain();
  check(
    "wrong-side settrigger (50 < release 55) rejected",
    firstMsg(b[0]).ignored === true,
  );
  h.input({ payload: "setrelease", setrelease: 75 });
  b = h.drain();
  check(
    "wrong-side setrelease (75 > trigger 60) rejected",
    firstMsg(b[0]).ignored === true,
  );
  h.input({ payload: "setrelease", setrelease: 45 });
  h.input({ payload: "settrigger", settrigger: 50 });
  b = h.drain();
  check(
    "ordered pair move down (setrelease first) applies both",
    findEvent(b, "releaseset") !== null &&
      findEvent(b, "triggerset") !== null &&
      findEvent(b, "releaseset").ignored === false &&
      findEvent(b, "triggerset").ignored === false,
  );

  // Below-mode guard mirrored
  h = mkNode(
    numCfg({ triggerdir: "below", triggervalue: "5", releasevalue: "8" }),
  );
  h.drain();
  h.input({ payload: "settrigger", settrigger: 10 });
  check(
    "below-mode wrong-side settrigger (10 > release 8) rejected",
    firstMsg(h.drain()[0]).ignored === true,
  );
  h.input({ payload: "settrigger", settrigger: 6 });
  check(
    "below-mode valid settrigger 6 applies",
    firstMsg(h.drain()[0]).ignored === false,
  );

  // Boolean overrides
  h = mkNode({
    typemode: "boolean",
    boolrule: "all",
    boolreleaserule: "sameastrigger",
    quorum: "0",
    stalewindow: "0",
    emitonchange: true,
    heartbeatinterval: "0",
    persist: false,
  });
  h.drain();
  h.input({ payload: "settrigger", settrigger: "atleastn" });
  check(
    "boolean atleastn without count rejected",
    firstMsg(h.drain()[0]).ignored === true,
  );
  h.input({ payload: "settrigger", settrigger: "atleastn", settriggern: 2 });
  h.drain();
  h.input({ topic: "d1", payload: true });
  h.input({ topic: "d2", payload: false });
  let b2 = h.drain();
  check(
    "atleastn(2) override: 1 true does not trigger",
    findEvent(b2, "triggered") === null,
  );
  h.input({ topic: "d3", payload: true });
  b2 = h.drain();
  check(
    "atleastn(2) override: 2 true triggers",
    findEvent(b2, "triggered") !== null,
  );
  h.input({ payload: "setrelease", setrelease: 0.34 });
  h.drain();
  h.input({ topic: "d3", payload: false }); // fraction 1/3 = 0.333 < 0.34 releases
  b2 = h.drain();
  check(
    "boolean fraction release override (1/3 < 0.34 releases)",
    findEvent(b2, "released") !== null,
  );

  // Blank-config parsing regressions (Number("") === 0 hazard)
  h = mkNode(numCfg({ triggervalue: "60", releasevalue: "" }));
  h.drain();
  h.input({ topic: "s1", payload: 60 });
  let b3 = h.drain();
  check(
    "blank release: trigger still fires at the trigger value",
    findEvent(b3, "triggered") !== null,
  );
  h.input({ topic: "s1", payload: 59 });
  b3 = h.drain();
  check(
    "blank release behaves as same-as-trigger (59 < 60 releases; a phantom 0 would never release)",
    findEvent(b3, "released") !== null,
  );
  h = mkNode(numCfg({ triggervalue: "", releasevalue: "" }));
  h.drain();
  h.input({ topic: "s1", payload: 1e9 });
  b3 = h.drain();
  check(
    "blank trigger never triggers (null, not a phantom 0)",
    findEvent(b3, "triggered") === null,
  );

  // Inverted configured pair: constructor sanitization guard
  h = mkNode(numCfg({ triggervalue: "60", releasevalue: "55" }));
  check("valid configured pair does not warn", h.warns.length === 0);
  h = mkNode(numCfg({ triggervalue: "50", releasevalue: "63" }));
  h.drain();
  check(
    "inverted pair (above-mode) warns exactly once",
    h.warns.length === 1 && h.warns[0].indexOf("wrong side") !== -1,
  );
  h.input({ topic: "s1", payload: 55 });
  b3 = h.drain();
  check(
    "sanitized pair triggers normally (55 >= 50)",
    findEvent(b3, "triggered") !== null,
  );
  h.input({ topic: "s1", payload: 52 });
  b3 = h.drain();
  check(
    "sanitized pair does NOT flap (52 stays latched; unsanitized release 63 would fire here)",
    findEvent(b3, "released") === null,
  );
  h.input({ topic: "s1", payload: 49 });
  b3 = h.drain();
  check(
    "sanitized release follows trigger (49 < 50 releases)",
    findEvent(b3, "released") !== null,
  );
  h = mkNode(
    numCfg({ triggerdir: "below", triggervalue: "8", releasevalue: "5" }),
  );
  check("inverted pair (below-mode) warns too", h.warns.length === 1);
}

// ---------------------------------------------------------------------------
// Suite 6: Status labels
// ---------------------------------------------------------------------------

function suite6() {
  suite("Suite 6: Status labels");

  let h = mkNode(numCfg({ quorum: "2" }));
  let s = h.lastStatus();
  check(
    "new node: grey dot Waiting F/Q",
    s.fill === "grey" &&
      s.shape === "dot" &&
      s.text === "Waiting (fresh/quorum: 0/2)",
  );
  h.input({ topic: "s1", payload: 50 });
  checkEq(
    "waiting counts update",
    h.lastStatus().text,
    "Waiting (fresh/quorum: 1/2)",
  );
  h.input({ topic: "s2", payload: 54 });
  s = h.lastStatus();
  check(
    "running: green dot with aggregate + fresh counts",
    s.fill === "green" && s.shape === "dot" && s.text === "52 (2/2 fresh)",
  );
  h.input({ topic: "s2", payload: 80 });
  s = h.lastStatus();
  check(
    "triggered: blue dot with Triggered prefix",
    s.fill === "blue" &&
      s.shape === "dot" &&
      s.text === "Triggered: 65 (2/2 fresh)",
  );
  h.input({ payload: "disable" });
  s = h.lastStatus();
  check(
    "disabled: grey ring with Disabled | prefix",
    s.fill === "grey" &&
      s.shape === "ring" &&
      s.text === "Disabled | Triggered: 65 (2/2 fresh)",
  );
  h.input({ payload: "enable" });
  h.input({ payload: "remove", removetopic: "s1" });
  s = h.lastStatus();
  check(
    "quorum lost while latched (Hold): yellow ring, Triggered | No quorum",
    s.fill === "yellow" &&
      s.shape === "ring" &&
      s.text === "Triggered | No quorum (1/2) | 80",
  );

  h = mkNode({
    typemode: "boolean",
    boolrule: "all",
    boolreleaserule: "sameastrigger",
    quorum: "0",
    stalewindow: "0",
    emitonchange: true,
    heartbeatinterval: "0",
    persist: false,
  });
  h.input({ topic: "d1", payload: true });
  h.input({ topic: "d2", payload: false });
  s = h.lastStatus();
  checkEq(
    "boolean status shows T/F of N counts",
    s.text,
    "1T/1F of 2 (2/2 fresh)",
  );
}

// ---------------------------------------------------------------------------
// Suite 7: Staleness, quorum policies, heartbeat (async)
// ---------------------------------------------------------------------------

async function suite7() {
  suite("Suite 7: Staleness, quorum policies, heartbeat (async)");

  const staleCfg = {
    stalewindow: "300",
    stalewindowunits: "Millisecond",
    quorum: "2",
  };

  // Hold policy: latch survives quorum loss; sourcestale shows pre-update quorum
  let h = mkNode(numCfg(staleCfg));
  h.drain();
  h.input({ topic: "s1", payload: 70 });
  h.input({ topic: "s2", payload: 70 });
  h.drain();
  await sleep(450);
  let b = h.drain();
  const st1 = findEvent(b, "sourcestale");
  check(
    "staleness expiry fires sourcestale (internal)",
    st1 !== null && st1.source === "internal",
  );
  checkEq(
    "documented nuance: sourcestale shows pre-update quorum:true",
    st1.quorum,
    true,
  );
  check(
    "quorumlost follows the sourcestale events",
    findEvent(b, "quorumlost") !== null,
  );
  check(
    "HOLD policy: no release on quorum loss",
    findEvent(b, "released") === null,
  );
  h.input({ payload: "query" });
  const q = h.drain()[0][2];
  checkEq("held latch still reports triggered", q.consensusState, "triggered");
  checkEq("held latch reports quorum:false", q.quorum, false);
  // Recovery
  h.input({ topic: "s1", payload: 70 });
  h.input({ topic: "s2", payload: 70 });
  b = h.drain();
  check(
    "reading from stale source is sourcerecovered",
    findEvent(b, "sourcerecovered") !== null,
  );
  check("quorum regained on recovery", findEvent(b, "quorumregained") !== null);
  check(
    "no duplicate trigger on recovery (still latched)",
    findEvent(b, "triggered") === null,
  );
  await h.close(false);

  // Release policy
  h = mkNode(
    numCfg(Object.assign({}, staleCfg, { quorumlostpolicy: "release" })),
  );
  h.drain();
  h.input({ topic: "a", payload: 70 });
  h.input({ topic: "b", payload: 70 });
  h.drain();
  await sleep(450);
  b = h.drain();
  const relB = findBatch(b, "released");
  check(
    "RELEASE policy: quorum loss releases on outputs 2+4",
    relB !== null && relB[1] !== null && relB[3] !== null,
  );
  checkEq("release reason is quorumlost", relB[1].releaseReason, "quorumlost");
  await h.close(false);

  // Staggered expiries fire independently
  h = mkNode(numCfg(staleCfg));
  h.drain();
  h.input({ topic: "s1", payload: 50 });
  await sleep(180);
  h.input({ topic: "s2", payload: 50 });
  h.drain();
  await sleep(200); // ~380ms after s1, ~200ms after s2
  b = h.drain();
  const stale1 = eventsOf(b).filter(function (m) {
    return m.consensusEvent === "sourcestale";
  });
  check(
    "staggered lastSeen: only the older source expired",
    stale1.length === 1 && stale1[0].staleTopic === "s1",
  );
  await sleep(180);
  b = h.drain();
  const stale2 = eventsOf(b).filter(function (m) {
    return m.consensusEvent === "sourcestale";
  });
  check(
    "...and the younger expired on its own later clock",
    stale2.length === 1 && stale2[0].staleTopic === "s2",
  );
  await h.close(false);

  // Disabled node: data tracking continues, suppressed release fires on enable
  h = mkNode(
    numCfg(Object.assign({}, staleCfg, { quorumlostpolicy: "release" })),
  );
  h.drain();
  h.input({ topic: "a", payload: 70 });
  h.input({ topic: "b", payload: 70 });
  h.drain();
  h.input({ payload: "disable" });
  h.drain();
  await sleep(450);
  b = h.drain();
  check(
    "staleness tracking continues while disabled",
    findEvent(b, "sourcestale") !== null,
  );
  const suppRel = findBatch(b, "released");
  check(
    "RELEASE-policy quorum release suppressed while disabled (ignored:true, out4 only)",
    suppRel !== null && suppRel[3].ignored === true && !suppRel[1],
  );
  h.input({ payload: "enable" });
  b = h.drain();
  const genRel = findBatch(b, "released");
  check(
    "enable fires the suppressed release genuinely on outputs 2+4",
    genRel !== null && genRel[1] !== null && genRel[1].ignored === false,
  );
  await h.close(false);

  // setstale two-directional re-evaluation (external source)
  h = mkNode(numCfg({ quorum: "2", stalewindow: "0" }));
  h.drain();
  h.input({ topic: "s1", payload: 50 });
  h.input({ topic: "s2", payload: 50 });
  h.drain();
  await sleep(50);
  h.input({ payload: "setstale", setstale: 1 }); // 1ms: both instantly stale
  b = h.drain();
  const extStale = findEvent(b, "sourcestale");
  check(
    "setstale shrink fires sourcestale with EXTERNAL source",
    extStale !== null && extStale.source === "external",
  );
  check("...and quorumlost", findEvent(b, "quorumlost") !== null);
  h.input({ payload: "setstale", setstale: 0 }); // disable: both recover
  b = h.drain();
  check(
    "setstale 0 recovers sources (sourcerecovered, external)",
    findEvent(b, "sourcerecovered") !== null &&
      findEvent(b, "sourcerecovered").source === "external",
  );
  check("...and quorum regained", findEvent(b, "quorumregained") !== null);
  await h.close(false);

  // Heartbeat: continuous from deploy, survives everything, stops on close
  h = mkNode(
    numCfg({ heartbeatinterval: "150", heartbeatintervalunits: "Millisecond" }),
  );
  h.drain();
  await sleep(220);
  b = h.drain();
  const hb = findBatch(b, "query");
  check(
    "heartbeat ticks before any reading (continuous from deploy)",
    hb !== null && hb[2] !== null && hb[2].source === "internal",
  );
  checkEq(
    "pre-data heartbeat snapshot shows waiting",
    hb[2].consensusState,
    "waiting",
  );
  h.input({ topic: "s1", payload: 70 });
  h.drain();
  await sleep(220);
  b = h.drain();
  check(
    "heartbeat keeps ticking after readings",
    findEvent(b, "query") !== null,
  );
  await h.close(false);
  await sleep(220);
  checkEq("heartbeat stops on close", h.drain().length, 0);
}

// ---------------------------------------------------------------------------
// Suite 8: Persistence (async)
// ---------------------------------------------------------------------------

async function suite8() {
  suite("Suite 8: Persistence");

  const pCfg = numCfg({
    persist: true,
    quorum: "2",
    stalewindow: "400",
    stalewindowunits: "Millisecond",
    triggervalue: "60",
    releasevalue: "55",
  });

  // Populate + latch + override, then quick restore
  const id = "persist-a";
  const h = mkNode(Object.assign({}, pCfg, { id: id }));
  h.drain();
  h.input({ topic: "s1", payload: 50 });
  h.input({ topic: "s2", payload: 80 }); // mean 65 -> triggered
  h.input({ payload: "settrigger", settrigger: 62 });
  h.drain();
  check("state file written", fs.existsSync(stateFileFor(id)));
  await h.close(false);

  const h2 = mkNode(Object.assign({}, pCfg, { id: id }));
  let b = h2.drain();
  checkEq("quick restore is event-silent", b.length, 0);
  h2.input({ payload: "query" });
  const q = h2.drain()[0][2];
  checkEq("latch restored", q.consensusState, "triggered");
  checkEq("aggregate recomputed from restored values", q.aggregate, 65);
  checkEq(
    "source roster restored",
    Object.keys(q.sources).sort().join(","),
    "s1,s2",
  );
  checkEq("restored quorum satisfied", q.quorum, true);
  // Discriminating override-survival: release, then land between config
  // trigger (60) and persisted override (62)
  h2.input({ topic: "s2", payload: 20 }); // mean 35 < 55: releases
  h2.drain();
  h2.input({ topic: "s2", payload: 73 }); // mean 61.5: >= config 60, < override 62
  b = h2.drain();
  check(
    "settrigger override survives restart (61.5 vs 62 must not trigger)",
    findEvent(b, "triggered") === null,
  );
  h2.input({ topic: "s2", payload: 75 }); // mean 62.5 >= 62
  b = h2.drain();
  check("...and 62.5 vs 62 triggers", findEvent(b, "triggered") !== null);
  await h2.close(false);

  // Outage restore: wait past the stale window before reviving
  await sleep(550);
  const h3 = mkNode(Object.assign({}, pCfg, { id: id }));
  b = h3.drain();
  const staleEvts = eventsOf(b).filter(function (m) {
    return m.consensusEvent === "sourcestale";
  });
  checkEq("outage restore stales both sources", staleEvts.length, 2);
  checkEq(
    "restore sourcestale is internally sourced",
    staleEvts[0].source,
    "internal",
  );
  checkEq(
    "regression: restore sourcestale shows pre-update quorum:true",
    staleEvts[0].quorum,
    true,
  );
  check(
    "outage restore announces quorumlost",
    findEvent(b, "quorumlost") !== null,
  );
  check(
    "restore is transition-silent (no trigger/release)",
    findEvent(b, "triggered") === null && findEvent(b, "released") === null,
  );
  const s = h3.lastStatus();
  check(
    "outage restore status: yellow ring, held latch prefix intact",
    s.fill === "yellow" &&
      s.shape === "ring" &&
      s.text === "Triggered | No quorum (0/2) | --",
  );
  await h3.close(false);

  // Node removal deletes state; persist:false cleans up an existing file
  const h4 = mkNode(Object.assign({}, pCfg, { id: "persist-b" }));
  h4.input({ topic: "s1", payload: 10 });
  h4.drain();
  check(
    "persist-b state file written",
    fs.existsSync(stateFileFor("persist-b")),
  );
  await h4.close(true);
  check(
    "close(removed) deletes the state file",
    !fs.existsSync(stateFileFor("persist-b")),
  );

  const h5 = mkNode(Object.assign({}, pCfg, { id: "persist-c" }));
  h5.input({ topic: "s1", payload: 10 });
  h5.drain();
  await h5.close(false);
  check(
    "persist-c file survives non-removal close",
    fs.existsSync(stateFileFor("persist-c")),
  );
  mkNode(numCfg({ id: "persist-c", persist: false }));
  check(
    "constructing with persist:false deletes stale state file",
    !fs.existsSync(stateFileFor("persist-c")),
  );
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

(async function main() {
  try {
    suite1();
    suite2();
    suite3();
    suite4();
    suite5();
    suite6();
    await suite7();
    await suite8();
  } catch (err) {
    failCount++;
    console.error(
      "\nFAIL: harness threw: " + (err && err.stack ? err.stack : err),
    );
  }

  console.log("\n============================================");
  console.log(
    "TOTAL: " +
      (passCount + failCount) +
      " checks, " +
      passCount +
      " passed, " +
      failCount +
      " failed",
  );
  console.log("============================================");
  process.exitCode = failCount > 0 ? 1 : 0;
})();
