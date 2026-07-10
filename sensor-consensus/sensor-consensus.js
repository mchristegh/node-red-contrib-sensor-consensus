/**
 * sensor-consensus
 * A Node-RED sensor-fusion / consensus node: fuses latest readings from
 * multiple sources (keyed by msg.topic) into a single trusted aggregate,
 * with freshness (staleness windows), quorum, disagreement detection,
 * and a latched threshold trigger. Purpose-built 4-output event model:
 *   1. Trigger - fires only on a true untriggered -> triggered transition
 *   2. Release - fires only on a true triggered -> untriggered transition
 *   3. Query   - fires on an incoming query message, or a heartbeat tick
 *   4. Events  - fires for every other event, including tagged copies of
 *                ignored/blocked/suppressed actions
 *
 * Sibling of node-red-contrib-timer-events and
 * node-red-contrib-timer-threshold - shares the same event envelope
 * philosophy (ignored, source), command conventions, and output
 * exclusivity guarantees. See sensor-consensus-design.md for the
 * authoritative design record.
 *
 * Copyright (C) 2026 mchristegh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

module.exports = function(RED) {
  "use strict";

  // ---------------------------------------------------------------------------
  // Module-level constants
  // ---------------------------------------------------------------------------

  const CONSENSUS_STATE = {
    WAITING:     "waiting",
    UNTRIGGERED: "untriggered",
    TRIGGERED:   "triggered"
  };

  // Canonical event-type list. Note: `ignored` is a modifier on the real
  // event that was ATTEMPTED, never a separate event category - a rejected
  // payload is still a READING (ignored:true), a transition suppressed
  // while disabled is still TRIGGERED/RELEASED (ignored:true).
  //
  // One-message-one-event rule: a single accepted reading emits exactly
  // one reading-class event - the most specific of SOURCEADDED /
  // SOURCERECOVERED / READING - each carrying the full recomputed
  // aggregate. Transitions caused by that same reading (quorum, trigger/
  // release) are separate events, fired afterward in causal order.
  const CONSENSUS_EVENT = {
    TRIGGERED:       "triggered",
    RELEASED:        "released",
    READING:         "reading",
    SOURCEADDED:     "sourceadded",
    SOURCERECOVERED: "sourcerecovered",
    SOURCESTALE:     "sourcestale",
    SOURCEREMOVED:   "sourceremoved",
    QUORUMLOST:      "quorumlost",
    QUORUMREGAINED:  "quorumregained",
    MINORITYREPORT:  "minorityreport",
    DISABLED:        "disabled",
    ENABLED:         "enabled",
    RESET:           "reset",
    TRIGGERSET:      "triggerset",
    RELEASESET:      "releaseset",
    STALESET:        "staleset",
    QUERY:           "query"
  };

  const EVENT_SOURCE = {
    EXTERNAL: "external",
    INTERNAL: "internal"
  };

  const TYPE_MODE = {
    NUMERIC: "numeric",
    BOOLEAN: "boolean"
  };

  const AGGREGATION = {
    MEAN:        "mean",
    MEDIAN:      "median",
    MIN:         "min",
    MAX:         "max",
    TRIMMEDMEAN: "trimmedmean",
    // Reported in the envelope when trimmed mean lacks the >= 3 fresh
    // sources it needs and silently falls back (design: never fail, never
    // go silent - name the function actually used).
    MEANFALLBACK: "mean(fallback)",
    // Boolean mode always uses fraction-of-fresh-sources-true.
    FRACTION:     "fraction"
  };

  const TRIGGER_DIRECTION = {
    ABOVE: "above",   // trigger when aggregate >= trigger value
    BELOW: "below"    // trigger when aggregate <= trigger value
  };

  // Boolean-mode vote rule presets. Each maps to a rule over the fraction
  // of FRESH sources reporting true (the denominator is freshCount, with
  // quorum ensuring "enough" freshness).
  const BOOL_RULE = {
    ANY:      "any",       // fraction > 0
    MAJORITY: "majority",  // fraction >= 0.5
    ALL:      "all",       // fraction === 1.0
    ATLEASTN: "atleastn",  // trueCount >= N
    // Release-rule-only preset: release the moment the trigger rule stops
    // being satisfied (no flap suppression).
    SAMEASTRIGGER: "sameastrigger"
  };

  const QUORUM_LOST_POLICY = {
    HOLD:    "hold",     // latch keeps last consensus until data returns
    RELEASE: "release"   // quorum loss releases immediately (fail-safe)
  };

  // Recognized command strings. Command precedence is a hard rule: a
  // recognized command in msg.payload (any case) is ALWAYS a command, in
  // both type modes, before any coercion is attempted - so a command word
  // can never function as a boolean true/false value.
  const PAYLOAD = {
    QUERY:      "query",
    DISABLE:    "disable",
    ENABLE:     "enable",
    RESET:      "reset",
    REMOVE:     "remove",
    SETTRIGGER: "settrigger",
    SETRELEASE: "setrelease",
    SETSTALE:   "setstale"
  };

  const UNITS = {
    MILLISECOND: "Millisecond",
    SECOND:      "Second",
    MINUTE:      "Minute",
    HOUR:        "Hour"
  };

  const UNITS_INPUT = {
    MILLISECOND: "millisecond",
    SECOND:      "second",
    MINUTE:      "minute",
    HOUR:        "hour"
  };

  // Boolean coercion: native true/false and numeric 1/0 always coerce;
  // everything else is matched (lowercased, trimmed) against the node's
  // configurable true/false value lists. These are the list DEFAULTS.
  const DEFAULT_TRUE_VALUES  = ["true", "on", "yes", "1"];
  const DEFAULT_FALSE_VALUES = ["false", "off", "no", "0"];

  // ---------------------------------------------------------------------------
  // Node definition
  // ---------------------------------------------------------------------------

  function SensorConsensus(n) {
    RED.nodes.createNode(this, n);
    let fs   = require('fs');
    let path = require('path');
    let nodefile = n.id.toString();
    let nodepath = "";
    require('./cycle.js');

    if (n._alias != null) {
      nodepath = n._flow.path.replace(/\//g, "-") + "-";
      nodefile = n._alias;
    }

    const stateFile = path.join(RED.settings.userDir, "sensorconsensus-state", nodepath + nodefile);

    // -------------------------------------------------------------------------
    // Node property initialization (defensive parsing throughout)
    // -------------------------------------------------------------------------

    this.typemode        = n.typemode        || TYPE_MODE.NUMERIC;
    this.aggregation     = n.aggregation     || AGGREGATION.MEAN;
    this.triggerdir      = n.triggerdir      || TRIGGER_DIRECTION.ABOVE;
    this.triggervalue    = parseConfigNumber(n.triggervalue);
    // Blank release means "same as trigger" (no hysteresis). NOTE:
    // Number("") === 0, so blank MUST be caught before Number() - a
    // blank release parsed as 0 would make an Above-mode node never
    // release. parseConfigNumber returns null for blank/invalid.
    this.releasevalue    = parseConfigNumber(n.releasevalue);
    if (this.releasevalue === null) this.releasevalue = this.triggervalue;
    // Config-time cross-validation guard. Editor-side cross-field
    // validators are unreliable in Node-RED (they see pre-edit values),
    // so an inverted configured pair is sanitized here instead: the
    // release is ignored (falls back to same-as-trigger, i.e. the
    // release-follows-trigger rule) with a single warn - rather than
    // shipping a pair that flaps Trigger/Release on every reading in
    // the band. This is the node's only node.warn(): a configuration
    // defect, not a runtime command (those surface as ignored:true
    // events per house convention).
    if (this.triggervalue !== null && this.releasevalue !== null &&
        (this.triggerdir === TRIGGER_DIRECTION.BELOW
          ? this.releasevalue < this.triggervalue
          : this.releasevalue > this.triggervalue)) {
      this.warn("Configured release value " + this.releasevalue +
                " is on the wrong side of trigger value " + this.triggervalue +
                " for direction '" + this.triggerdir +
                "' - ignoring release (same as trigger)");
      this.releasevalue = this.triggervalue;
    }
    this.boolrule        = n.boolrule        || BOOL_RULE.MAJORITY;
    this.boolrulen       = isNaN(Number(n.boolrulen)) ? 1 : Number(n.boolrulen);
    this.boolreleaserule = n.boolreleaserule || BOOL_RULE.SAMEASTRIGGER;
    this.boolreleasen    = isNaN(Number(n.boolreleasen)) ? 1 : Number(n.boolreleasen);
    this.truevalues      = parseValueList(n.truevalues,  DEFAULT_TRUE_VALUES);
    this.falsevalues     = parseValueList(n.falsevalues, DEFAULT_FALSE_VALUES);
    this.expectedsources = parseTopicList(n.expectedsources);
    this.quorum          = isNaN(Number(n.quorum)) ? 0 : Number(n.quorum);
    this.stalewindow     = isNaN(Number(n.stalewindow)) ? 0 : Number(n.stalewindow);
    this.stalewindowunits = n.stalewindowunits || UNITS.SECOND;
    this.quorumlostpolicy = n.quorumlostpolicy || QUORUM_LOST_POLICY.HOLD;
    this.emitonchange    = n.emitonchange !== false;   // default CHECKED
    this.heartbeatinterval      = isNaN(Number(n.heartbeatinterval)) ? 0 : Number(n.heartbeatinterval);
    this.heartbeatintervalunits = n.heartbeatintervalunits || UNITS.SECOND;
    this.persist         = n.persist || false;

    let node = this;

    // -------------------------------------------------------------------------
    // Runtime state variables
    // -------------------------------------------------------------------------

    // Source registry: topic -> { value:        coerced latest value (number|boolean|null),
    //                             lastSeen:     Date or null (null = expected, never seen),
    //                             stale:        boolean,
    //                             seen:         boolean }
    // Expected sources are pre-registered at construction with seen:false.
    let sources = {};

    let consensusState  = CONSENSUS_STATE.WAITING;
    let disabled        = false;
    let quorumOK        = null;    // null until first computed; then boolean
    let aggregate       = null;    // current aggregate value, null while no data
    let aggregationUsed = null;    // AGGREGATION.* actually applied (fallback-aware)
    let minorityTopics  = [];      // boolean mode: current disagreeing fresh topics
    let boolTrueCount   = 0;       // boolean mode: fresh sources currently reporting true
    let lastReportedMinority = []; // last minority set surfaced via MINORITYREPORT
                                   // (report fires on set CHANGE, not per reading)

    // Runtime overrides (settrigger / setrelease / setstale). null = use config.
    let overrideTrigger = null;
    let overrideRelease = null;
    let overrideStaleMS = null;

    // Baseline message lineage (the originalMsg analog): the most recent
    // ACCEPTED reading's msg. Internally-sourced events (staleness expiry,
    // heartbeat, internally-caused quorum/trigger changes, restore) clone
    // this as their payload base. Commands clone their own triggering msg.
    let baselineMsg = null;

    // Independent timer handles, per house convention - the staleness
    // clock and the heartbeat never share handles and are cleared only by
    // their own stop functions.
    //
    // Staleness clock design: NOT a polling interval. One setTimeout armed
    // to the single earliest upcoming source expiry; re-armed after every
    // accepted reading, setstale, remove, reset, and each expiry it
    // processes. Zero timers when staleness is disabled or nothing fresh.
    let stalenessTimeout = null;
    let heartbeatTimer   = null;

    const maxTimeout = 2147483647;

    // -------------------------------------------------------------------------
    // Config-parsing helpers (module-scope-safe, used above)
    // -------------------------------------------------------------------------

    /**
     * Parses an optional numeric config field. Blank, missing, or
     * non-numeric input returns null ("not set") - critically, blank is
     * caught BEFORE Number(), because Number("") === 0 and a phantom
     * zero silently corrupts threshold semantics.
     */
    function parseConfigNumber(raw) {
      if (raw === undefined || raw === null || String(raw).trim() === "") return null;
      let v = Number(raw);
      return isNaN(v) ? null : v;
    }

    function parseValueList(raw, defaults) {
      if (typeof raw !== 'string' || raw.trim() === "") return defaults.slice();
      return raw.split(",").map(function(v) { return v.trim().toLowerCase(); })
                           .filter(function(v) { return v !== ""; });
    }

    function parseTopicList(raw) {
      if (typeof raw !== 'string' || raw.trim() === "") return [];
      return raw.split(",").map(function(v) { return v.trim(); })
                           .filter(function(v) { return v !== ""; });
    }

    // -------------------------------------------------------------------------
    // Expected-source pre-registration
    // -------------------------------------------------------------------------

    node.expectedsources.forEach(function(topic) {
      sources[topic] = { value: null, lastSeen: null, stale: false, seen: false };
    });

    // -------------------------------------------------------------------------
    // Persist restore
    // -------------------------------------------------------------------------

    if (this.persist === true) {
      try {
        if (fs.existsSync(stateFile)) {
          let saved = JSON.retrocycle(JSON.parse(readState()));

          if (saved.sources && typeof saved.sources === 'object') {
            Object.keys(saved.sources).forEach(function(t) {
              let s = saved.sources[t];
              sources[t] = {
                value:    s.value,
                lastSeen: s.lastSeen ? new Date(s.lastSeen) : null,
                stale:    false,   // deliberately not persisted - recomputed
                                   // against wall-clock time just below
                seen:     s.seen === true
              };
            });
          }
          if (typeof saved.consensusState === 'string') consensusState = saved.consensusState;
          if (typeof saved.disabled === 'boolean')      disabled       = saved.disabled;
          if (saved.overrideTrigger !== undefined && saved.overrideTrigger !== null) overrideTrigger = saved.overrideTrigger;
          if (saved.overrideRelease !== undefined && saved.overrideRelease !== null) overrideRelease = saved.overrideRelease;
          if (saved.overrideStaleMS !== undefined && saved.overrideStaleMS !== null) overrideStaleMS = saved.overrideStaleMS;
          if (saved.baselineMsg && typeof saved.baselineMsg === 'object' &&
              Object.keys(saved.baselineMsg).length > 0) {
            baselineMsg = saved.baselineMsg;
          }

          // Wall-clock staleness pass: a long outage restores into "all
          // sources stale, quorum lost" rather than pretending old
          // readings are fresh. Mark + recompute first, then dispatch,
          // same ordering as onStalenessExpiry.
          let staleMS = effectiveStaleMS();
          let nowMS   = (new Date()).getTime();
          let staled  = [];
          if (staleMS > 0) {
            Object.keys(sources).forEach(function(t) {
              let s = sources[t];
              if (s.seen && s.lastSeen !== null && (nowMS - s.lastSeen.getTime()) >= staleMS) {
                s.stale = true;
                staled.push(t);
              }
            });
          }
          computeAggregate();

          // Quorum is seeded from the PRE-staleness roster (what it
          // presumably was before shutdown) BEFORE the sourcestale
          // dispatches, so restore envelopes match the live expiry path's
          // documented nuance: sourcestale shows the pre-update quorum,
          // and the quorumlost that follows is the correction. (Regaining
          // on restore is unreachable - fresh <= seen - the two-way
          // dispatch below is defensive symmetry only.)
          let seenCount = Object.keys(sources).filter(function(t) { return sources[t].seen; }).length;
          quorumOK      = node.quorum <= 0 ? true : seenCount >= node.quorum;

          staled.forEach(function(t) {
            dispatchEvent(CONSENSUS_EVENT.SOURCESTALE, baselineMsg, false,
                          EVENT_SOURCE.INTERNAL, { staleTopic: t });
          });

          let newQuorum = node.quorum <= 0 ? true : freshTopics().length >= node.quorum;
          if (newQuorum !== quorumOK) {
            quorumOK = newQuorum;
            dispatchEvent(newQuorum ? CONSENSUS_EVENT.QUORUMREGAINED : CONSENSUS_EVENT.QUORUMLOST,
                          baselineMsg, false, EVENT_SOURCE.INTERNAL);
          }

          // The latch restores silently - restore NEVER fires Trigger or
          // Release by itself; the first genuine transition comes from
          // live evaluation (next reading, staleness tick, or command).
          // Known edge, deliberate: a RELEASE-policy node restoring into
          // quorum-lost stays latched until that first live evaluation.
          armStalenessClock();
        }
      } catch (error) {
        node.error("Error processing persistent file data for sensor-consensus node " + n.id.toString() + "\n\n" + error.toString());
      }
    } else {
      deleteState();
    }

    // Heartbeat runs continuously from deploy (deviation from the timer
    // siblings, where it is scoped to a run): this is a monitoring node,
    // and its idle problems - waiting, stale sources, lost quorum - are
    // exactly what a watchdog needs visibility into.
    startHeartbeat();
    node.status(buildStatus());

    // -------------------------------------------------------------------------
    // Event listeners
    // -------------------------------------------------------------------------

    this.on("input", function(msg) {
      handleInputEvent(msg, false);
    });

    this.on("close", function(removed, done) {
      stopStalenessClock();
      stopHeartbeat();
      node.status({});
      if (removed) deleteState();
      done();
    });

    // -------------------------------------------------------------------------
    // Status helper
    // -------------------------------------------------------------------------

    /**
     * Status label per the design doc's table. Purely cosmetic - never
     * produces an output message.
     *   waiting            grey dot    "Waiting (fresh/quorum: F/Q)"
     *   untriggered, OK    green dot   "<aggregate> (F/S fresh)"
     *   triggered          blue dot    "Triggered: <aggregate> (F/S fresh)"
     *   quorum lost        yellow ring "No quorum (F/Q) | <last aggregate>"
     *                                  (+ "Triggered | " prefix if latched
     *                                   under the Hold policy)
     *   disabled           grey ring   "Disabled | " + normal text
     */
    function buildStatus() {
      let fresh = freshTopics().length;
      let known = Object.keys(sources).length;
      let q     = quorumOK === null ? (node.quorum <= 0) : quorumOK;
      let fill, shape, text;

      if (consensusState === CONSENSUS_STATE.WAITING) {
        fill  = "grey";
        shape = "dot";
        // Small pragmatic deviation from the design table: with quorum
        // disabled there is no meaningful F/Q pair to show, so just the
        // fresh count.
        text  = node.quorum > 0
          ? "Waiting (fresh/quorum: " + fresh + "/" + node.quorum + ")"
          : "Waiting (" + fresh + " fresh)";
      } else if (!q) {
        fill  = "yellow";
        shape = "ring";
        text  = "No quorum (" + fresh + "/" + node.quorum + ") | " + renderAggregateStatus();
        if (consensusState === CONSENSUS_STATE.TRIGGERED) text = "Triggered | " + text;
      } else if (consensusState === CONSENSUS_STATE.TRIGGERED) {
        fill  = "blue";
        shape = "dot";
        text  = "Triggered: " + renderAggregateStatus() + " (" + fresh + "/" + known + " fresh)";
      } else {
        fill  = "green";
        shape = "dot";
        text  = renderAggregateStatus() + " (" + fresh + "/" + known + " fresh)";
      }

      if (disabled) {
        return { fill: "grey", shape: "ring", text: "Disabled | " + text };
      }
      return { fill: fill, shape: shape, text: text };
    }

    /**
     * Renders the aggregate for the status label: numeric at a sensible
     * fixed precision, boolean mode as true/false counts over the fresh
     * set, "--" when there is no aggregate.
     */
    function renderAggregateStatus() {
      if (aggregate === null) return "--";
      if (node.typemode === TYPE_MODE.BOOLEAN) {
        let fresh = freshTopics().length;
        return boolTrueCount + "T/" + (fresh - boolTrueCount) + "F of " + fresh;
      }
      return Number.isInteger(aggregate) ? String(aggregate) : aggregate.toFixed(2);
    }

    // -------------------------------------------------------------------------
    // Coercion helpers
    // -------------------------------------------------------------------------

    /**
     * Numeric coercion: Number(payload); NaN rejects. Strings that parse
     * ("21.5") are accepted.
     * Returns { ok: boolean, value: number|undefined }.
     */
    function coerceNumeric(payload) {
      let v = Number(payload);
      return isNaN(v) ? { ok: false } : { ok: true, value: v };
    }

    /**
     * Boolean coercion per the design doc's table: native booleans and
     * numeric 1/0 always coerce; strings are lowercased/trimmed and
     * matched against the configurable lists; anything else rejects.
     * Returns { ok: boolean, value: boolean|undefined }.
     */
    function coerceBoolean(payload) {
      if (payload === true  || payload === 1) return { ok: true, value: true };
      if (payload === false || payload === 0) return { ok: true, value: false };
      if (typeof payload === 'string') {
        let s = payload.trim().toLowerCase();
        if (node.truevalues.indexOf(s)  !== -1) return { ok: true, value: true };
        if (node.falsevalues.indexOf(s) !== -1) return { ok: true, value: false };
      }
      return { ok: false };
    }

    /**
     * Parses a boolean-mode threshold override from a settrigger/
     * setrelease command value: a fraction (a number, or numeric string,
     * in 0..1) -> { fraction }, or a preset name -> { rule } / { rule, n }.
     * ATLEASTN takes its count from the companion property
     * (msg.settriggern / msg.setreleasen). SAMEASTRIGGER is accepted only
     * where allowSameAsTrigger is true (setrelease). Returns null when
     * unparseable - the caller surfaces that as ignored:true.
     */
    function parseBoolOverride(value, nValue, allowSameAsTrigger) {
      if (typeof value === 'string') {
        let s = value.trim().toLowerCase();
        if (s === BOOL_RULE.ANY || s === BOOL_RULE.MAJORITY || s === BOOL_RULE.ALL) return { rule: s };
        if (s === BOOL_RULE.ATLEASTN) {
          let cnt = Number(nValue);
          return (!isNaN(cnt) && cnt >= 1) ? { rule: BOOL_RULE.ATLEASTN, n: cnt } : null;
        }
        if (allowSameAsTrigger && s === BOOL_RULE.SAMEASTRIGGER) return { rule: BOOL_RULE.SAMEASTRIGGER };
      }
      let f = Number(value);
      if (!isNaN(f) && f >= 0 && f <= 1) return { fraction: f };
      return null;
    }

    // -------------------------------------------------------------------------
    // Utility helpers
    // -------------------------------------------------------------------------

    function convertToMilliseconds(value, units) {
      switch (units) {
        case UNITS.SECOND:      return value * 1000;
        case UNITS.MINUTE:      return value * 1000 * 60;
        case UNITS.HOUR:        return value * 1000 * 60 * 60;
        case UNITS.MILLISECOND: return value;
        default:                return value;
      }
    }

    function normalizeUnits(units) {
      return typeof units === 'string' ? units.toLowerCase().replace(/s$/, '') : null;
    }

    function msgValueToMs(value, units) {
      switch (units) {
        case UNITS_INPUT.SECOND: return value * 1000;
        case UNITS_INPUT.MINUTE: return value * 1000 * 60;
        case UNITS_INPUT.HOUR:   return value * 1000 * 60 * 60;
        default:                 return value;
      }
    }

    function effectiveStaleMS() {
      return overrideStaleMS !== null
        ? overrideStaleMS
        : convertToMilliseconds(node.stalewindow, node.stalewindowunits);
    }

    function freshTopics() {
      return Object.keys(sources).filter(function(t) {
        return sources[t].seen && !sources[t].stale;
      });
    }

    /**
     * Builds the per-source breakdown for the envelope:
     * { <topic>: { value, lastSeen (ISO or null), stale, seen } }
     */
    function sourcesSnapshot() {
      let snap = {};
      Object.keys(sources).forEach(function(t) {
        snap[t] = {
          value:    sources[t].value,
          lastSeen: sources[t].lastSeen ? sources[t].lastSeen.toISOString() : null,
          stale:    sources[t].stale,
          seen:     sources[t].seen
        };
      });
      return snap;
    }

    // -------------------------------------------------------------------------
    // Event message construction + output dispatch
    // -------------------------------------------------------------------------

    /**
     * Builds the standard event envelope by cloning a base message (the
     * live triggering msg, or baselineMsg for internally-sourced events)
     * and layering the standard state/metadata fields on top.
     */
    function buildEventMessage(consensusEvent, baseMsg, ignored, source) {
      let evtMsg = RED.util.cloneMessage(baseMsg || {});
      evtMsg.consensusEvent = consensusEvent;
      evtMsg.consensusState = consensusState;
      evtMsg.aggregate      = aggregate;
      evtMsg.aggregation    = aggregationUsed;
      evtMsg.quorum         = quorumOK === null ? (node.quorum <= 0) : quorumOK;
      evtMsg.freshCount     = freshTopics().length;
      evtMsg.sourceCount    = Object.keys(sources).length;
      evtMsg.sources        = sourcesSnapshot();
      evtMsg.disabled       = disabled;
      evtMsg.ignored        = ignored;
      evtMsg.source         = source;
      return evtMsg;
    }

    /**
     * Central output router for every event. Applies the fixed
     * output-exclusivity rules:
     *   - Output 1 (Trigger): CONSENSUS_EVENT.TRIGGERED only, and only
     *                         when ignored is false. A true trigger always
     *                         also fires on output 4.
     *   - Output 2 (Release): CONSENSUS_EVENT.RELEASED only, and only
     *                         when ignored is false. Always also fires on
     *                         output 4.
     *   - Output 3 (Query):   CONSENSUS_EVENT.QUERY only. Never fires on
     *                         output 4.
     *   - Output 4 (Events):  every event except QUERY, including ignored
     *                         copies of what would otherwise be output 1/2
     *                         events.
     *
     * extraProps layers event-specific fields (releaseReason,
     * rejectedValue, staleTopic, recoveredTopic, addedTopic, removedTopic,
     * minorityTopics, triggerSet, releaseSet, staleSet) onto the message.
     */
    function dispatchEvent(consensusEvent, baseMsg, ignored, source, extraProps) {
      let evtMsg = buildEventMessage(consensusEvent, baseMsg, ignored, source);
      if (extraProps) {
        for (let key in extraProps) {
          if (Object.prototype.hasOwnProperty.call(extraProps, key)) evtMsg[key] = extraProps[key];
        }
      }

      if (consensusEvent === CONSENSUS_EVENT.QUERY) {
        node.send([null, null, evtMsg, null]);
        return;
      }

      let out1 = null;
      let out2 = null;
      let out4 = evtMsg;

      if (!ignored) {
        if (consensusEvent === CONSENSUS_EVENT.TRIGGERED) {
          out1 = RED.util.cloneMessage(evtMsg);
        } else if (consensusEvent === CONSENSUS_EVENT.RELEASED) {
          out2 = RED.util.cloneMessage(evtMsg);
        }
      }

      node.send([out1, out2, null, out4]);
    }

    // -------------------------------------------------------------------------
    // Aggregation
    // -------------------------------------------------------------------------

    /**
     * Recomputes `aggregate`, `aggregationUsed`, and (boolean mode)
     * `minorityTopics` from the fresh sources' latest values.
     *
     * Numeric mode: configured function over fresh values. Trimmed mean
     * requires >= 3 fresh sources; below that it silently falls back to
     * mean and reports AGGREGATION.MEANFALLBACK - never fails, never goes
     * silent.
     *
     * Boolean mode: aggregate = fraction of fresh sources reporting true
     * (0.0-1.0). minorityTopics = fresh topics disagreeing with the
     * consensus value when one exists (fraction not exactly 0.5).
     *
     * With zero fresh sources: aggregate = null, aggregationUsed = null.
     */
    function computeAggregate() {
      let freshT = freshTopics();
      let values = freshT.map(function(t) { return sources[t].value; });

      if (values.length === 0) {
        aggregate       = null;
        aggregationUsed = null;
        boolTrueCount   = 0;
        minorityTopics  = [];
        return;
      }

      if (node.typemode === TYPE_MODE.BOOLEAN) {
        boolTrueCount   = values.filter(function(v) { return v === true; }).length;
        aggregate       = boolTrueCount / values.length;
        aggregationUsed = AGGREGATION.FRACTION;
        // A consensus value exists only when the fraction is not exactly
        // split; the minority is every fresh source disagreeing with it.
        let consensusValue = aggregate > 0.5 ? true : (aggregate < 0.5 ? false : null);
        minorityTopics = consensusValue === null ? [] : freshT.filter(function(t) {
          return sources[t].value !== consensusValue;
        });
        return;
      }

      boolTrueCount  = 0;
      minorityTopics = [];
      let sorted = values.slice().sort(function(a, b) { return a - b; });
      let mean   = function(arr) {
        return arr.reduce(function(a, b) { return a + b; }, 0) / arr.length;
      };

      switch (node.aggregation) {
        case AGGREGATION.MEDIAN: {
          let mid = Math.floor(sorted.length / 2);
          aggregate = (sorted.length % 2 === 1)
            ? sorted[mid]
            : (sorted[mid - 1] + sorted[mid]) / 2;
          aggregationUsed = AGGREGATION.MEDIAN;
          break;
        }
        case AGGREGATION.MIN:
          aggregate       = sorted[0];
          aggregationUsed = AGGREGATION.MIN;
          break;
        case AGGREGATION.MAX:
          aggregate       = sorted[sorted.length - 1];
          aggregationUsed = AGGREGATION.MAX;
          break;
        case AGGREGATION.TRIMMEDMEAN:
          if (sorted.length >= 3) {
            // Drops the single highest and single lowest fresh value.
            aggregate       = mean(sorted.slice(1, sorted.length - 1));
            aggregationUsed = AGGREGATION.TRIMMEDMEAN;
          } else {
            // Fewer than 3 fresh sources: silent fallback to mean, named
            // in the envelope - never fails, never goes silent.
            aggregate       = mean(sorted);
            aggregationUsed = AGGREGATION.MEANFALLBACK;
          }
          break;
        case AGGREGATION.MEAN:
        default:
          aggregate       = mean(sorted);
          aggregationUsed = AGGREGATION.MEAN;
          break;
      }
    }

    // -------------------------------------------------------------------------
    // Evaluation (quorum + trigger/release)
    // -------------------------------------------------------------------------

    /**
     * The decision core, run after every recompute (accepted reading,
     * staleness expiry, remove, setstale/settrigger/setrelease, enable,
     * restore's staleness pass). Sequence - each step fires its events in
     * causal order AFTER the reading-class event has been dispatched by
     * the caller:
     *
     *   1. Quorum check: freshCount vs node.quorum (0 = disabled, always
     *      satisfied). On a boundary crossing, dispatch QUORUMLOST /
     *      QUORUMREGAINED (source = whatever caused the recompute).
     *      On loss while latched: HOLD policy freezes the latch
     *      (state stays TRIGGERED, quorum:false); RELEASE policy releases
     *      immediately - a genuine RELEASED (output 2 + 4) with
     *      releaseReason:"quorumlost".
     *   2. If below quorum or aggregate is null: evaluation suspended -
     *      return (the aggregate itself was still reported by the caller).
     *   3. First-ever successful evaluation: WAITING -> UNTRIGGERED
     *      silently (entering evaluation is not itself an event), then
     *      continue to step 4.
     *   4. Threshold evaluation against the effective trigger/release
     *      values (overrides first, then config; boolean presets mapped
     *      to fraction rules):
     *        UNTRIGGERED + trigger condition met   -> TRIGGERED
     *        TRIGGERED   + release condition met   -> RELEASED
     *                                                 (releaseReason:"threshold")
     *      While disabled, a warranted transition is SUPPRESSED: state
     *      does not change; dispatch the event with ignored:true on
     *      output 4 only. On `enable`, this function runs again and the
     *      genuine transition fires then (source:"external" - the enable
     *      command is its live trigger).
     *   5. Boolean mode: if minorityTopics CHANGED (set comparison, not
     *      per-reading), dispatch MINORITYREPORT with the new set - a
     *      persistently stuck sensor produces one report, not a stream.
     *
     * baseMsg/source: the message and source of whatever caused the
     * recompute; internally-caused paths pass baselineMsg + INTERNAL.
     */
    function evaluate(baseMsg, source) {
      // 1. Quorum boundary.
      let freshCount = freshTopics().length;
      let newQuorum  = node.quorum <= 0 ? true : freshCount >= node.quorum;

      if (quorumOK === null) {
        // First computation: adopt silently - you can't lose (or regain)
        // what you never had.
        quorumOK = newQuorum;
      } else if (newQuorum !== quorumOK) {
        quorumOK = newQuorum;
        dispatchEvent(newQuorum ? CONSENSUS_EVENT.QUORUMREGAINED : CONSENSUS_EVENT.QUORUMLOST,
                      baseMsg, false, source);
      }

      // 2. Below quorum (or no data): evaluation suspended. Under the
      //    Release policy a latched node releases here - checked on EVERY
      //    pass rather than only on the boundary crossing, so a release
      //    suppressed while disabled fires genuinely on enable.
      if (!quorumOK || aggregate === null) {
        if (consensusState === CONSENSUS_STATE.TRIGGERED &&
            node.quorumlostpolicy === QUORUM_LOST_POLICY.RELEASE) {
          attemptTransition(CONSENSUS_EVENT.RELEASED, baseMsg, source,
                            { releaseReason: "quorumlost" });
        }
        return;
      }

      // 3. First successful evaluation: WAITING -> UNTRIGGERED silently
      //    (entering evaluation is not itself an event).
      if (consensusState === CONSENSUS_STATE.WAITING) {
        consensusState = CONSENSUS_STATE.UNTRIGGERED;
      }

      // 4. Threshold evaluation. Only the relevant side of the latch is
      //    checked - the latch itself is what prevents re-fires.
      if (consensusState === CONSENSUS_STATE.UNTRIGGERED && triggerConditionMet()) {
        attemptTransition(CONSENSUS_EVENT.TRIGGERED, baseMsg, source);
      } else if (consensusState === CONSENSUS_STATE.TRIGGERED && releaseConditionMet()) {
        attemptTransition(CONSENSUS_EVENT.RELEASED, baseMsg, source,
                          { releaseReason: "threshold" });
      }

      // 5. Boolean-mode minority report: fires when the disagreeing set
      //    CHANGES (including shrinking back to empty = resolution), not
      //    on every reading - a stuck sensor produces one report, not a
      //    stream. Only reachable with quorum satisfied, per design.
      if (node.typemode === TYPE_MODE.BOOLEAN) {
        let current = minorityTopics.slice().sort().join("\u0000");
        let last    = lastReportedMinority.slice().sort().join("\u0000");
        if (current !== last) {
          lastReportedMinority = minorityTopics.slice();
          dispatchEvent(CONSENSUS_EVENT.MINORITYREPORT, baseMsg, false, source,
                        { minorityTopics: minorityTopics.slice() });
        }
      }
    }

    /**
     * Boolean-mode vote rule evaluation over the current fresh set.
     */
    function boolRuleMet(rule, nValue) {
      let freshCount = freshTopics().length;
      if (freshCount === 0) return false;
      let fraction = boolTrueCount / freshCount;
      switch (rule) {
        case BOOL_RULE.ANY:      return boolTrueCount > 0;
        case BOOL_RULE.MAJORITY: return fraction >= 0.5;
        case BOOL_RULE.ALL:      return boolTrueCount === freshCount;
        case BOOL_RULE.ATLEASTN: return boolTrueCount >= nValue;
        default:                 return false;
      }
    }

    /**
     * Numeric-mode effective trigger (runtime override first, then
     * config).
     */
    function effectiveTrigger() {
      return overrideTrigger !== null ? overrideTrigger : node.triggervalue;
    }

    /**
     * Numeric-mode DISTINCT release value, or null when none exists -
     * i.e. no runtime release override, and the configured release
     * equals the configured trigger (the no-hysteresis default).
     */
    function distinctRelease() {
      if (overrideRelease !== null) return overrideRelease;
      if (node.releasevalue !== null && node.releasevalue !== node.triggervalue) return node.releasevalue;
      return null;
    }

    /**
     * Numeric-mode effective release. Release-follows-trigger rule: when
     * no distinct release exists, the effective release IS the effective
     * trigger, dynamically - the numeric analog of boolean-mode
     * SAMEASTRIGGER - so settrigger moves both thresholds together and
     * the no-hysteresis case can never self-invalidate.
     */
    function effectiveRelease() {
      let r = distinctRelease();
      return r !== null ? r : effectiveTrigger();
    }

    /**
     * Trigger condition against the effective trigger (runtime override
     * first, then config). Boolean-mode overrides may be { fraction } or
     * { rule, n } - both shapes are set by the settrigger command.
     */
    function triggerConditionMet() {
      if (aggregate === null) return false;
      if (node.typemode === TYPE_MODE.BOOLEAN) {
        if (overrideTrigger !== null) {
          if (typeof overrideTrigger.fraction === 'number') return aggregate >= overrideTrigger.fraction;
          return boolRuleMet(overrideTrigger.rule, overrideTrigger.n);
        }
        return boolRuleMet(node.boolrule, node.boolrulen);
      }
      let t = effectiveTrigger();
      if (t === null) return false;
      return node.triggerdir === TRIGGER_DIRECTION.BELOW ? aggregate <= t : aggregate >= t;
    }

    /**
     * Release condition. Numeric mode uses STRICT inequality past the
     * release value (Above: aggregate < R; Below: aggregate > R) so that
     * with no hysteresis configured (R === trigger) an aggregate sitting
     * exactly on the threshold stays latched instead of flapping
     * trigger/release on every reading at that value. Boolean mode
     * releases when the effective release rule stops being satisfied;
     * SAMEASTRIGGER releases the moment the trigger rule itself fails.
     */
    function releaseConditionMet() {
      if (aggregate === null) return false;
      if (node.typemode === TYPE_MODE.BOOLEAN) {
        if (overrideRelease !== null) {
          if (typeof overrideRelease.fraction === 'number') return aggregate < overrideRelease.fraction;
          if (overrideRelease.rule === BOOL_RULE.SAMEASTRIGGER) return !triggerConditionMet();
          return !boolRuleMet(overrideRelease.rule, overrideRelease.n);
        }
        if (node.boolreleaserule === BOOL_RULE.SAMEASTRIGGER) return !triggerConditionMet();
        return !boolRuleMet(node.boolreleaserule, node.boolreleasen);
      }
      let r = effectiveRelease();
      if (r === null) return false;
      return node.triggerdir === TRIGGER_DIRECTION.BELOW ? aggregate > r : aggregate < r;
    }

    /**
     * Attempts a latch transition, honoring the disable-suppresses-
     * decisions rule: while disabled, state does NOT change and the
     * would-be event is dispatched ignored:true on output 4 only. The
     * suppressed transition fires genuinely when enable re-runs
     * evaluate().
     */
    function attemptTransition(targetEvent, baseMsg, source, extraProps) {
      if (disabled) {
        dispatchEvent(targetEvent, baseMsg, true, source, extraProps);
        return;
      }
      consensusState = (targetEvent === CONSENSUS_EVENT.TRIGGERED)
        ? CONSENSUS_STATE.TRIGGERED
        : CONSENSUS_STATE.UNTRIGGERED;
      dispatchEvent(targetEvent, baseMsg, false, source, extraProps);
    }

    // -------------------------------------------------------------------------
    // Staleness clock
    // -------------------------------------------------------------------------

    /**
     * Arms one setTimeout for the EARLIEST upcoming source expiry
     * (lastSeen + effectiveStaleMS), clearing any previous arm. No-op
     * (and clears) when staleness is disabled or no fresh sources exist.
     * Expiries beyond the 32-bit setTimeout ceiling are chained via
     * maxTimeout hops, same pattern as the timer siblings.
     */
    function armStalenessClock() {
      stopStalenessClock();
      let staleMS = effectiveStaleMS();
      if (staleMS <= 0) return;

      let earliest = null;
      Object.keys(sources).forEach(function(t) {
        let s = sources[t];
        if (s.seen && !s.stale && s.lastSeen !== null) {
          if (earliest === null || s.lastSeen.getTime() < earliest) earliest = s.lastSeen.getTime();
        }
      });
      if (earliest === null) return;   // nothing fresh to expire

      let delay = (earliest + staleMS) - (new Date()).getTime();
      if (delay < 0) delay = 0;
      // Chaining beyond the 32-bit setTimeout ceiling falls out of the
      // re-arm design for free: cap the hop at maxTimeout, and an early
      // wake that finds nothing expired simply re-arms for the remainder.
      if (delay > maxTimeout) delay = maxTimeout;
      stalenessTimeout = setTimeout(onStalenessExpiry, delay);
    }

    /**
     * Fires when the earliest expiry lands: marks every source whose
     * lastSeen has aged past the window as stale (there may be several if
     * expiries coincide), dispatching SOURCESTALE (source:"internal",
     * staleTopic, cloned from baselineMsg) per source, then recomputes,
     * evaluates, persists, updates status, and re-arms for the next
     * earliest expiry.
     */
    function onStalenessExpiry() {
      stalenessTimeout = null;
      let staleMS = effectiveStaleMS();
      if (staleMS <= 0) return;

      let now = (new Date()).getTime();
      let newlyStale = Object.keys(sources).filter(function(t) {
        let s = sources[t];
        return s.seen && !s.stale && s.lastSeen !== null &&
               (now - s.lastSeen.getTime()) >= staleMS;
      });

      if (newlyStale.length === 0) {
        // Early wake (a maxTimeout hop, or timer skew): just re-arm for
        // the remainder.
        armStalenessClock();
        return;
      }

      // Mark + recompute FIRST so every dispatched envelope reflects the
      // post-expiry world; then one SOURCESTALE per expired source; then
      // one evaluation pass for the lot.
      newlyStale.forEach(function(t) { sources[t].stale = true; });
      computeAggregate();
      newlyStale.forEach(function(t) {
        dispatchEvent(CONSENSUS_EVENT.SOURCESTALE, baselineMsg, false,
                      EVENT_SOURCE.INTERNAL, { staleTopic: t });
      });
      evaluate(baselineMsg, EVENT_SOURCE.INTERNAL);
      writeState();
      node.status(buildStatus());
      armStalenessClock();
    }

    function stopStalenessClock() {
      if (stalenessTimeout) {
        clearTimeout(stalenessTimeout);
        stalenessTimeout = null;
      }
    }

    // -------------------------------------------------------------------------
    // Heartbeat
    // -------------------------------------------------------------------------

    /**
     * Fixed wall-clock setInterval producing a Query snapshot
     * (output 3, source:"internal"). Continuous from deploy; independent
     * of every command and evaluation; restarted fresh after a restore.
     */
    function startHeartbeat() {
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
        heartbeatTimer = null;
      }
      if (node.heartbeatinterval > 0) {
        let intervalMS = convertToMilliseconds(node.heartbeatinterval, node.heartbeatintervalunits);
        if (intervalMS > 0) {
          heartbeatTimer = setInterval(function() {
            dispatchEvent(CONSENSUS_EVENT.QUERY, baselineMsg, false, EVENT_SOURCE.INTERNAL);
          }, intervalMS);
        }
      }
    }

    function stopHeartbeat() {
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
        heartbeatTimer = null;
      }
    }

    // -------------------------------------------------------------------------
    // Input event handler
    // -------------------------------------------------------------------------

    /**
     * Gating order per the design doc:
     *   1. Command gate  - recognized command strings always win, both modes
     *   2. Topic gate    - non-command with missing/empty topic: rejected
     *                      READING (ignored:true)
     *   3. Coercion gate - payload must coerce under the type mode:
     *                      rejected READING (ignored:true, rejectedValue)
     *   4. Accepted      - update source, recompute, dispatch the
     *                      reading-class event (SOURCEADDED /
     *                      SOURCERECOVERED / READING - one message, one
     *                      event; READING subject to emit-on-change-only,
     *                      the other two always emitted), then evaluate()
     *
     * Rejected readings ALWAYS emit regardless of emit-on-change-only -
     * they are diagnostics, and suppressing them would reintroduce silent
     * drops.
     */
    function handleInputEvent(msg, isRestore) {
      const msgPayload = typeof msg.payload === 'string' ? msg.payload.toLowerCase() : msg.payload;
      const msgSource  = isRestore ? EVENT_SOURCE.INTERNAL : EVENT_SOURCE.EXTERNAL;

      // -- Command gate ------------------------------------------------------

      if (msgPayload === PAYLOAD.QUERY) {
        dispatchEvent(CONSENSUS_EVENT.QUERY, msg, false, msgSource);
        return;
      }

      if (msgPayload === PAYLOAD.DISABLE) {
        if (disabled) {
          dispatchEvent(CONSENSUS_EVENT.DISABLED, msg, true, msgSource);
          return;
        }
        // Disable suppresses decisions, not data: readings keep
        // accumulating, staleness/quorum tracking continues, only the
        // latch is frozen (see attemptTransition / evaluate).
        disabled = true;
        writeState();
        node.status(buildStatus());
        dispatchEvent(CONSENSUS_EVENT.DISABLED, msg, false, msgSource);
        return;
      }

      if (msgPayload === PAYLOAD.ENABLE) {
        if (!disabled) {
          dispatchEvent(CONSENSUS_EVENT.ENABLED, msg, true, msgSource);
          return;
        }
        disabled = false;
        dispatchEvent(CONSENSUS_EVENT.ENABLED, msg, false, msgSource);
        // A transition suppressed while disabled fires genuinely now -
        // the enable command is its live trigger (source: external).
        evaluate(msg, msgSource);
        writeState();
        node.status(buildStatus());
        return;
      }

      if (msgPayload === PAYLOAD.RESET) {
        // Back to WAITING: learned sources, values, latch, quorum history,
        // and minority tracking all clear; expected sources stay on the
        // roster as never-seen. Runtime overrides (settrigger/setrelease/
        // setstale) and the disabled flag deliberately SURVIVE a reset -
        // reset clears data, not configuration. Always succeeds, any state.
        sources = {};
        node.expectedsources.forEach(function(topic) {
          sources[topic] = { value: null, lastSeen: null, stale: false, seen: false };
        });
        consensusState       = CONSENSUS_STATE.WAITING;
        quorumOK             = null;   // fresh start: can't lose what you never had
        lastReportedMinority = [];
        computeAggregate();
        armStalenessClock();           // clears - nothing fresh remains
        writeState();
        node.status(buildStatus());
        dispatchEvent(CONSENSUS_EVENT.RESET, msg, false, msgSource);
        return;
      }

      if (msgPayload === PAYLOAD.REMOVE) {
        let removeTopic = msg.removetopic;
        if (typeof removeTopic !== 'string' ||
            !Object.prototype.hasOwnProperty.call(sources, removeTopic)) {
          dispatchEvent(CONSENSUS_EVENT.SOURCEREMOVED, msg, true, msgSource,
                        { removedTopic: removeTopic });
          return;
        }
        delete sources[removeTopic];
        computeAggregate();
        dispatchEvent(CONSENSUS_EVENT.SOURCEREMOVED, msg, false, msgSource,
                      { removedTopic: removeTopic });
        evaluate(msg, msgSource);
        armStalenessClock();
        writeState();
        node.status(buildStatus());
        return;
      }

      if (msgPayload === PAYLOAD.SETTRIGGER) {
        let trigApplied = null;
        if (node.typemode === TYPE_MODE.BOOLEAN) {
          trigApplied = parseBoolOverride(msg.settrigger, msg.settriggern, false);
        } else {
          let v = Number(msg.settrigger);
          if (!isNaN(v)) {
            // Symmetric wrong-side guard (mirrors setrelease), but only
            // when a DISTINCT release exists - under release-follows-
            // trigger, a no-hysteresis settrigger moves both thresholds
            // together and can never invalidate the pair. An inverted
            // pair would flap outputs 1/2 on every reading in the band,
            // which is exactly what hysteresis exists to prevent.
            let distinctRel = distinctRelease();
            let wrongSide = distinctRel !== null &&
              (node.triggerdir === TRIGGER_DIRECTION.BELOW ? v > distinctRel : v < distinctRel);
            if (!wrongSide) trigApplied = v;
          }
        }
        if (trigApplied === null) {
          dispatchEvent(CONSENSUS_EVENT.TRIGGERSET, msg, true, msgSource,
                        { triggerSet: msg.settrigger });
          return;
        }
        overrideTrigger = trigApplied;
        dispatchEvent(CONSENSUS_EVENT.TRIGGERSET, msg, false, msgSource,
                      { triggerSet: trigApplied });
        evaluate(msg, msgSource);   // takes effect immediately
        writeState();
        node.status(buildStatus());
        return;
      }

      if (msgPayload === PAYLOAD.SETRELEASE) {
        let relApplied = null;
        if (node.typemode === TYPE_MODE.BOOLEAN) {
          relApplied = parseBoolOverride(msg.setrelease, msg.setreleasen, true);
        } else {
          let v = Number(msg.setrelease);
          if (!isNaN(v)) {
            // Validated against the trigger direction: the release value
            // must sit on the release side of the EFFECTIVE trigger
            // (override first, then config).
            let effTrig = effectiveTrigger();
            let wrongSide = effTrig !== null &&
              (node.triggerdir === TRIGGER_DIRECTION.BELOW ? v < effTrig : v > effTrig);
            if (!wrongSide) relApplied = v;
          }
        }
        if (relApplied === null) {
          dispatchEvent(CONSENSUS_EVENT.RELEASESET, msg, true, msgSource,
                        { releaseSet: msg.setrelease });
          return;
        }
        overrideRelease = relApplied;
        dispatchEvent(CONSENSUS_EVENT.RELEASESET, msg, false, msgSource,
                      { releaseSet: relApplied });
        evaluate(msg, msgSource);   // takes effect immediately
        writeState();
        node.status(buildStatus());
        return;
      }

      if (msgPayload === PAYLOAD.SETSTALE) {
        let staleUnits   = normalizeUnits(msg.setstaleunits);
        let staleRaw     = Number(msg.setstale);
        let staleApplied = isNaN(staleRaw) ? null : msgValueToMs(staleRaw, staleUnits);
        if (staleApplied === null || staleApplied < 0) {
          dispatchEvent(CONSENSUS_EVENT.STALESET, msg, true, msgSource,
                        { staleSet: staleApplied === null ? msg.setstale : staleApplied });
          return;
        }
        overrideStaleMS = staleApplied;   // 0 = staleness disabled
        dispatchEvent(CONSENSUS_EVENT.STALESET, msg, false, msgSource,
                      { staleSet: staleApplied });

        // Takes effect immediately: every seen source's staleness flag is
        // re-evaluated against the new window, in BOTH directions - a
        // shrunk window can expire sources on the spot, a widened (or
        // disabled) one can freshen sources whose data now sits back
        // inside it. Same mark/recompute-first ordering as
        // onStalenessExpiry. These flips carry the command's source
        // (external for a live setstale) since the incoming command is
        // what caused them - the one path where SOURCESTALE is not
        // internally sourced.
        let nowMS   = (new Date()).getTime();
        let staleMS = effectiveStaleMS();
        let toStale = [];
        let toFresh = [];
        Object.keys(sources).forEach(function(t) {
          let s = sources[t];
          if (!s.seen || s.lastSeen === null) return;
          let isStale = staleMS > 0 && (nowMS - s.lastSeen.getTime()) >= staleMS;
          if (isStale  && !s.stale) toStale.push(t);
          if (!isStale &&  s.stale) toFresh.push(t);
        });
        toStale.forEach(function(t) { sources[t].stale = true;  });
        toFresh.forEach(function(t) { sources[t].stale = false; });
        if (toStale.length > 0 || toFresh.length > 0) {
          computeAggregate();
          toStale.forEach(function(t) {
            dispatchEvent(CONSENSUS_EVENT.SOURCESTALE, msg, false, msgSource, { staleTopic: t });
          });
          toFresh.forEach(function(t) {
            dispatchEvent(CONSENSUS_EVENT.SOURCERECOVERED, msg, false, msgSource, { recoveredTopic: t });
          });
          evaluate(msg, msgSource);
        }
        armStalenessClock();
        writeState();
        node.status(buildStatus());
        return;
      }

      // -- Topic gate --------------------------------------------------------

      if (typeof msg.topic !== 'string' || msg.topic.trim() === "") {
        dispatchEvent(CONSENSUS_EVENT.READING, msg, true, msgSource, { rejectedValue: msg.payload });
        return;
      }

      // -- Coercion gate -----------------------------------------------------

      let coerced = (node.typemode === TYPE_MODE.BOOLEAN)
        ? coerceBoolean(msg.payload)
        : coerceNumeric(msg.payload);

      if (!coerced.ok) {
        dispatchEvent(CONSENSUS_EVENT.READING, msg, true, msgSource, { rejectedValue: msg.payload });
        return;
      }

      // -- Accepted reading --------------------------------------------------

      // One message, one reading-class event: the most specific of
      // SOURCEADDED / SOURCERECOVERED / READING, decided BEFORE mutating.
      let topic    = msg.topic;
      let existing = sources[topic];
      let readingEvent;
      let readingExtras = null;

      if (!existing || !existing.seen) {
        readingEvent  = CONSENSUS_EVENT.SOURCEADDED;
        readingExtras = { addedTopic: topic };
      } else if (existing.stale) {
        readingEvent  = CONSENSUS_EVENT.SOURCERECOVERED;
        readingExtras = { recoveredTopic: topic };
      } else {
        readingEvent  = CONSENSUS_EVENT.READING;
      }

      sources[topic] = { value: coerced.value, lastSeen: new Date(), stale: false, seen: true };
      baselineMsg    = msg;

      let prevAggregate = aggregate;
      computeAggregate();

      // READING is subject to emit-on-change-only; SOURCEADDED and
      // SOURCERECOVERED are always noteworthy and always emitted.
      let suppressReading = (readingEvent === CONSENSUS_EVENT.READING) &&
                            node.emitonchange &&
                            aggregate === prevAggregate;
      if (!suppressReading) {
        dispatchEvent(readingEvent, msg, false, msgSource, readingExtras);
      }

      // Quorum + trigger/release + minority, in causal order.
      evaluate(msg, msgSource);

      armStalenessClock();
      writeState();
      node.status(buildStatus());
    }

    // -------------------------------------------------------------------------
    // Persist helpers
    // -------------------------------------------------------------------------

    function writeState() {
      if (node.persist !== true) return;
      try {
        if (!fs.existsSync(path.dirname(stateFile))) {
          fs.mkdirSync(path.dirname(stateFile), { recursive: true });
        }
        // Serialize per-source lastSeen as ISO strings.
        let persistSources = {};
        Object.keys(sources).forEach(function(t) {
          persistSources[t] = {
            value:    sources[t].value,
            lastSeen: sources[t].lastSeen ? sources[t].lastSeen.toISOString() : null,
            seen:     sources[t].seen
            // stale is deliberately NOT persisted - it is recomputed
            // against wall-clock time on restore.
          };
        });
        fs.writeFileSync(stateFile, JSON.stringify(JSON.decycle({
          sources:         persistSources,
          consensusState:  consensusState,
          disabled:        disabled,
          overrideTrigger: overrideTrigger,
          overrideRelease: overrideRelease,
          overrideStaleMS: overrideStaleMS,
          baselineMsg:     baselineMsg !== null ? baselineMsg : {}
        })));
      } catch (error) {
        node.error("Error writing persistent file for sensor-consensus node " + node.id.toString() + "\n\n" + error.toString());
      }
    }

    function readState() {
      try {
        let contents = fs.readFileSync(stateFile).toString();
        if (typeof contents !== 'undefined') return contents;
      } catch (error) {
        node.error("Error reading persistent file for sensor-consensus node " + node.id.toString() + "\n\n" + error.toString());
      }
      return -1;
    }

    function deleteState() {
      try {
        if (fs.existsSync(stateFile)) fs.unlinkSync(stateFile);
      } catch (error) {
        node.error("Error deleting persistent file for sensor-consensus node " + node.id.toString() + "\n\n" + error.toString());
      }
    }
  }

  RED.nodes.registerType("sensor-consensus", SensorConsensus);
}
