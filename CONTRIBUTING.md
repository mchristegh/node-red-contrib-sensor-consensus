# Contributing

Thanks for your interest in this project. Please read this before
opening a pull request — it will save you time.

## Code contributions are not being accepted

This project does not accept pull requests. PRs will be closed without
review, regardless of content or quality — it's a policy decision about
how the project is maintained, not a judgment of your work.

If you've found a problem or have an idea, an issue is the right channel,
and issues genuinely are welcome.

## Reporting a bug

Please [open an issue](../../issues) and include:

1. **What you expected and what happened instead** — a sentence or two
   each.
2. **Your node configuration** — the settings from the edit dialog
   (duration, units, and any non-default options such as lock, threshold,
   heartbeat, cooldown, or persistence).
3. **Debug output from output 4 (Events)** — wire it to a debug node set
   to show the _complete msg object_ and reproduce the problem. The
   event envelope (`timerEvent`, `ignored`, `source`, `timerState`)
   almost always identifies the cause.
4. **Your Node-RED and Node.js versions.**

Before filing, it's worth a skim of the
[Troubleshooting](../../wiki/Troubleshooting) page — several behaviors
that look like bugs are documented, deliberate design decisions.

## Requesting a feature

[Open an issue](../../issues) describing the _problem_ you're trying to
solve, not just the proposed mechanism — the use case is what makes a
request actionable. If it can be expressed in terms of the existing
event model (outputs, commands, the message envelope), even better.

No promises on timelines or acceptance, but every issue gets read.

## Documentation

The [project wiki](../../wiki) is the full reference. If you spot an
error or gap in it, that's an issue too — please quote the page and
passage.
