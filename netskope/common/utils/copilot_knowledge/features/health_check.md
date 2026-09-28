# Pre-Upgrade Health Check

> Preview feature — may not be published on docs.netskope.com yet. If a public docs page for this
> exists, prefer it and cite it over this note. This is the fallback grounding until then.

The **Health Check** lets an admin validate a running Cloud Exchange host against the prerequisites
for a target CE version **before** starting an upgrade — so problems are found while the current
deployment is still up, not after it has been taken down. It's read-only and safe to run anytime.

## Where it is / what the user does
- **Settings → General**, a **"Health Check"** action that opens the **"Cloud Exchange Health
  Check"** dialog. (Tooltip: "Validate this host against the prerequisites for a target Cloud
  Exchange version before upgrading.")
- In the dialog the user sets two things, both pre-filled with sensible defaults:
  - **Deployment type** — **Standalone (SA)** or **HA** (cluster). Pre-filled from the server's
    known topology (derived from the HA node list), so the user usually just confirms it.
  - **Target Cloud Exchange version** — the version they intend to upgrade TO. Pre-filled from the
    currently installed version as a starting point; the user sets the real target.
- Click **Run Health Check**. It runs asynchronously on the host; the dialog polls status every few
  seconds and shows progress ("Health check in progress…").
- When it finishes, the dialog shows a **summary** of the results, and a **Download report** action
  is enabled (report available as **HTML** or **JSON**).

## What it checks / use-cases
- It validates the host against the prerequisites for the chosen target version — the kinds of
  pre-flight conditions that would otherwise cause a mid-upgrade failure (host readiness, version
  compatibility, and, for HA, that every node is reachable).
- **Use it before every upgrade**, especially on HA: catch an unreachable/misconfigured node or an
  unmet prerequisite up front, fix it, and only then take the deployment down to upgrade.
- The summary reports overall pass/fail plus, for HA, how many nodes could not be reached
  (`nodes_unreachable`) — a non-zero count is surfaced as a warning even if the rest passed.

## Good to know / gotchas
- **Runs on the management server**, not the main CE API — that is deliberate, so the check works as
  a pre-upgrade gate independent of the core stack.
- **Read-only** — it validates and reports; it changes nothing and never starts the upgrade itself.
- **HA:** an unreachable node is flagged ("Health check completed, but N node(s) could not be
  reached") — resolve node connectivity before upgrading, or that node will be missed.
- The **downloadable report** (HTML for reading, JSON for tooling/automation) is the artifact to
  review or attach to a change ticket before proceeding. A download attempted before any run has
  completed returns a clear "no report available yet" message rather than an error.
- Status the dialog reflects: running → completed / failed / error (and "not found" when no run has
  happened yet). A failed/errored check surfaces the reason; fix it and re-run before upgrading.
