# System / General settings (Settings → General) — configuration guidance

Guidance for CE platform-wide settings under **Settings → General**: Proxy,
Secrets Manager, HA Config, Logging, Tasks Cleanup, and related platform options.
These are cross-cutting (not per-module). Headings use the EXACT labels the user
sees in the UI.

> **Version sensitivity (read this first).** Platform settings — especially Proxy,
> HA, and Secrets Manager — have CHANGED across CE major versions, and the public
> docs sometimes lag the product. For any critical setting below, confirm the exact
> behavior against the official docs for the user's CE version before stating it as
> fact; if a live doc lookup disagrees with this note, prefer the doc AND say the
> behavior is version-dependent. Do not present a version-specific option as
> universally available.

## Proxy (Settings → General → Proxy)

CE uses a **single, global HTTP(S) proxy** configured under Settings → General →
Proxy (a Proxy Server Address + Port, with optional username/password for an
authenticated proxy). When configured, it applies **platform-wide to all plugins**
— proxy changes take effect without a CE core restart.

**There is no per-plugin / per-configuration proxy choice.** The old
per-configuration **"Use System Proxy"** toggle on a plugin's Basic Information step
was **removed in CE 6.0** — from 6.0 onward the global proxy simply applies to every
plugin; individual plugin configs do not opt in or out. Do **not** tell a user to
set or toggle a proxy on a specific plugin's config, and do not describe a
per-plugin proxy option as current behavior.

> Caution: older doc pages (the "Cloud Exchange Proxy" KB article, some FAQs, and
> pre-6.0 hardening guides) still describe the per-plugin "Use System Proxy" toggle.
> That wording is **stale** for CE 6.0+. If web search returns it, do not repeat the
> per-plugin toggle — state the global-only behavior and, if the user is on a version
> where it matters, point them at the release-specific docs to confirm. (This is the
> version-sensitivity rule above applied to a concrete case.)

If a user asks whether only some plugins can use the proxy, explain that the proxy
is global (all-or-nothing at the platform level) and that routing only specific
traffic through a proxy is a network/proxy-side concern, not a CE per-plugin setting.

## Other Settings → General options (brief)

- **Secrets Manager** — where CE stores plugin credentials/secrets; version- and
  deployment-sensitive (external vaults vs built-in). Confirm specifics against the
  docs for the user's version before advising.
- **HA Config** — high-availability / multi-node settings. Cross-node and
  version-sensitive; ground HA guidance on the official HA docs.
- **Logging** — the platform logging level (this is the platform log setting, not
  the AI "Posture assessment" log-analysis feature).
- **Tasks Cleanup** — retention/cleanup of completed task records.
- **Disk Alarm** (`disk_alarm`) — a **read-only, system-computed status flag**, not
  a user-facing toggle. CE flips it automatically when a node's available disk
  space drops below the platform's minimum threshold, and raises a warning
  notification when that happens. **There is no "Enable Disk Alarm" setting
  anywhere in Settings → General (or elsewhere) for a user to turn on** — do not
  describe steps to enable it, and do not invent a settings section/toggle for
  it. If asked how to "enable" it, explain that it is an automatic low-disk-space
  indicator the platform manages itself, not a configurable option; if the user's
  goal is to be alerted proactively, point them at existing disk-usage monitoring
  (e.g. the System Health dashboard) instead of a nonexistent toggle.

For any of these, if the user needs an exact value or a step-by-step, prefer the
official docs for their CE version over a general statement here.
