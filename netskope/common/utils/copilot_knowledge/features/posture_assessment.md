# Posture Assessment (single-log Assess & multi-log Posture Assessment)

> Preview feature — may not be published on docs.netskope.com yet. If a public docs page for this
> exists, prefer it and cite it over this note. This is the fallback grounding until then.

**Posture Assessment** is the umbrella feature/label — both the per-log and the multi-log flow now
present themselves under it (renamed from "Analyze" / "Platform Log Analyzer"). Under the hood there
are still two distinct request shapes on the **Settings → Logging** (Audit) screen, both requiring
the **`ai_read`** scope and an **active LLM provider**:
- **Assess (one log)** — AI root-cause for **one** log line. Column header "Assess"; row tooltip
  "Posture assessment"; confirm dialog and result modal both titled **"Cloud Exchange Posture
  Assessment"**.
- **Posture Assessment (many logs)** — an overall health triage across **many** logs (the ones
  matching your current filter). Same "Cloud Exchange Posture Assessment" titling, run from the
  Logs toolbar instead of a row.
- Backend telemetry folds both under one feature (`AIFeature.POSTURE_ASSESSMENT`, dashboard label
  "Posture Assessment") — they were previously split as "Analyze" and "Posture Assessment" and are
  now one reported feature, though the two request shapes (`/api/copilot/analyze` vs
  `/api/copilot/triage`) are unchanged internally.

## Assess (one log)
- **What you do:** in the Logs table, each analyzable row shows an **Assess** (brain) icon
  — hover shows "Posture assessment". Click it → confirm ("Cloud Exchange Posture Assessment" /
  "Perform analysis of the selected log") → a result modal titled **"Cloud Exchange Posture
  Assessment"** opens and analyzes that single log (a quick request, no streaming).
- **What you get:** **Summary**, **Probable Root Cause** (top one or two), **Suggested
  Remediation** (numbered steps), **Sources** (doc citations), and a footer with **Confidence %**,
  input/output tokens, and a **Web Enriched** tag if it consulted the docs.
- **Use it when** you're looking at one specific error and want "why did this happen, how do I fix
  it" — fast and targeted.

## Posture Assessment (many logs)
- **What you do:** a **Posture Assessment** (health) icon in the Logs toolbar assesses the logs
  **matching your current filter** (set the QueryBuilder filter first — by time, type, error code,
  message). Click → a count-aware confirm ("Run Cloud Exchange Posture Assessment" / "assess the N
  logs matching your filter") → a **Posture Assessment** modal opens and **streams its progress** as
  live "Analysis steps" (counting logs → building an error heatmap → reading real log windows →
  drilling into pivotal logs → optionally searching docs).
- **What you get:** an **overall posture** — **Healthy / Degraded / Critical** — plus **Health
  Scores** per category (Data ingestion, Netskope tenant, System, Container, Platform health;
  0–100 bars), an **Incident Timeline** (newest-first, severity-chipped events), prioritized
  **Recommended Actions** (each with estimated impact + numbered steps + how many logs it affects),
  **Sources**, and a footer (Confidence %, **Logs analyzed**, Tool calls, tokens, Web Enriched, and
  a **Download report (.md)** of the whole assessment).
- **Use it when** you want "how healthy is my deployment (or this slice of it)" — a triaged,
  prioritized overview across many logs.

## Good to know / gotchas
- **Both need an active LLM provider** (Settings → LLM Provider). Without one you get a clear
  "No active LLM provider is configured" error — configure one first.
- **Filter before a Posture Assessment.** It **samples** up to a server cap (default ~500 logs), it
  does **not** read every matching log — so on a huge filter the result is partial and costs more
  tokens. If your filter matches a very large set the dialog warns you and suggests narrowing it.
  The footer's **"Logs analyzed"** is the true number sampled (may be far below the match count).
- **Citations are documentation-grounded** (docs.netskope.com), not open web browsing; inline `[N]`
  markers map to the numbered Sources.
- **"Web Enriched"** means the answer was grounded in the web/docs (set either from an explicit web
  search or from detecting standard citations — so it can be true without a visible "web_search"
  step).
- **Confidence** is the model's self-reported confidence (0–100%); low is an honesty signal when the
  logs are sparse/ambiguous, not a bug.
- **Closing the multi-log Posture Assessment modal cancels the run** (it disconnects the stream,
  which stops token spend) — useful if you're cost-conscious. The single-log Assess request has no
  stream to cancel.
- **Two request shapes, one label.** Don't tell a user "click Analyze" — the button reads **Assess**
  and the dialog/modal both say "Cloud Exchange Posture Assessment" for either flow. Distinguish them
  by scope (one log vs. the filtered set) when explaining, not by a separate feature name.
- **Steps never break your deployment:** by design neither remediation nor action-item steps contain
  shell commands or code changes (CE runs in Docker). "Contact support" steps tell you to attach the
  diagnostics file from **Settings → General → Run Diagnose**.
- The downloadable posture report is generated in the browser (air-gap/on-prem safe, no network).
