# LLM Provider configuration

> Preview feature — may not be published on docs.netskope.com yet. If a public docs page for this
> exists, prefer it and cite it over this note. This is the fallback grounding until then.

The **LLM Provider** is the AI backend that powers every AI feature in Cloud Exchange — the Cloud
Exchange Copilot, single-log **Analyze**, **Posture Assessment**, and the proactive "Needs
attention" findings. You configure and enable exactly one provider; that turns the AI features on.

## Where it is / what you can do
- **Settings → LLM Provider** (admin only — needs the **`ai_write`** scope; read-only users see the
  configured providers but can't change them).
- **Add one:** click **Configure Provider** → pick the LLM plugin from the Store (filtered to the
  "LLMProvider" category) → the Configure modal opens back on this screen. Fill the fields and Save.
- **Manage:** each configured provider shows as a tile with **edit**, **enable/disable**, and
  **delete** actions. Status shows **Enabled** (green) / **Disabled**.

## Fields you set (bundled Anthropic plugin)
- **Configuration Name** (required) — a name for this config; **immutable after creation** (delete +
  recreate to rename).
- **API Key** (required) — the provider API key (Anthropic: from console.anthropic.com). **Write-only**
  — never shown back; on edit, leave blank to keep the existing key, re-enter only to change it.
- **Model** (required) — bundled choices: **Claude Opus 5**, **Claude Opus 4.8**, **Claude Sonnet 5**
  (default **Sonnet 5**). Recommendation: **Sonnet 5** for cost/latency on routine analysis; **Opus
  5 / 4.8** for the hardest multi-log agentic reasoning.
- **Agentic Effort Calibration** (required, appears after you pick a model) — Low / Medium / High / (default **High**). How much reasoning effort the model spends on agentic tasks
  (higher = more thorough, slower/costlier).
- **Enable SSL verification** (default **on**) — verify TLS on outbound calls; turn off only for a
  proxy that breaks the chain.

`max_tokens` is **not** a UI field (tuned per model; an operator can override with the
`AI_COPILOT_MAX_TOKENS` env var). Proxy is **not** per-provider either — the plugin uses CE's
system-wide proxy setting.

## The logic / use-cases
- **One active provider at a time, globally.** Only one provider may be enabled across all plugins.
  Creating/enabling a second while one is active is rejected ("Another LLM provider is already
  enabled… disable it first"). **To switch providers: disable the current one, then enable the new
  one** — there's no auto-swap. You can pre-create providers disabled and flip later.
- **Validation is a live probe on Save/enable** (not a separate button): it checks the model is
  supported + the effort level valid, then calls the provider to confirm the **API key works and the
  model exists** before persisting. A failure blocks the save and shows the exact reason.
- **A provider is the prerequisite for all AI features** — with none active, Copilot/Analyze/Posture
  all return "No active LLM provider is configured. Enable an LLM provider…".
- Configuring the first provider registers the proactive attention scan + hides the "configure a
  provider" banner; deleting the last one unregisters the scan, purges existing findings, and brings
  the banner back.

## Good to know / gotchas
- **Common validation errors** (shown on Save): invalid API key (401/403), model unavailable (404),
  network/proxy unreachable, rate-limited (429), provider down (5xx). Fix and re-save.
- **Air-gapped/offline:** validation needs a **reachable** provider endpoint, so configuring a
  provider will fail in a fully air-gapped deployment unless outbound access to the provider API is
  allowed (directly or via the CE system proxy). Web-search enrichment likewise needs it enabled for
  your account.
- **Configuring end-to-end needs `ai_write` + `settings_read`** — no single AI role bundles
  `settings_read`, so a pure-AI admin may see the copilot's "go to LLM Provider settings" prompt but
  still need settings access to finish.
- The exact model list is owned by the installed provider **plugin** (its manifest), so it tracks
  whatever plugin version is installed — the three Claude models above are the current bundled set.
- A missing/corrupt plugin after a config exists gives a distinct "Configured LLM provider plugin
  could not be loaded" error (different from the no-provider case).
