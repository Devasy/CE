# Secrets Manager (external secret stores)

> Preview feature — may not be published on docs.netskope.com yet. If a public docs page for this
> exists, prefer it and cite it over this note. This is the fallback grounding until then.

The **Secrets Manager** lets you keep credentials out of Cloud Exchange's database: instead of
storing an API token/password in a plugin or tenant config, you store a **reference** to a secret in
your own external vault. CE resolves the reference at use time, so you can rotate/audit the secret in
the vault and CE picks up the new value automatically.

## Supported providers (exactly three)
CE supports **HashiCorp Vault**, **Azure Key Vault**, and **AWS Secrets Manager** — and only these.
(CyberArk, Delinea/Thycotic, and GCP Secret Manager are **not** supported.) One provider is active
at a time, globally, for the whole deployment.

Auth methods per provider:
- **HashiCorp Vault:** Token · AppRole · Username & Password. (`clusterURL` required; optional
  Enterprise `namespace`.)
- **Azure Key Vault:** Client Secret · Certificate (PEM). (`vaultUrl`, `tenantId`, `clientId`.)
- **AWS Secrets Manager:** *Deployed on AWS* (uses the CE host's IAM instance role — no creds
  stored) · *IAM Roles Anywhere* (for on-prem CE — uses an X.509 client cert + private key to mint
  temporary STS creds). Plus an AWS `region`.

## What you can do
- **Settings → General → Secrets Manager** (admin only — needs `settings_write`). Toggle **Enable**,
  pick a provider, fill the provider's fields (the form is dynamic and shows only the fields for the
  chosen auth method), and **Save**. Save runs a **live connection/auth test** — it won't persist a
  config that can't authenticate.
- **Reference a stored secret elsewhere in CE:** any credential field that supports it shows a
  **lock toggle**. Toggle it on ("use Secrets Manager") and enter the secret's location; CE stores a
  `secret:...` reference instead of the plaintext. The toggle only appears when a Secrets Manager is
  enabled. Reference shape by provider:
  - **HashiCorp** → engine / path / key → `secret:{engine}/data/{path}:{key}`
  - **Azure** → secret name → `secret:{name}` (the vault URL is the global setting, not in the ref)
  - **AWS** → secret id (+ optional JSON key) → `secret:{id}` or `secret:{id}:{key}`

## The logic / use-cases
- **Why:** central secret management + rotation, and no long-lived credentials sitting in CE's Mongo.
  Rotate in the vault → CE resolves the new value on next use, no CE re-config.
- **What can use it:** effectively any credential field across CE — all plugin module configs (CTE,
  CTO, CLS, CRE, EDM, CFC), tenant tokens, plugin-repo passwords, and even the **LLM provider** API
  key — because CE resolves `secret:` references transparently at config-load time; the plugin never
  knows a vault was involved.
- **Auth-method trade-offs:** AWS "Deployed on AWS" stores zero credentials (best when CE runs in
  AWS); "IAM Roles Anywhere" is the on-prem path (cert/key stored, temp STS creds not). Azure
  certificate avoids a shared secret but you manage the PEM. HashiCorp AppRole/Userpass let CE mint
  its own token.

## Good to know / gotchas
- **Secret fields blank out on the form by design** — CE never returns stored secrets to the UI. On
  edit, leave a secret field blank to keep the existing value; re-enter it only to change it.
- **Save is a real auth test:** Azure lists secrets (needs List+Get permission), AWS probes
  GetSecretValue on a throwaway name (needs `secretsmanager:GetSecretValue`), HashiCorp authenticates
  live. A permissions/connectivity problem shows a specific error and blocks the save.
- **You can't disable Secrets Manager, or switch providers, while any `secret:` reference still
  exists** anywhere in CE ("cannot be disabled while in use" / "cannot switch provider…"). Replace
  those references with plaintext first. (References are provider-specific — a HashiCorp reference
  won't resolve under AWS/Azure, which is why switching is guarded.)
- **A missing/unreachable referenced secret** makes the dependent operation fail with a clear message
  (e.g. "AWS secret 'X' not found") rather than silently using a blank; CE auto-retries once on an
  auth error (handles rotated tokens) before failing.
- **Prerequisites live outside CE:** AWS IAM role/trust + `GetSecretValue` policy; Azure app
  registration with Key Vault Get+List; HashiCorp policy on the KV path (note the mandatory `/data/`
  segment for KV-v2 references).
- The `plain:` prefix is an escape hatch to store a literal value that happens to start with
  `secret:` without it being treated as a reference.
