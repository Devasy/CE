# Exact Data Match (EDM) configuration — best practices & field rationale

Guidance for configuring EDM plugins, sanitization, and sharing. Field
*definitions* come from the plugin manifest; this adds the *why*. Headings use the
EXACT UI labels; storage keys appear once in (parentheses). Never show a storage
key to the user.

EDM generates EDM hashes from sensitive data and applies them to a Netskope tenant
for DLP exact-data matching. It has **no filter business rules** — its "rule" is a
1:1 source→destination SHARING. The flow is: Plugins (+ Sanitization) → Sharing
(source→destination) → Sharing and Upload Management (verify apply on the tenant).

## IMPORTANT — Netskope-owned engine (do not edit)
The internal hash-generation / EDK / tenant-upload engine is **Netskope-owned**.
The copilot may DIAGNOSE issues there but must NEVER propose edits to it. Guide only
the CE-owned surface: sanitization config, sharing, plugin parameters, manual upload.

## Per-configuration (plugin) fields — Basic Information

### Configuration Name (`name`)
Unique; immutable after creation. Used in hash file naming.

### Plugin Type (`pluginType`) — Netskope forwarder/receiver plugin only
`forwarder` or `receiver`. A **receiver** ingests pre-generated hashes over
CE-to-CE and therefore has NO downstream configuration steps.

### Sync Interval (`time` + `unit`) — non-Netskope plugins
How often a source pulls. Hidden for the Netskope plugin.

### Tenant (`tenant`) + Enable SSL verification (`sslValidation`)
Netskope plugins only (Tenant requires a manifest provider_id): the tenant EDM
hashes are applied to.

## Sanitization step
For hash-generating plugins the wizard includes a **Sanitization** step: choose,
per column, the Name Column, Normalization (none/string/number), and whether to
Create a Dictionary. "Proceed without sanitization" skips it. Preview Good/Bad File
shows how a sample sanitizes before you commit.

## Sharing (NOT filter rules)
On the **Sharing** screen create a 1:1 pairing: ONE source configuration →
ONE destination configuration (source must be pull-capable, destination
push-capable; source ≠ destination; no duplicate pair). This sharing IS the flow —
there is no filter query. Inspect configured pairs with get_edm_sharing.

### Manual sync (apply a source now)
Sharing normally runs on the source's polling schedule. To push a source's hashes to
the tenant IMMEDIATELY — after creating a pairing, changing sanitization, or to retry a
failed apply — there is a manual **Sync** action on the Sharing screen (per source). It
runs the full lifecycle for that source: pull → generate EDM hashes → upload to ALL
destinations paired with that source that are ready (status completed / failed /
scheduled). Destinations already in progress are SKIPPED, so a Sync that "does nothing"
usually means an apply is still running — check **Sharing and Upload Management** for the
live status. Guide the user to the Sync control; the copilot never triggers it.

## Sharing and Upload Management (hash apply status)
Where generated hashes are tracked as they upload + apply on the tenant. Statuses
progress generating_hash → uploading_hash → checking_apply_status → completed
(or failed). A hash that stays in checking_apply_status / apply_in_progress and
never clears is a STUCK apply — the tenant keeps returning pending/in-progress.
Inspect in-flight applies with get_edm_hash_status.

## Manual upload
A CSV can be uploaded, sanitized, hashed, and shared directly (a separate flow from
a polling plugin), tracked with its own status.

## Exact Data Match Setting (Settings → Exact Data Match)
A single form (no tabs): **Delete EDM Hash Files** (`edmFilesCleanup`, days) —
how long generated hash files are retained.
