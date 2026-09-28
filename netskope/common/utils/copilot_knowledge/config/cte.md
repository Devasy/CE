# Threat Exchange (CTE) configuration — best practices & field rationale

Guidance for configuring CTE plugins, sharing (business rules), and module
settings. Field *definitions* come from the plugin manifest; this adds the
*why / what's a sensible value*. Always confirm specifics against the docs.

Headings below use the EXACT labels the user sees in the UI. Internal storage
keys appear once in (parentheses) only so raw tool output can be mapped to the
label — never show a storage key to the user.

## Directionality: pull and share are independent, and single-direction is normal

Most CTE plugins can both **pull** indicators (ingest) and **share** them (push to a
destination), but a configuration supporting both does **not** mean it must be set up for
both. Admins deliberately run many configs in one direction: a threat feed configured
**pull-only** (ingest into CE), or an enforcement/destination plugin configured
**share-only** (push out). Several plugins even expose an explicit toggle for this (e.g.
CrowdStrike's "Enable Polling", which is meant to be turned off for a push-only config).

Therefore: a config that has only ever pulled, or only ever shared, is **normal and
usually intentional — not a failure or an incomplete setup**. Do not flag "pull has never
run" or "share has never run" as a problem on its own. Only raise it if the user's stated
goal actually needs the missing direction (e.g. they said they want to share this feed but
no sharing rule sends it anywhere), and then frame it as "confirm this matches your intent",
not "this is broken".

When the setup is healthy, you MAY present the unused direction (or another unused
capability — tagging, IoC retraction) as an **OPPORTUNITY**: an added advantage on top of
what already works — say what they'd gain and what it takes, and offer to walk them through
enabling it. Positive framing only; never present a working uni-directional setup as a gap.

Note on authentication: Netskope-vendor plugins (e.g. the "netskope" CTE plugin)
authenticate via the tenant configured once under **Settings → Netskope Tenants** — the
plugin config only selects the tenant. There is no per-config URL or API-token field.

## Per-configuration (plugin) fields — Basic Information step

### Sync Interval (`pollInterval`)
How often the plugin syncs. Shorter = fresher IoCs but more API load and more
chance of hitting the source's rate limits. Start at the source's recommended
cadence; only shorten if freshness genuinely matters for enforcement. On the
Netskope plugin the label reads **Sharing Sync Interval** — it governs sharing
only, because that plugin extracts IoCs from malware/malsite alerts every 30
seconds regardless.

### Initial Range (plugin parameter, in days)
How far back to pull on first run (seeding historical data). Larger seeds pull
more but take longer and are more likely to hit pagination/rate limits — seed
what you'll actually use, then rely on incremental pulls.
**Note: this field only takes effect during the initial setup of a configuration; updating it on an existing configuration will not trigger a re-pull of historical data.**

### Indicator Aging Criteria (`ageAfterDays`, in days)
When pulled indicators expire locally (0–365). Too long keeps stale IoCs in
enforcement (policy bloat); too short drops indicators that are still relevant.
Match it to how long the source considers an indicator valid.

### Override Reputation (`defaultReputation`)
Optional per-source reputation (0–10). Use it to express how much you trust a
feed so downstream tiered policies can weight sources differently.

### Tags Aggregate Strategy (`tagsAggregateStrategy`: Append vs Overwrite)
How tags from multiple sources merge on the same indicator. **Append** preserves
all sources' tags (richer filtering); **Overwrite** keeps only the latest.
Prefer Append unless tag noise is a problem.

### Enable SSL verification (`sslValidation`)
Verify the source's SSL certificate. Keep it on; turn off only for lab/self-
signed endpoints, and say so explicitly when recommending that.

### Common Parameter Behaviors
Some plugin-specific parameters (e.g. 'Retraction Interval' in various feeds) only take effect during the initial setup of a configuration. Updating these on an existing configuration will not trigger a retrospective action or change existing behavior. Always explicitly warn the user if a suggested field is 'setup-only'.

## Sharing / business rules

### A business rule is the FILTER only — sharing is a SEPARATE screen

This is the single most important CTE fact for guidance. In the UI these are two
different screens:

- **Business Rules** (Threat Exchange > Business Rules): you author ONLY the
  rule's **name** and its **filter** (which indicators it matches). That is all
  the page has. There is **no destination, no target, and no `sharedWith` field**
  on this page.
- **Sharing** (Threat Exchange > Sharing): where the rule is actually WIRED to
  deliver — you pick a source configuration, the business rule, a destination
  configuration, and the destination's share action. Saving this is what
  populates the rule's stored `sharedWith`.

So a rule can match indicators yet deliver nowhere until a Sharing entry maps it
to a destination — that is exactly what "matches but shares to no destination"
means, and the fix is to add a Sharing entry, **not** to edit the rule.

Because of this split, when diagnosing delivery **never tell the user to
"confirm/inspect `sharedWith` on the rule"** — that field is not on the Business
Rules page; it only appears via the Sharing screen. To check whether a rule
delivers, send them to the **Sharing** screen (an empty Sharing table = the rule
reaches nothing), or read it programmatically (a rule's `sharesToConfigs` = 0
means it is unwired). `sharedWith` is an internal storage key — do not surface it
to the user at all; talk about the **Sharing** screen instead.

### A CTE rule matches EITHER indicators or a CRE entity

A business rule carries an **entity**: either the native **Threat Indicators** data
(the default, and what a rule with no `entity` means) or a **CRE entity** — CTE and
CRE share data across modules, so a CTE rule can read a CRE entity's records and
share those instead of indicators.

The wiring differs by entity, and both are configured on the same **Sharing** screen:

| Rule entity | Matches | Sharing stored in |
|---|---|---|
| Threat Indicators | indicators | `sharedWith` (source config → destination config → actions) |
| a CRE entity | that entity's records | `creShare` (destination config → actions — **no source layer**, the source is the entity) |

The two are **mutually exclusive** on a rule. So an empty `sharedWith` does **not**
mean a rule is unwired: a CRE-entity rule with `creShare` set is fully configured.
Check both before saying a rule delivers nowhere (`sharesToConfigs` already counts
both). Describe a CRE-entity rule as matching that entity's **records**, never as
matching "indicators".

Note the knock-on: **disabling the CRE module mutes AND locks** every CTE rule on a
CRE entity (and disabling CTE does the same to CRE rules on Threat Indicators). A
locked rule rejects every edit until the other module is re-enabled — so if a user
cannot edit such a rule, the fix is to re-enable the other module, not to change the
rule.

### Rule filter targets

Keep filters specific: sharing everything to every destination causes false
positives and licensing cost downstream. Assign confidence/labels so tiered
enforcement is possible.

### Exceptions
Exclude noisy indicators (by sub-filter or tag) from an otherwise-broad rule
rather than loosening the whole rule.

## Module settings (Settings → Threat Exchange)

### IoC(s) Retraction + Retraction Interval (`iocRetraction`, `iocRetractionInterval`)
When enabled, CE fetches retracted indicators and marks them as retracted —
indicators count as retracted if they were deleted at the source, fall outside
the plugin configuration's retraction interval, or fall outside that
configuration's pulling scope. Retracted indicators are pulled back from shared
destinations on each destination's Sync Interval. The retraction task runs every
**Retraction Interval** days. **Enable it** if you see a high
retracted-vs-unretracted ratio on the dashboard — otherwise withdrawn indicators
linger in enforcement.

### Reconciliation Criteria (`criteria`)
How IoC metadata is updated when different sources report the same IoC:

- **Always Overrides** — the IoC metadata is overridden with the latest IoC (default).
- **Never Overrides** — the IoC metadata is not overridden; the oldest is kept.
- **Higher Severity Source Override** — the higher-severity source's IoC metadata wins.

### Delete Inactive IoC(s) Indicators (`deleteInactiveIndicators`)
When enabled, CE deletes indicators marked inactive and not retracted — and also
deletes inactive+retracted indicators after verifying their retraction status.
Turn on to keep the indicator store lean; leave off if you need to retain
history.

### Generate Alerts (`generateAlerts`)
A module-level default (on by default) for the **Generate Alert** field some
destination plugins expose on a Sharing Configuration — it tells the
*third-party platform* to raise its own alert when an indicator CE shared is
later detected there. This is **not** the same feature as Risk Exchange
(CRE)'s Generate Alert toggle on an Action Configuration, which instead
raises a UBA-style alert into Ticket Orchestrator — the two are unrelated
despite the identical name. Leave on unless you specifically want shared
indicators to stay silent on the destination platform.
