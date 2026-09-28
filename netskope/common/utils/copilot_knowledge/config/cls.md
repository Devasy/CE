# Log Shipper (CLS) configuration — best practices & field rationale

Guidance for configuring Cloud Log Shipper plugins, filter business rules, and
Log Delivery (the SIEM fan-out). Field *definitions* come from the plugin
manifest; this adds the *why*. Confirm vendor specifics against the plugin guide
(web_search its docSearchQuery). Headings use the EXACT UI labels; storage keys
appear once in (parentheses) only so raw tool output can be mapped — never show a
storage key to the user.

CLS pulls log data from a source, FILTERS it with business rules, TRANSFORMS it to
the destination's format, and forwards it to a SIEM destination. The flow is:
Plugins → Business Rules (filter) → Log Delivery (route filtered logs to a SIEM).

## Per-configuration (plugin) fields — Basic Information

### Configuration Name (`name`)
A unique name; immutable after creation.

### Pull Interval (`pollInterval` + `pollIntervalUnit`)
How often a PULL/source plugin fetches logs. Shown only for pull-capable,
non-Netskope plugins. Netskope-tenant sources do not show it (they use the
tenant configured under Settings → Netskope Tenants).

### Tenant (`tenant`)
Netskope-family plugins only: selects the configured Netskope tenant (no
per-config URL/token — those live on the tenant).

### Mapping (`attributeMapping`) + Format (`transformData`) — INLINE on Basic Information
For a non-Netskope PUSH plugin (a SIEM destination), the Basic Information step
also carries the attribute-mapping file (transforms CE fields to the SIEM's
fields) and the wire Format — **CEF** or **JSON**. These are NOT a separate wizard
step. Author custom mapping files under **Settings → Log Shipper → Mapping**.

When guiding a user through Basic Information, note this as an OPTIONAL choice, not a
requirement: the plugin ships a sensible DEFAULT mapping that works for the common case,
so most setups can leave the default selected and finish. Mention that if their use case
needs different fields on the wire (a non-standard SIEM schema, extra/renamed fields, a
specific CEF/JSON layout), they can instead pick a CUSTOM mapping here — authored under
Settings → Log Shipper → Mapping — and select it in this step. Frame it as "default is
fine unless you need X"; do not push a custom mapping when the default suffices.

## Configuration Parameters
After Basic Information, the manifest's Configuration Parameters step(s) collect
the plugin's own fields (credentials are secret — the admin enters them).

## Business rules (FILTER only)
A CLS business rule defines WHICH logs match, built in the VISUAL query builder (pick
a field, an operator and a value per condition, combined with AND/OR/NOT) over the
deployment's LEARNED log fields. Those fields are dynamic — call
get_business_rule_format for the real field/operator vocabulary and build the
conditions only from it. The field list populates once logs have been ingested; until
then, tell the user there are no filterable fields yet rather than inventing any. Do not describe the filter as a query string or a database query.
A rule carries the filter only.

The default **All** rule is undeletable (it matches everything). Use it for a
catch-all forward, or add narrower rules for specific destinations.

## Log Delivery (the routing — a SEPARATE screen)
Routing is NOT on the rule form. On the **Log Delivery** screen a rule's
`siemMappings` are set: `{ source configuration → [destination configurations] }`
— which log source forwards its matching logs to which SIEM destination. A rule
with empty siemMappings forwards nothing (that is the "unwired" state). Inspect
this with get_cls_mappings.

## WebTx
WebTx configurations track ingested volume in bytes (`bytesIngested`) rather than
a log count — a flat byte counter can indicate a stalled WebTx feed.

## Log Shipper Setting (Settings → Log Shipper)
Two tabs: **General** (log-delivery retry / page-size settings) and **Mapping**
(create/edit the CEF/JSON attribute-mapping files that plugins reference).
