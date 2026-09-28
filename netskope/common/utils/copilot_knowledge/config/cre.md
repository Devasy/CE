# Risk Exchange (CREv2) configuration — best practices & field rationale

Guidance for configuring Cloud Risk Exchange plugins, per-entity business rules,
and actions. Field *definitions* come from the plugin manifest / entity schema;
this adds the *why*. Confirm vendor specifics against the plugin guide (web_search
its docSearchQuery). Headings use the EXACT UI labels; storage keys appear once in
(parentheses). Never show a storage key to the user.

CRE ingests records into normalized ENTITIES (Users, Applications, …), scores /
evaluates them with business rules, and runs ACTIONS on configurations when a rule
matches. The flow is: Plugins (+ Entity Sources, optionally via Auto-Mapper) →
[Schema Editor] → Business Rules (per entity) → Actions → Records / Action Logs.
Once two or more CRE entities (and/or CTE indicators) need to be correlated
together instead of filtered one at a time, Universal Schema Builder (USB) is
the cross-entity layer on top of this same data — see its section below.

## Per-configuration (plugin) fields — Basic Information

### Configuration Name (`name`)
A unique name; immutable after creation.

### Sync Interval (`pollInterval` + `pollIntervalUnit`)
How often a non-Netskope plugin pulls. Netskope-tenant plugins hide this — they
pull on a FIXED 1-hour interval and instead show the Tenant selector.

### Tenant (`tenant`)
Netskope-family plugins only: selects the configured Netskope tenant (no per-config
URL/token).

## Entity Sources (the final wizard step)
After the parameter steps, every CRE plugin config ends with **Entity Sources**:
map each plugin field to a Cloud Risk Exchange ENTITY field (e.g. map the plugin's
"email" to the Users entity's "User"). Leaving an entity unmapped pulls nothing for
it but still lets actions run.

### Auto Map with AI (on Entity Sources)
Instead of mapping every plugin field by hand, click **Auto Map with AI** (after
picking the destination entity) to get a suggested mapping for the whole entity in
one AI call — it reuses a compatible existing entity field where one fits, or
proposes a **new** field where none does. Suggested new fields must be reviewed
("Configure new fields": label, unique/merge-key, normalization, aggregate
strategy) and created before the config can be saved. Re-running Auto Map is
incremental: rows the user leaves unchecked stay locked and are not re-suggested.
See get_ce_knowledge('cre_auto_mapper') for the full decision logic and gotchas
(preview feature — not yet on docs.netskope.com).

## Business rules (per-ENTITY, dynamic fields)
A CRE rule targets ONE entity (e.g. Users, Applications), chosen when the rule is
created and fixed after that. Its filter is built in the VISUAL query builder — the
user picks a field, an operator and a value per condition and combines them with
AND/OR/NOT. The filter FIELDS are DYNAMIC per entity — they are NOT a fixed list, and
differ by entity. Call get_cre_entities FIRST to get the chosen entity's real fields,
THEN get_business_rule_format to build the conditions from exactly those fields and
their valid operators. Never suggest a field the entity does not have, and never
describe the filter as a query string or a database query — the builder produces it.

The **Threat Indicators** entity is read-only (bridged from CTE) and needs a CTE
source configuration selected; it can be auto-disabled if CTE is off.

## Actions (the wiring)
A rule only FILTERS — it does not act on its own. Wiring is done on the SEPARATE
Actions screen: an action ties the rule to an operation on a configuration, and runs
when a record matches. A rule with no action scores/evaluates but does nothing (the
"unwired" state). Inspect the wiring and recent outcomes with get_cre_actions; the
Action Logs screen shows each action's status (success / failed / declined / awaiting
approval / scheduled).

## Schema Editor
Where an entity's fields are defined/extended. Adding a field here makes it available
to rules and mappings for that entity.

## Universal Schema Builder (USB) — Unified Mapping & Business Rules
A CRE business rule only filters ONE entity at a time. When the user needs to
CORRELATE records across multiple CRE entities and/or CTE indicators (e.g. "show
me Users whose risk score is low AND who appear in a recent indicator"), that is
**Universal Schema Builder**, a separate module-index page (not the per-entity
Business Rules screen):

- **Unified Mapping** — build the join: pick a base table, then join additional
  CTE/CRE tables one at a time on a drag-to-connect canvas. Every join is LEFT
  OUTER and equals-only, and at least one side must be a unique field. The joined
  result is never stored — only the join definition is saved; Preview/Execute
  recompute it on demand (default match mode: matched_only).
- **Business Rules** (on top of a saved mapping) — filter the mapping's joined
  rows and wire them to CTE sharing (push matching rows as indicators to a
  destination) and/or CRE actions (run a CRE action on a configuration). A
  mapping with no rule on it can only be viewed/previewed — it does nothing.

Joining/sharing CTE indicators needs the Threat Exchange module enabled AND CTE
access; a CRE-only user can still build and use CRE-to-CRE mappings and rules
freely. Call get_ce_knowledge('unified_mapping') for the join/rule model in full
and get_unified_mappings to inspect saved mappings and rules. Preview feature —
not yet on docs.netskope.com.

## Risk Exchange Setting (Settings → Risk Exchange)
Four tabs: **General** (Module level allow/deny for Generate Alert, Maintenance Window configuration for actions to be performed),
 **Logs Cleanup**(The action logs cleanup config[1,365] days),
 **Flap Suppression** (dampens rapid
score flip-flopping [1,1440] minutes)
**Records Cleanup** (age out stale records, only if enabled).
