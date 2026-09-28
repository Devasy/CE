# Unified Mapping (Unified Join Builder)

> Preview feature — may not be published on docs.netskope.com yet. If a public docs page for this
> exists, prefer it and cite it over this note. This is the fallback grounding until then.

**Unified Mapping** lets you build a live, joined view across your Threat Exchange indicators and/or
one or more Risk Exchange (CRE) entity collections — without copying or storing the joined data. You
define how the tables join; Cloud Exchange recomputes the joined rows on demand. The screens live under
**Universal Schema Builder (USB)**: **Unified Mapping** (the join builder) and, on a saved mapping,
**Business Rules** (filter the joined rows and wire them to CTE sharing / CRE actions — see below).

## What you can do
- **Pick a base table**, then **join** additional tables one at a time on a drag-to-connect canvas.
  Each join links a field on an already-present table to a field on the table you're adding
  (A→B, then AB→C, …).
- **Choose which columns** to show, run a **Preview** to see paginated joined rows, and **Save** the
  mapping so you can execute it again later.
- **Filter** a preview (an ad-hoc query) and pick a **match mode** to control which rows come back.
- **Build a Business Rule** on a saved mapping to filter its joined rows and share them to CTE and/or
  run CRE actions on them — see the "Business Rules" section below. A mapping with no rule on it can
  only be viewed/previewed.

## How it behaves (the logic)
- **Joins are LEFT OUTER and EQUALS-only.** The base row is always kept whether or not a match
  exists on the joined table. You can only join on equality (there's no ">", "contains", etc. for
  joins — those operators are for filters).
- **Every join must connect to the base table or a table joined in an earlier step**, and a table
  can be joined **at most once** (you can't join the same collection twice, and can't join the base
  table to itself).
- **Many-to-many guard:** at least one side of each join must be a **unique** field. Joining two
  non-unique fields is rejected — pick a unique key on one side (this is what keeps the joined view
  meaningful and fast). The guard also holds *after* saving: on the CRE Schema Editor the **Unique**
  toggle of any field a saved mapping uses as a join condition is locked on, and the API refuses the
  edit — turning it off is how a saved mapping ends up matching two non-unique fields. This applies
  whatever the opposite side of the join is, and to every mapping that names the field, so the field
  frees up only once none of them joins on it — the refusal names the mappings (up to three, then
  "and N more"; the core logs all of them). Turning Unique **on** is never blocked.
- **Match mode** (per run, not saved): `all` (every base row), `matched_only` (only rows where every
  joined table matched), or `unmatched_only`. **The default is matched_only**, not all.
- **The joined result is never stored** — only your join definition is saved; rows are recomputed
  and paginated each time you preview or execute. Counts always match the rows actually shown.

## Good to know / gotchas
- **Joining CTE `indicators` needs CTE access.** A CRE-only user can build CRE↔CRE mappings freely,
  but to include Threat Exchange indicators you need Threat Exchange enabled (Settings → General)
  **and** CTE read/write. Without it, the indicators table simply isn't offered, and saved mappings
  that use it are hidden.
- **Filters are preview-only.** A filter you add applies to Preview; a **saved** mapping always
  executes **unfiltered** (and match-mode is chosen per run). Don't expect a saved mapping to
  "remember" a filter or match mode.
- **Columns are a display selection, not a hard projection** — the result still carries every table's
  fields; your selection decides what's shown (and whether multi-value `sources` rows get expanded).
- **Case-insensitive join** is a mapping option; it uses a case-insensitive index under the hood, so
  it stays fast.
- Removing one join is done by editing the join list and re-saving the whole mapping.
- Normalized CRE fields (stored as `{value, plugins}`) can still be joined — CE joins on the field's
  value automatically.
- **A mapping can't be deleted while a rule actively uses it.** Deletion is blocked (409) if any
  Business Rule on it has a non-empty `cteShare` or `creActions`. A rule that merely filters the
  mapping with neither configured does NOT block deletion — remove/disable the rule's sharing/actions
  first, or delete the rule, to free the mapping up.
- **A CRE entity field used by a saved mapping's join, or by a Business Rule's filter/exceptions/field
  mapping/actions, can't be deleted from the Schema Editor** — the delete is blocked with an
  explanatory error rather than silently breaking the mapping/rule. Remove the join or the rule's
  reference to that field first.

## Business Rules (making a mapping DO something)
A saved Unified Mapping only lets you view/preview the joined data — nothing runs
on a schedule until a **Unified Mapping Business Rule** is built on top of it, on
Universal Schema Builder's separate **Business Rules** page. A rule:
- Targets exactly ONE saved mapping (chosen at creation, fixed after — you can't
  repoint a rule at a different mapping later).
- **Filters** that mapping's joined rows with the same visual query builder CTE/CRE
  rules use, built from the mapping's full flattened field list (every table's
  fields, not just the mapping's selected display columns) — plus optional
  **exceptions** (rows to exclude even if they'd otherwise match).
- Can be **muted** (optionally with an auto-unmute time), same as a CTE business
  rule — a muted rule matches nothing until unmuted.
- Wires the matching rows to independent, optional actions:
  - **cteShare** — destination configuration → share actions, pushing matching
    rows to CTE as indicators. Needs a rule-level **field mapping** (IOC fields —
    type, value, and any extra indicator fields — mapped from the joined row's
    columns) since a join row isn't already indicator-shaped; the field mapping
    is required once any `cteShare` is set, editable any time. Requires the
    Threat Exchange module enabled and `cte_write`.
  - **creActions** — CRE configuration → actions to perform, exactly like a
    normal CRE business rule's actions wiring, but evaluated against the joined
    rows instead of one entity's own records. Needs no field mapping (a CRE
    action resolves its own parameters from the row).
  - A rule may do either, both, or neither (neither = scores/evaluates nothing
    useful yet — same "unwired" idea as a CRE rule with no action).
- Runs on a schedule per rule/destination (share) or rule/configuration/action
  (CRE actions), same cadence model as CTE sharing and CRE actions; a manual
  "sync now" is also available for each, scoped to a lookback window in days.
  Manual syncs re-run on every matching row in the window regardless of whether
  it was already acted on; scheduled runs track an act-once marker per row so
  they don't repeat an action on a row already handled.
