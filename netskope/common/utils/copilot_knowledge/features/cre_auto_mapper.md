# CRE Auto-Mapper (AI-assisted field mapping)

> Preview feature — may not be published on docs.netskope.com yet. If a public docs page for this
> exists, prefer it and cite it over this note. This is the fallback grounding until then.

When you configure a Risk Exchange (CRE) plugin, one step is mapping the plugin's entity fields
(e.g. a vendor's "Device" fields) onto a Cloud Exchange **entity** you choose. The **Auto-Mapper**
does that field mapping for you with a single AI call: for the entity you selected, it suggests a
destination for every plugin field — reusing an existing entity field where one fits, or proposing a
**new** field to create where none does.

## What you can do
- On the plugin wizard's **Entity Sources** step, pick the target CE **Entity** (the AI does *not*
  pick the entity — you do), then click **Auto Map with AI**.
- Review the suggested rows. Each row maps a plugin field → an entity field; hover the suggested
  field to see the AI's **reason**. Existing entity fields and proposed **new** fields (shown with a
  "(New)") both appear in the dropdown.
- For proposed new fields, open **"Review All"** (or the per-row warning) to configure each new
  field — its **label**, whether it's **unique** (a merge key), **normalization** (for strings), and
  **aggregate strategy** — then **Create** them.
- **Re-map incrementally:** check the specific rows you want re-suggested and run Auto Map again;
  everything you leave unchecked stays put (it's treated as locked and won't be re-suggested or have
  its destination reused).
- **Save** the configuration when done.

## How it decides (the logic)
- **Type compatibility is a hard gate:** a plugin field is only mapped to an existing entity field
  of the **same type** (string↔string, number↔number, list↔list, boolean↔boolean, datetime↔datetime)
  — regardless of how similar the names are. If nothing type-compatible exists, it proposes a **new**
  field of the right type instead of forcing a bad match.
- **Meaning over names:** among type-compatible candidates it picks by real-world purpose (using the
  field's description + the entity field's sample values); name similarity is only a tie-breaker.
- **Unique / merge keys:** fields that identify a record or can correlate records across plugins are
  suggested as **unique** (map to an existing unique field if one fits, else a new unique field).
  Plain attributes (score, status, name, …) are never made unique. Score/severity/risk metrics
  always get a **new** field.
- **Reuse over duplication:** if the entity already has a compatible field that no other plugin uses
  yet, the AI adopts it rather than creating a near-duplicate.

## Good to know / gotchas
- **Needs an active LLM provider** (Settings → LLM Provider). Without one, Auto-Mapper can't run.
- **New fields must be created before you can Save.** A suggested new field is only a proposal until
  you configure and Create it; saving a mapping that points at a not-yet-created field is blocked.
- **A new field's type is fixed** once created (chosen from the plugin field's type) — you can change
  its label/unique/normalization/aggregate in the review modal, but not its type.
- AI-created fields are tagged as AI-suggested (provenance) but otherwise behave like any entity
  field.
- The mapping the AI returns is validated server-side — type-incompatible suggestions, duplicate
  destinations, or unknown fields are dropped automatically, so a bad suggestion can't be saved.
- If a very large entity's mapping is truncated by the model's token limit, you'll get a clear
  message to map it manually or retry — Auto-Mapper augments manual mapping, it doesn't replace it.
- Suggestions come from the plugin's field definitions + the entity's existing fields/sample values;
  it does not read your live records.
