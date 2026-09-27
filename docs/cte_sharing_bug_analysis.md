# CTE Sharing Bug Analysis: `sharedWith=[]` with `destinations.status="shared"`

## Overview

A customer reported an IOC showing `SharedWith = []` (empty) while the IOC sources panel
showed `<destination config name>: shared`. The IOC was confirmed active, of type URL, and
Test BR returned 1 matching IOC — yet manual sync did not push it to the Netskope tenant
URL list.

This document covers the full sharing flow, the root cause, reproduction steps, and
workarounds.

---

## Two Separate Fields Track Sharing State

| Field | Location in document | Updated at |
|---|---|---|
| `sharedWith: []` | Root of Indicator document | [share_indicators.py:101–102](../netskope/integrations/cte/tasks/share_indicators.py#L101) via `$addToSet` |
| `sources[].destinations[].status` | Nested per-source | [share_indicators.py:399](../netskope/integrations/cte/tasks/share_indicators.py#L399) (`pending→inprogress`) and [share_indicators.py:719](../netskope/integrations/cte/tasks/share_indicators.py#L719) (`inprogress→shared`) |

These two fields are written **independently**. This is the structural root of the divergence.

---

## Full Sharing Flow

### 1. Maintenance Window (`share_new_indicators=True`)

Triggered by the scheduler periodically. Calls `share_indicators()` with `share_new_indicators=True`.

#### Step 1 — `pending → inprogress` ([line 382–406](../netskope/integrations/cte/tasks/share_indicators.py#L382))

```python
update_many(
    {source: source_config, "destinations": {$elemMatch: {name: dest_config, status: "pending"}}},
    {$set: {"destinations.$[dest].status": "inprogress"}}
)
```

- Transitions **all** pending indicators for this source→destination pair.
- **No `active` check.** An inactive (`active=False`) IOC with `status="pending"` will
  transition to `"inprogress"`.

#### Step 2 — `preliminary_query` gate ([line 534–560](../netskope/integrations/cte/tasks/share_indicators.py#L534))

```python
preliminary_query = {
    "$and": [
        *base_query_conditions,      # BR filters + mute tag rules
        {"sources": {"$elemMatch": {"source": ..., "retracted": false}}},
        {"active": True},            # ← active check is HERE
    ]
}
count_documents = count(preliminary_query)
if count_documents > 0:
    # enter action loop
```

- Requires `active=True`.
- Does **not** check `destinations.status`.

#### Step 3 — `action_query` cursor ([line 588–613](../netskope/integrations/cte/tasks/share_indicators.py#L588))

When `share_new_indicators=True` AND `action_patch_supported=True` (URL_List action):

```python
action_query = {
    "$and": [
        *base_query_conditions,
        {
            "sources": {
                "$elemMatch": {
                    "source": source_config_name,
                    "$or": [{"retracted": False}, {"retracted": {"$exists": False}}],
                    "destinations.name": destination_config_name,   # dot-notation
                    "destinations.status": "inprogress"             # dot-notation
                }
            }
        },
        {"active": True},
    ]
}
```

When `patch_supported=False` or `share_new_indicators=False`, the else-branch is used — no
destinations status check.

#### Step 4 — Push and update `sharedWith` ([line 648–662](../netskope/integrations/cte/tasks/share_indicators.py#L648))

`plugin.push()` is called with a cursor from `action_query`. On success:

```python
# validate_result_and_update(), line 101–102
db.indicators.update_many(filters=action_query, {$addToSet: {sharedWith: dest_config_name}})
```

`sharedWith` is only updated for IOCs that **matched `action_query`**.

#### Step 5 — `inprogress → shared` ([line 701–726](../netskope/integrations/cte/tasks/share_indicators.py#L701))

```python
update_many(
    {
        "value": {"$nin": failed_iocs},
        "sources.$[s].destinations.$[d]": {"name": dest_config, "status": "inprogress"}
    },
    {$set: {"destinations.$[d].status": "shared"}}
)
```

- Marks **all** inprogress indicators as `"shared"`, excluding only those explicitly returned
  as `failed_iocs` by the plugin.
- `failed_iocs` only contains IOCs that were **in the push cursor** and the plugin reported as
  failed. IOCs filtered out of the cursor entirely are **not** in `failed_iocs`.
- Only runs when `action_run_success=True` and `share_new_indicators=True`
  ([line 676](../netskope/integrations/cte/tasks/share_indicators.py#L676)).

---

### 2. Manual Sync (`share_new_indicators=False`)

Triggered from the UI via `POST /business_rules/sync`
([business_rule.py:109–123](../netskope/integrations/cte/routers/business_rule.py#L109)).

- Pushes an entry into the destination config's `manualSync` array in the DB.
- **Not executed immediately** — consumed only when the next maintenance window runs
  ([share_indicators.py:259–281](../netskope/integrations/cte/tasks/share_indicators.py#L259)).
- Calls `share_iocs()` **without** `share_new_indicators=True` (defaults to `False`).

With `share_new_indicators=False`:
- Step 1 (`pending→inprogress`) is **skipped**.
- `action_query` uses the else-branch — **no `destinations.status` check**.
- Step 5 (`inprogress→shared`) is **skipped** (gated on `share_new_indicators=True`).
- Only `sharedWith` (`$addToSet`) is updated if push succeeds.

### 3. Test Business Rule

Calls `build_mongo_query()` ([share_indicators.py:127–184](../netskope/integrations/cte/tasks/share_indicators.py#L127)):

- Checks: source exists, not retracted, matches BR filters, `active=True`, `lastSeen` cutoff.
- **No `destinations.status` check at all.**
- Does not modify the database.
- Returns count of matching URL/hash IOCs.

---

## Condition Checklist for Each Query

| Condition | `pending→inprogress` | `preliminary_query` | `action_query` (patch_supported=True, maint.) | `action_query` (else-branch / manual sync) | Test BR |
|---|:---:|:---:|:---:|:---:|:---:|
| `active=True` | — | ✓ | ✓ | ✓ | ✓ |
| Not retracted | — | ✓ | ✓ | ✓ | ✓ |
| Source matches | ✓ | ✓ | ✓ | ✓ | ✓ |
| BR filters | — | ✓ | ✓ | ✓ | ✓ |
| Mute exceptions (tag-based) | — | ✓ | ✓ | ✓ | ✓ |
| Mute exceptions (filter-based `$nor`) | — | — | — | — | ✓ |
| `destinations.name = dest_config` | — | — | ✓ (dot-notation) | — | — |
| `destinations.status = "inprogress"` | — | — | ✓ (dot-notation) | — | — |
| `destinations.status = "pending"` | ✓ | — | — | — | — |
| `lastSeen` cutoff | — | — | ✓ (if set) | ✓ (if set) | ✓ |

**Note on mute exceptions:** Test BR applies filter-based mute rules via `$nor` correctly.
The actual sharing's `base_query_conditions` is built from `query["$and"]` only — `query["$nor"]`
is silently dropped. Filter-based mute rules are not applied during actual push. This means
muted IOCs (by filter) will still be pushed, while test BR will exclude them.

---

## Root Cause: The Divergence Bug

### The Gap

```
pending → inprogress   [no active check]
            ↓
preliminary_query gate [active=True required]
            ↓
   count_documents = 0 (IOC is inactive)
            ↓
   action loop SKIPPED entirely
   validate_result_and_update() never called
   $addToSet on sharedWith never runs
            ↓
   action_run_success = True  (initial value, never ANDed with anything)
   failed_iocs = []
            ↓
   Line 676: action_run_success=True → failed_iocs_by_destination["netskope"] = []
            ↓
   Lines 701–726: all "inprogress" indicators → "shared"
   IOC was "inprogress" and not in failed_iocs (empty list)
            ↓
   destinations.status = "shared"  ✓
   sharedWith never touched → stays []  ✗
```

An IOC that is **`active=False`** at the time the maintenance window runs will:

1. Transition `pending → inprogress` (no active check).
2. Be excluded from `preliminary_query` (requires `active=True`).
3. Never enter the action loop — never pushed — `sharedWith` not updated.
4. Still get marked `"shared"` by the `inprogress→shared` sweep (it was inprogress, not in
   `failed_iocs`).

If the IOC later becomes `active=True` again (source re-pulls it, `lastSeen` updated):
- `destinations.status = "shared"` — `pending→inprogress` won't pick it up (not "pending").
- `sharedWith = []` — not in the Netskope URL list.
- Test BR finds it (no destinations check).
- Maintenance window skips it every cycle.
- IOC is **permanently stuck**.

---

## Secondary Bug: `already_shared=True` Returns `None`

In `validate_result_and_update()` ([line 70–108](../netskope/integrations/cte/tasks/share_indicators.py#L70)):

```python
if push_result.success is True:
    if not push_result.already_shared:
        # ... $addToSet sharedWith ...
        return True, push_result.should_run_cleanup, [...]
    # ← NO return if success=True but already_shared=True
else:
    return False, push_result.should_run_cleanup, []
```

If the plugin returns `PushResult(success=True, already_shared=True)`, the function
implicitly returns `None`.

The caller at [line 648](../netskope/integrations/cte/tasks/share_indicators.py#L648):
```python
validate_result, should_run_action_cleanup, action_failed_iocs = validate_result_and_update(...)
# ↑ TypeError: cannot unpack non-iterable NoneType
```

This exception is caught silently by `except Exception:` at
[line 694](../netskope/integrations/cte/tasks/share_indicators.py#L694) and logged as
`CTE_1011`. After the exception:

- `action_run_success` is never set to False → remains `True`.
- `failed_iocs` stays `[]`.
- Line 676 fires → `failed_iocs_by_destination["netskope"] = []`.
- Lines 701–726 mark all inprogress IOCs as `"shared"`.
- `sharedWith` is never updated.

Result: same divergence — `destinations.status="shared"`, `sharedWith=[]`.

---

## `$elemMatch` Semantics Issue in `action_query`

In the `action_query` for `patch_supported=True`, the destinations check uses **dot-notation**
inside the sources `$elemMatch`:

```python
"sources": {
    "$elemMatch": {
        "source": source_config_name,
        "destinations.name": destination_config_name,   # resolves across all elements
        "destinations.status": "inprogress"             # resolves across all elements
    }
}
```

MongoDB dot-notation on an array resolves each condition **independently** — they can match
**different** elements. If an IOC has multiple destinations:

```json
"destinations": [
    {"name": "netskope", "status": "shared"},
    {"name": "other_dest", "status": "inprogress"}
]
```

The query passes: `destinations.name:"netskope"` hits element 0, `destinations.status:"inprogress"`
hits element 1. The IOC gets pulled into the push cursor even though its netskope destination
is already `"shared"` — causing a **redundant push** to the Netskope URL list.

The correct form would be a nested `$elemMatch` on destinations:

```python
"destinations": {
    "$elemMatch": {
        "name": destination_config_name,
        "status": "inprogress"
    }
}
```

This is a separate bug that causes extra pushes but does not directly cause `sharedWith=[]`.

---

## Test BR vs Manual Sync vs Maintenance Window

| | Test BR | Manual Sync | Maintenance Window |
|---|---|---|---|
| Executes immediately | Yes | No (queued for next maintenance window) | Scheduled |
| `share_new_indicators` | N/A | `False` | `True` |
| Checks `destinations.status` | No | No (else-branch) | Yes (when `patch_supported=True`) |
| Checks `active=True` | Yes | Yes | Yes (preliminary + action) |
| `pending→inprogress` runs | No | No | Yes |
| `inprogress→shared` runs | No | No | Yes |
| `sharedWith` updated | No | Yes (if push succeeds) | Yes (if push succeeds) |
| Mute exceptions (filter-based) | Correctly excluded | Not excluded | Not excluded |
| DB modified | No | Yes | Yes |
| `lastseen` cutoff applied at | Time of test | Time maintenance window runs | N/A (no `lastseen` in maint.) |

**Timing note on manual sync + `lastseen`:** Test BR computes the `lastSeen` cutoff at the
moment the user clicks Test. Manual sync is queued and consumed at the next maintenance
window — `datetime.now()` is recalculated then. An IOC whose `lastSeen` is near the boundary
of the configured `days` window may pass Test BR but fail the lastseen filter by the time
manual sync actually runs.

---

## How to Reproduce

### Minimal Setup
- 1 source plugin (any plugin that can ingest indicators)
- 1 Netskope destination configured with URL_List action (`patch_supported=True`)
- 1 Business Rule matching URL type indicators

### Steps

1. **Ingest a URL IOC** from the source plugin.
   - Verify: `active=True`, `sources[0].destinations[0].status="pending"`, `sharedWith=[]`

2. **Mark the IOC as `active=False`** via the indicator update API (or direct DB update) before
   the next maintenance window fires.

3. **Trigger or wait for the maintenance window.**

4. **Observe the resulting state:**
   - `sources[0].destinations[0].status = "shared"` ← incorrectly set
   - `sharedWith = []` ← never updated
   - IOC was never pushed to the Netskope URL list

5. **Mark the IOC back to `active=True`** (or re-pull from source to simulate source updating
   `lastSeen` and resetting active status).

6. **Run Test BR** → shows 1 matching IOC.

7. **Trigger manual sync** → queued. When the next maintenance window runs the manual sync:
   - `action_query` uses else-branch (no destinations.status check) → IOC is found → pushed.
   - `sharedWith` is updated — **this is the manual sync workaround**.
   - However, if the push returns `CTE_1007` or `CTE_1011` in logs, the push failed and
     `sharedWith` is still not updated.

---

## Workarounds

### Immediate Fix (Per-IOC, via DB)

Reset the stuck indicator's `destinations.status` back to `"pending"`. The next maintenance
window will pick it up and push it correctly.

```js
db.indicators.updateOne(
    {
        "value": "<the_ioc_value>",
        "sources.source": "<source_config_name>"
    },
    {
        $set: {
            "sources.$[s].destinations.$[d].status": "pending"
        }
    },
    {
        arrayFilters: [
            {"s.source": "<source_config_name>"},
            {"d.name": "<destination_config_name>"}
        ]
    }
)
```

### Bulk Fix (All Affected IOCs)

Find all indicators where `destinations.status="shared"` for a given destination but the
destination name is absent from `sharedWith`, and reset them to `"pending"`:

```js
db.indicators.updateMany(
    {
        "sharedWith": {"$ne": "<destination_config_name>"},
        "sources": {
            "$elemMatch": {
                "source": "<source_config_name>",
                "destinations": {
                    "$elemMatch": {
                        "name": "<destination_config_name>",
                        "status": "shared"
                    }
                }
            }
        }
    },
    {
        $set: {
            "sources.$[s].destinations.$[d].status": "pending"
        }
    },
    {
        arrayFilters: [
            {"s.source": "<source_config_name>"},
            {"d.name": "<destination_config_name>", "d.status": "shared"}
        ]
    }
)
```

### Why Manual Sync May Not Work on a Stuck IOC

Manual sync uses `share_new_indicators=False` which uses the broader action_query (no
destinations.status check). It **will** find the IOC if it is `active=True` and matches the
BR. However it will not work if:

- The maintenance window hasn't consumed the `manualSync` queue yet.
- Push fails → logged as `CTE_1007`.
- Plugin returns `already_shared=True` → `CTE_1011` logged, silent exception, `sharedWith`
  not updated.
- The `lastseen` boundary moved between when the user triggered manual sync and when the
  maintenance window consumed it.

**To diagnose:** search logs for `CTE_1007` or `CTE_1011` at the time of the maintenance
window that ran after the manual sync was triggered.

---

## Code-Level Fixes Required

### Fix 1 — `validate_result_and_update`: Handle `already_shared=True` ([line 73](../netskope/integrations/cte/tasks/share_indicators.py#L73))

Add an explicit return for the `already_shared=True` branch so it does not return `None`:

```python
if push_result.success is True:
    if not push_result.already_shared:
        # ... existing $addToSet logic ...
        return True, push_result.should_run_cleanup, [...]
    else:
        # already shared — not a failure, but nothing to update
        return True, push_result.should_run_cleanup, []
else:
    return False, push_result.should_run_cleanup, []
```

### Fix 2 — `inprogress→shared` should not fire for IOCs not in the push cursor ([line 701–726](../netskope/integrations/cte/tasks/share_indicators.py#L701))

The sweep at lines 701–726 marks every "inprogress" indicator as "shared" regardless of
whether it was actually pushed. The sweep should only apply to IOCs that were part of the
`action_query` cursor. Options:

- Track the values that were in the cursor and use `{"value": {"$in": pushed_values}}` in
  lines 701–726 instead of relying only on `$nin: failed_iocs`.
- Alternatively: after building the cursor at line 616, collect the IOC values that were
  sent to push. Use that set for the `inprogress→shared` update rather than the
  complement-of-failed-iocs approach.

### Fix 3 — Use nested `$elemMatch` on destinations in `action_query` ([line 597–599](../netskope/integrations/cte/tasks/share_indicators.py#L597))

Replace dot-notation with a proper nested `$elemMatch`:

```python
# Current (dot-notation — resolves conditions independently across array elements)
"destinations.name": destination_config_name,
"destinations.status": "inprogress"

# Fixed (nested $elemMatch — both conditions must match the same element)
"destinations": {
    "$elemMatch": {
        "name": destination_config_name,
        "status": "inprogress"
    }
}
```

### Fix 4 — Apply filter-based mute exceptions during actual sharing

`build_mongo_query()` correctly adds filter-based mutes via `$nor`. The inline
`base_query_conditions` in `share_iocs()` only spreads `query["$and"]`, dropping `$nor`.
The fix is to carry `query.get("$nor", [])` into the `action_query` as well.

---

## Summary

| Symptom | Root cause |
|---|---|
| `destinations.status="shared"` but `sharedWith=[]` | IOC was `active=False` during maintenance window; `pending→inprogress` transitioned it, preliminary_query skipped it, `inprogress→shared` swept it anyway |
| `sharedWith=[]` after `already_shared=True` push result | `validate_result_and_update` has no return for this path → `None` → TypeError → silent `CTE_1011` → `inprogress→shared` still fires |
| Test BR finds IOC but maintenance window skips it | `build_mongo_query` has no destinations.status check; maintenance window action_query requires `"inprogress"` which a stuck-at-"shared" IOC never has |
| Manual sync finds IOC but may not push it | Manual sync's `action_query` uses else-branch (no status check), so it does find it — but push failure (`CTE_1007`) or `already_shared=True` bug (`CTE_1011`) can silently block `sharedWith` update |
| IOC pushed redundantly to destination | `$elemMatch` dot-notation semantics match across multiple destinations array elements independently |
