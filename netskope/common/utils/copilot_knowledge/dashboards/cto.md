# Ticket Orchestrator (CTO/ITSM) dashboard — what the numbers mean

Glossary for the **Ticket Orchestrator** tab: configured plugins, alert/event/
ticket counts, deduplication, and ticket status distribution.

## Total Alerts / Events / Tickets
Volume stored and tickets created. Compare tickets-created against alerts-stored:
if almost every alert becomes its own ticket, grouping/dedup may be too loose and
the service desk risks "ticket storming".

## Total Duplicate Tickets (dedupeCount)
The cumulative count of alerts merged into existing tickets by the business
rules' dedup logic. A **high** dedupe count is usually good (related alerts
collapse into one ticket). But a single rule with a very high average dedupe per
ticket can mean its grouping is too broad — unrelated alerts merge and context is
lost. **Action:** review that rule's `dedupeRules`/`dedupeFields`; tighten the
filter or the grouping key.

## Tickets status overview (groupByStatus)
Distribution of tickets by status (new, in_progress, on_hold, closed, failed,
notification, deleted). What to look for:
- A cluster of **failed** — tickets that couldn't be created/synced. Pair with
  the plugin run status and recent logs; often an ITSM auth/permission or
  field-mapping problem.
- Many tickets stuck **pending_approval** for a long time — an approval workflow
  that isn't being actioned.

## syncStatus (per ticket)
`pending → in_progress → success | failed`. A backlog of `failed` syncs, or many
tickets whose `lastUpdatedAt` is newer than `lastSyncedAt` (unsynced changes),
indicates the sync task is failing or delayed. **Action:** check connectivity and
credentials to the ITSM system.

## Rules that never create tickets
A business rule that has produced zero tickets is either filtering nothing that
occurs, has an invalid filter, or targets a disabled configuration/queue.
**Action:** verify the filter is valid and the target config/queue is active;
clean up dead rules.

## Unmapped required fields
If the ITSM system requires fields the mapping doesn't populate, ticket creation
fails. (Detecting which fields are required needs a live `fields` fetch from the
plugin, so this is a partial check.) **Action:** complete the field mapping for
the destination.

> True false-positive detection (e.g. "this rule's tickets are always benign")
> needs a false-positive flag CE doesn't record — out of scope; don't infer it.
