# Threat Exchange (CTE) dashboard — what the numbers mean

Glossary for the **Threat Exchange** tab: configured plugins, indicator (IoC)
counts, pulled-vs-shared statistics, and top indicators. Use it to interpret a
reading and suggest tuning.

## Total Active IoCs / Reported in last 7 days
Active indicators currently held, and how many arrived recently. A flat or
falling 7-day count for a feed that should be active can mean the source plugin
isn't pulling (check its run status), not that the threat landscape went quiet.

## Pulled IoCs (per source / per type)
Counts of indicators pulled, broken down by source configuration and type
(file, url, host, address), and split into:
- **unretractedCount** — still considered active by the source.
- **retractedCount** — the source has withdrawn them.
A **high retracted-to-unretracted ratio** for a source, especially when IoC
retraction is turned off in CTE settings, means stale indicators are lingering in
enforcement. **Action:** consider enabling retraction (and an interval) so
withdrawn indicators are removed, or review whether the feed is low quality.

## Shared IoCs (filtered / shared / mismatch)
For a business rule + source→destination pair:
- **filteredCount** — indicators that matched the rule's filter.
- **sharedCount** — of those, how many were actually shared to the destination.
- **misMatchCount** = filtered − shared — matched the filter but were NOT shared.
A large **mismatch** usually points at a destination/action misconfiguration or a
plugin that rejected those indicator types. **Action:** check the rule's
`sharedWith` actions and the destination plugin's supported types.

## externalHits (top indicators)
How often non-Netskope tools reported an indicator — a rough proxy for how
valuable/corroborated it is. A pulled source whose indicators consistently show
very low external hits may be low-yield. **Action:** consider deprioritising or
tightening that feed (note: this is a current-snapshot read, not a trend).

## "Pulled but never shared"
If a source's indicators are pulled but no business rule references that source
in its `sharedWith`, the feed is costing storage/processing without reaching any
destination. **Action:** add a sharing rule for it, or disable the source if it
isn't needed.

> These are current-state observations (no historical trend data). Phrase
> suggestions accordingly and keep them advisory — the admin applies any change.
