# Plugin tile / run status — what the fields mean

Each configured plugin shows run health on its tile (and in the dashboard plugin
status table). Use this to explain why a plugin looks unhealthy and what to do.

## active (Enabled / Disabled)
Whether the configuration is turned on. A disabled config does nothing and won't
pull/share/sync — expected if intentionally paused, a problem if the admin
thinks it's running.

## delete
Delete a plugin

## lastRunAt (pull / share / sync)
Timestamp of the most recent run for each task type. If `lastRunAt` is far older
than the configured poll interval, the schedule isn't firing — check the
scheduler and whether the config is active or locked.

## lastRunSuccess (pull / share / sync) = false
The most recent run of that task failed. This is the headline "something's wrong"
signal. **Action:** open the audit log for this configuration, find the most
recent error (it carries a `CE_xxxx`/`CTE_xxxx`/`CTO_xxxx` code, details, and
sometimes a resolution), and remediate from there. Common causes: expired/invalid
credentials, the third-party API unreachable (proxy/SSL), or a rate limit.

## lockedAt (pull / share / sync) / "(Running)"
A run is currently in progress (the task is locked so it can't run concurrently).
Normal during a run. **Stale lock:** if `lockedAt` is very old and nothing is
actually running, the worker may have died mid-run — the next cycle usually
clears it; if it persists, investigate worker health.

## checkpoint
The point the plugin has successfully pulled up to. It only advances on a
successful pull, so a checkpoint that hasn't moved alongside `lastRunSuccess.pull
= false` confirms the feed is stalled, not just quiet.

> When the user clicks "Explain status" on a failing tile, read the run-health
> fields + the most recent error log for that config, state the likely cause in
> plain language, and give a cited fix (use the error-code knowledge + docs).
