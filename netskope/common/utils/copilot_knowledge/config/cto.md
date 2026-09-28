# Ticket Orchestrator (CTO/ITSM) configuration — best practices & field rationale

Guidance for configuring ITSM plugins (ServiceNow, Jira, etc.), business rules,
field mappings, and module settings. Field *definitions* come from the plugin
manifest; this adds the *why*. Confirm specifics against the docs / the plugin
guide (web_search the guide's docSearchQuery for the full vendor steps).

Headings below use the EXACT labels the user sees in the UI. Internal storage
keys appear once in (parentheses) only so raw tool output can be mapped to the
label — never show a storage key to the user.

## Data flow: a SOURCE brings alerts/events in, a DESTINATION creates the tickets

Ticket Orchestrator needs TWO kinds of configuration, and a ticket only happens
when both exist:

- A **source** — brings alerts/events INTO CE for rules to match. The usual source
  is the Netskope tenant (a Netskope-vendor source config, tenant-based, no URL or
  token). Without a source there are no alerts/events, so a business rule has
  nothing to match and no ticket is ever created.
- A **destination** — the ticketing system (ServiceNow, Jira, etc.) where the
  ticket is actually created (a third-party plugin with its own URL + credentials).

A business rule (the FILTER) sits between them: it selects which incoming
alerts/events matter, and the Queues screen routes matched items to a destination.

Because of this, when the user is missing plugins, do NOT assume they only need a
destination to "push tickets." A destination alone has nothing to push — the flow
also needs a source feeding alerts/events. If a business rule already exists but
there are no plugins (or no source), point out that a source is required for the
rule to match anything, and ASK whether they already receive alerts/events from a
source (e.g. their Netskope tenant) or would like to configure the source too — as
well as the destination. Confirm what they intend rather than silently guiding only
one side.

## Per-configuration (plugin) fields

### Authentication step (URL + credentials) — THIRD-PARTY plugins only
Third-party ITSM plugins (ServiceNow, Jira, …) take an instance URL plus an API
token / username-password. These are **secret** fields — the copilot never fills
them; the admin enters them in the form. Many ITSM plugins are multi-step: an
Authentication step, then a Parameters step, then a dynamic step whose options
(tables, queues, projects) are fetched live once credentials are valid.

**Netskope-vendor plugins are the exception**: they authenticate through the
Netskope tenant already configured under **Settings → Netskope Tenants** — the
plugin configuration only *selects* that tenant. There is **no per-config
instance URL or API token field** on a Netskope plugin config; never instruct
the user to enter one. (The tenant itself — URL + token — is configured once,
in Settings, not per plugin.) The plugin's real fields come from its manifest —
ground steps in get_plugin_walkthrough, never in the generic pattern above.

### Destination table / project / queue (often dynamic)
Where tickets are created. These are usually **dynamic** fields — their valid
values are fetched from the ITSM system after credentials are entered, so they
can't be pre-filled; guide the user to enter credentials first, then pick.

### Sync Interval (`pollInterval`)
How often the configuration fetches data from its source. Start at the
recommended cadence; shortening it raises API load on the source.

### Update Incidents back to the Netskope Tenant (`updateIncidents`)
Netskope-tenant configs only: syncs ticket changes (status, assignee, severity)
back to the source incident — fields sync only when they changed since the last
sync. Enabling it also activates the Resolve Incidents workflow: the incident is
marked Resolved on the Netskope tenant when the Resolve Incident option is
selected in the Queue Configuration. Leave off for fire-and-forget ticket
creation.

## Business rules (alert/event → ticket)

### Filters (this is ALL a business rule does)
A CTO/ITSM business rule is FILTERING ONLY: its filter defines WHICH alerts/events
match. It does NOT choose a queue, a destination config, or field mappings — that
wiring is done SEPARATELY in the Queue Configuration window (see "Queues" below).
So a guided setup must keep the rule step (filter) and the queue step (routing +
mappings) as two DISTINCT steps; never ask for queue/mapping input on the rule
screen. Keep filters specific so a broad campaign doesn't create thousands of tickets.

### Deduplication (dedupe fields on the rule)
Collapse related alerts into one ticket (e.g. dedupe by app + user + host). This
is the main defence against "ticket storming". Choose a dedup key that groups
*genuinely related* alerts — too broad merges unrelated incidents and loses
context; too narrow defeats dedup. If the dashboard shows a very high average
dedupe per ticket for one rule, its key is probably too broad.

### Mute rules
Suppress known-benign alert patterns from creating tickets at all — better than
creating-and-closing.

## Field mappings & custom fields
Map Netskope alert fields to ITSM ticket fields (title/description/severity + every
field the ITSM system *requires*, or ticket creation fails). For a THIRD-PARTY ITSM
plugin this is the plugin wizard's **4th step — Mapping Configuration** — so a
third-party plugin walkthrough has FOUR steps (Basic Information → Authentication →
Configuration Parameters → Mapping Configuration); a walkthrough that stops at three
is INCOMPLETE and must be corrected. Use custom-field mappings for non-standard
target fields. (Per-queue field mappings can additionally be set later in the Queue
Configuration window — the queue carries its own mappings; that is a separate screen
from both the plugin wizard and the business rule.)

## Queues (business rule → queue → destination config)
A business rule routes its matched alerts/events onto a **queue**; the ticketing
configuration that owns the queue drains it into tickets on its runs. Mapping
best practices:
- Map each rule to the queue of the destination config that should receive
  those tickets — the queue choice IS the routing decision.
- Prefer one queue per destination/purpose. Fanning unrelated rules into one
  queue makes triage and failure isolation harder; splitting related traffic
  across many queues adds overhead without benefit.
- Dedupe/mute at the RULE (before it routes) is the storm defence — a flood of
  near-duplicate tickets means the rule's dedupe key is too narrow or missing,
  not that the queue is "broken."
- A queue here is a ticketing **routing target** — the CTO analogue of a CTE
  **sharing** definition (rule → queue → destination config). It is NOT the
  platform's internal RabbitMQ message queue, so it has no ready/unacknowledged
  depth or back-pressure to inspect on this page. If a rule's matched
  alerts/events aren't turning into tickets, the cause is the destination config
  failing or under-scheduled (check its run status), or the rule not matching /
  not mapped to a queue — not the queue "itself."

## Module settings (Settings → Ticket Orchestrator)

### Delete Alerts / Delete Events / Delete Tickets (`alertCleanup`, `eventCleanup`, `ticketsCleanup`, in days)
Data retention for alerts, events, and tickets — items older than the given
number of days are deleted. The ticket cleanup query restricts *which* tickets
are eligible for deletion (e.g. only closed ones). Set retention to balance
audit needs against storage; deleting only closed tickets is a safe default for
the ticket query.

### Delete Notifications (`notificationsCleanup` + hours/days unit)
Retention for notification-type tasks (hours or days).
