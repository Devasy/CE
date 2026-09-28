# System Health dashboard — what the numbers mean

Plain-language glossary for the **System Status** tab (RabbitMQ queues, MongoDB,
node CPU/memory/disk, service status, certificate expiry). Use it to explain a
reading to an admin, flag what's concerning, and recommend an action. Thresholds
below are general guidance, not hard SLAs — confirm specifics in the docs.

## RabbitMQ queues
The message broker CE workers pull jobs from. Backlogs here are the earliest
signal that ingestion/sharing is falling behind.

### queue_messages_ready
Messages waiting to be picked up by a worker. A small, fluctuating value is
normal. **Concerning:** a number that climbs steadily or sits in the thousands —
workers aren't keeping up. **Action:** check worker/node CPU and memory; consider
temporarily throttling low-priority pulls; verify no plugin is stuck retrying.

### queue_messages_unacknowledged
Messages delivered to a worker but not yet acknowledged — work in progress.
Persistently high or growing means consumers are slow or stuck (a long-running
or wedged task). **Action:** correlate with the plugin run status and recent
error logs for the configuration that feeds this queue.

### state
`running` is healthy. Anything else (e.g. a stopped/idle queue while work is
pending) warrants investigating the broker and worker processes.

## MongoDB / replica set
`status: true` (or all replica members healthy) is normal. A primary that is
unreachable or a degraded replica set blocks reads/writes — treat as important.

## Node CPU / memory / disk (system stats)
Per-node resource time series.
- **CPU load average:** brief spikes are fine; sustained saturation (load average
  near or above the processor count for long periods) explains queue backlogs.
- **Memory percent:** sustained high usage (e.g. >85–90%) risks workers being
  killed; pair with backlog symptoms.
- **Disk percent_used:** rising disk is the quiet killer — a full disk halts the
  broker and database. Watch trend, not just the instant value; act well before
  it fills (clean up retention via the module cleanup settings, or scale disk).

## Service status (Core / UI / MongoDB / RabbitMQ)
Each service should be active/up. A down Core or RabbitMQ stops all module work;
a down UI affects only the console. Map a down service to the diagnostic logs.

## certExpiry (ui / mongodb_rabbitmq)
SSL certificate expiry timestamps. An **expired** cert breaks the relevant
integration (UI access, or the Mongo/RabbitMQ TLS handshake). **Action:** renew
well before the date; treat <30 days as a reminder, expired as critical.

> If the user asks "is this healthy / should I worry?", read the live values via
> the dashboard tools, compare against the guidance above, name the one or two
> things worth acting on, and ground any remediation step in the docs.
