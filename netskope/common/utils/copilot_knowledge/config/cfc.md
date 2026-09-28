# Custom File Classification (CFC) configuration — best practices & field rationale

Guidance for configuring CFC plugins, filter business rules, and sharing. Field
*definitions* come from the plugin manifest; this adds the *why*. Headings use the
EXACT UI labels; storage keys appear once in (parentheses). Never show a storage key
to the user.

CFC ingests file/image metadata from a source, FILTERS it with business rules, and
SHARES matching files to a Netskope image CLASSIFIER for training. Filtering and
routing are SEPARATE screens. The flow is: Plugins (+ Directory config) → Business
Rules (filter) → Sharing (rule → classifier on a destination) → verify.

## Per-configuration (plugin) fields — Basic Information

### Configuration Name (`name`)
Unique; immutable after creation.

### Sync Interval (`time` + `unit`) — non-Netskope (source) plugins
How often a source pulls file metadata. Hidden for the Netskope destination plugin.

### Tenant (`tenant`) + Enable SSL verification (`sslValidation`)
Netskope destination plugin only: the tenant classifiers are trained on.

## File / Directory configuration steps
For a file-source plugin the manifest adds **Directory Configuration** (shared
directory name, directory path, filename filter) and a **Preview File Results** step
that previews the matched files (count + size) before you commit.

## Business rules (FILTER only)
A CFC business rule selects WHICH files/images match, built in the VISUAL query
builder (pick a field, an operator and a value per condition, combined with
AND/OR/NOT) over file and image-metadata fields — call get_business_rule_format for
the real fields and their valid operators, and build the conditions only from those.
Do not describe the filter as a query string or a database query. A rule carries the
filter ONLY; it does not route anywhere by itself.

## Sharing (the routing — a SEPARATE screen)
On the **Sharing** screen a rule is mapped to a classifier: each `mappings[]` entry
ties one business rule → a classifier (classifierName / classifierID) + a training
type (positive/negative) on a destination configuration. A rule referenced by NO
sharing mapping is "unwired" (the UI's `mapped=false`). Inspect the mappings with
get_cfc_sharing.

Classifiers come LIVE from the Netskope tenant DLP API (fetched on the Sharing
screen) — a mapping whose classifierID is missing points at a classifier that was
DELETED from the tenant (its mapping carries an errorState). get_cfc_classifiers
lists the classifiers already referenced by existing mappings.

### Manual sync (re-share a source now)
Sharing normally runs on the source's own schedule. When a user wants the matched
files pushed to the classifier IMMEDIATELY — after fixing a mapping, re-pointing a
deleted classifier, or just to confirm the wiring — there is a manual **Sync** action
on the Sharing screen (per source→destination mapping). It re-generates and re-uploads
the hash for that source's sharing. Guide the user to that Sync control; the copilot
never triggers it. It is BLOCKED while a share for that source is already generating or
uploading a hash ("already in progress — try again shortly"), so if Sync appears to do
nothing, an upload is likely already running — check **Sharing and Upload Management**
(/cfc/management) for the live status.

## File Metadata / Sharing and Upload Management
The **File Metadata** screen browses ingested file/image metadata; **Sharing and
Upload Management** (/cfc/management) tracks upload/share status per file.

## Custom File Classification Setting (Settings → Custom File Classification)
A single form (no tabs): **Delete File Metadata** (`cfcImageMetadataCleanup`, days)
— how long ingested file metadata is retained.
