# EDM Module Architecture Flow and Bug Report

Date: 2026-04-10
Branch analyzed: feat/module-edm

## Scope

This document maps the EDM runtime flow in this repository from source pull to Netskope tenant apply, including:

- Source plugin pull path (Linux File Share EDM)
- Sanitization, normalization, hash generation
- Sharing pipeline and destination plugin upload
- Apply-status polling and cleanup tasks
- Manual upload and Forwarder/Receiver side flow
- Bugs found with exact code lines and reproducible steps

## High-Level Architecture

### Core components

- API entrypoint and EDM router mounting:
  - [netskope/common/api/main.py#L35](netskope/common/api/main.py#L35)
  - [netskope/common/api/main.py#L148](netskope/common/api/main.py#L148)
  - [netskope/integrations/edm/routers/__init__.py#L13](netskope/integrations/edm/routers/__init__.py#L13)
- EDM periodic scheduler task wiring:
  - [netskope/integrations/edm/routers/configurations.py#L403](netskope/integrations/edm/routers/configurations.py#L403)
  - [netskope/integrations/edm/routers/configurations.py#L405](netskope/integrations/edm/routers/configurations.py#L405)
  - [netskope/common/utils/scheduler.py#L39](netskope/common/utils/scheduler.py#L39)
- Runtime tasks:
  - [netskope/integrations/edm/tasks/plugin_lifecycle_task.py#L60](netskope/integrations/edm/tasks/plugin_lifecycle_task.py#L60)
  - [netskope/integrations/edm/tasks/share_data.py#L44](netskope/integrations/edm/tasks/share_data.py#L44)
  - [netskope/integrations/edm/tasks/poll_edm_apply_status.py#L85](netskope/integrations/edm/tasks/poll_edm_apply_status.py#L85)
  - [netskope/integrations/edm/tasks/edm_hashes_aging_task.py#L63](netskope/integrations/edm/tasks/edm_hashes_aging_task.py#L63)

## Mermaid Diagram 1: Scheduled Source to Tenant Flow

```mermaid
flowchart TD
    A[User configures EDM source plugin] --> B[/api/edm/plugins/configurations]
    B --> C[(EDM_CONFIGURATIONS)]
    C --> D[Scheduler registers edm.execute_plugin]

    D --> E[Celery task edm.execute_plugin]
    E --> F[Instantiate source plugin]
    F --> G[LinuxFileShareEDMPlugin.pull]

    G --> G1[SSH connect + SFTP fetch CSV]
    G1 --> G2[Validate CSV header and row shape]
    G2 --> G3{Proceed without sanitization?}
    G3 -->|Yes| G4[Rename CSV to .good]
    G3 -->|No| G5[run_sanitizer -> .good/.bad]
    G4 --> H[generate_csv_edm_hash]
    G5 --> H

    H --> H1[generate_edm_hash -> pdd_metadata_x.tgz/json]
    H1 --> H2[storage.edm_hash_folder + storage.edm_hashes_cfg]
    H2 --> I[(EDM_CONFIGURATIONS.storage update)]

    E --> J[Queue edm.share_data]
    J --> K[Load EDM business sharing mapping]
    K --> L[Resolve destination plugin(s)]
    L --> M[NetskopeEDMPlugin.push]

    M --> M1[Build EDM uploader config]
    M1 --> M2[EDMHashUploader.execute]
    M2 --> M3[DLP EDM API: create staging]
    M3 --> M4[Upload parts]
    M4 --> M5[Complete upload]
    M5 --> M6[Apply uploaded file]

    M6 --> N{Apply accepted?}
    N -->|Yes| O[Insert EDM_HASHES_STATUS polling record]
    N -->|No| P[Mark failed]

    O --> Q[Periodic edm.poll_edm_hash_upload_status]
    Q --> R[GET staging/apply status]
    R --> S{Status}
    S -->|pending/inprogress| T[apply_in_progress]
    S -->|completed| U[completed + remove poll record]
    S -->|error| V[failed + delete staging file]

    J --> W[Finally cleanup source hash dir]
    X[Periodic edm.age_edm_hashes] --> Y[Delete aged EDM files from disk]
```

## Mermaid Diagram 2: Manual Upload and CE Forwarder/Receiver

```mermaid
flowchart TD
    A1[User uploads CSV] --> A2[/api/edm/manual_upload/upload]
    A2 --> A3[Validate CSV rows + <=25 columns]
    A3 --> A4[(EDM_MANUAL_UPLOAD_CONFIGURATIONS)]
    A4 --> A5[Queue edm.execute_manual_upload_task]

    A5 --> A6{Proceed without sanitization?}
    A6 -->|Yes| A7[Rename CSV to .good]
    A6 -->|No| A8[ManualUploadManager.csv_sanitize]
    A7 --> A9[ManualUploadManager.generate_csv_edm_hash]
    A8 --> A9
    A9 --> A10[Destination plugin push]
    A10 --> A11[Optional apply poll registration]

    B1[Remote CE forwarder push] --> B2[/api/edm/nce_upload]
    B2 --> B3[Store received hash bundle by ce_identifier]
    B3 --> B4[share_data(hash_dict=received_hashes)]
    B4 --> B5[Push to local destination configuration(s)]
```

## Stage-by-Stage Code Flow

### 1) Configuration and scheduling

- Configuration CRUD and scheduling:
  - [netskope/integrations/edm/routers/configurations.py#L315](netskope/integrations/edm/routers/configurations.py#L315)
  - [netskope/integrations/edm/routers/configurations.py#L403](netskope/integrations/edm/routers/configurations.py#L403)
  - [netskope/integrations/edm/routers/configurations.py#L405](netskope/integrations/edm/routers/configurations.py#L405)
- Poll interval validation range (12h to 1y):
  - [netskope/integrations/edm/utils/validators.py#L22](netskope/integrations/edm/utils/validators.py#L22)

### 2) Source pull and local validation (Linux File Share EDM)

- Plugin metadata (pull-only source):
  - [netskope/plugins/linux_file_share_edm/manifest.json#L8](netskope/plugins/linux_file_share_edm/manifest.json#L8)
  - [netskope/plugins/linux_file_share_edm/manifest.json#L9](netskope/plugins/linux_file_share_edm/manifest.json#L9)
- Runtime pull entry:
  - [netskope/plugins/linux_file_share_edm/main.py#L677](netskope/plugins/linux_file_share_edm/main.py#L677)
- SSH and file pull:
  - [netskope/plugins/linux_file_share_edm/main.py#L245](netskope/plugins/linux_file_share_edm/main.py#L245)
  - [netskope/plugins/linux_file_share_edm/main.py#L451](netskope/plugins/linux_file_share_edm/main.py#L451)
  - [netskope/plugins/linux_file_share_edm/main.py#L484](netskope/plugins/linux_file_share_edm/main.py#L484)
- CSV validation:
  - [netskope/plugins/linux_file_share_edm/main.py#L487](netskope/plugins/linux_file_share_edm/main.py#L487)
  - [netskope/plugins/linux_file_share_edm/main.py#L533](netskope/plugins/linux_file_share_edm/main.py#L533)
  - [netskope/plugins/linux_file_share_edm/main.py#L539](netskope/plugins/linux_file_share_edm/main.py#L539)
- Storage contract updates:
  - [netskope/plugins/linux_file_share_edm/main.py#L694](netskope/plugins/linux_file_share_edm/main.py#L694)
  - [netskope/plugins/linux_file_share_edm/main.py#L695](netskope/plugins/linux_file_share_edm/main.py#L695)
  - [netskope/plugins/linux_file_share_edm/main.py#L696](netskope/plugins/linux_file_share_edm/main.py#L696)

### 3) Sanitization and normalization

- Sanitization call path:
  - [netskope/plugins/linux_file_share_edm/main.py#L760](netskope/plugins/linux_file_share_edm/main.py#L760)
  - [netskope/plugins/linux_file_share_edm/main.py#L837](netskope/plugins/linux_file_share_edm/main.py#L837)
- Sanitizer core:
  - [netskope/integrations/edm/utils/sanitization.py#L494](netskope/integrations/edm/utils/sanitization.py#L494)
  - [netskope/integrations/edm/utils/sanitization.py#L201](netskope/integrations/edm/utils/sanitization.py#L201)
  - [netskope/integrations/edm/utils/sanitization.py#L278](netskope/integrations/edm/utils/sanitization.py#L278)
  - [netskope/integrations/edm/utils/sanitization.py#L468](netskope/integrations/edm/utils/sanitization.py#L468)

### 4) Hash generation

- Plugin side hash generation wrapper:
  - [netskope/plugins/linux_file_share_edm/main.py#L977](netskope/plugins/linux_file_share_edm/main.py#L977)
  - [netskope/plugins/linux_file_share_edm/main.py#L1031](netskope/plugins/linux_file_share_edm/main.py#L1031)
  - [netskope/plugins/linux_file_share_edm/main.py#L1044](netskope/plugins/linux_file_share_edm/main.py#L1044)
  - [netskope/plugins/linux_file_share_edm/main.py#L1052](netskope/plugins/linux_file_share_edm/main.py#L1052)
- Hash engine internals:
  - [netskope/integrations/edm/utils/edm/hash_generator/edm_hash_generator.py#L158](netskope/integrations/edm/utils/edm/hash_generator/edm_hash_generator.py#L158)
  - [netskope/integrations/edm/utils/edm/hash_generator/edm_hash_generator.py#L233](netskope/integrations/edm/utils/edm/hash_generator/edm_hash_generator.py#L233)
  - [netskope/integrations/edm/utils/edm/hash_generator/edm_hash_generator.py#L490](netskope/integrations/edm/utils/edm/hash_generator/edm_hash_generator.py#L490)
  - [netskope/integrations/edm/utils/edm/hash_generator/edm_hash_generator.py#L966](netskope/integrations/edm/utils/edm/hash_generator/edm_hash_generator.py#L966)

### 5) Sharing and destination upload

- Sharing task and business sharing mapping:
  - [netskope/integrations/edm/tasks/share_data.py#L44](netskope/integrations/edm/tasks/share_data.py#L44)
  - [netskope/integrations/edm/tasks/share_data.py#L132](netskope/integrations/edm/tasks/share_data.py#L132)
  - [netskope/integrations/edm/tasks/share_data.py#L209](netskope/integrations/edm/tasks/share_data.py#L209)
- Netskope EDM destination plugin push:
  - [netskope/plugins/netskope_edm/main.py#L89](netskope/plugins/netskope_edm/main.py#L89)
  - [netskope/plugins/netskope_edm/main.py#L96](netskope/plugins/netskope_edm/main.py#L96)
  - [netskope/plugins/netskope_edm/main.py#L142](netskope/plugins/netskope_edm/main.py#L142)
- EDM uploader internals:
  - [netskope/integrations/edm/utils/edm/edm_uploader/edm_hash_uploader.py#L56](netskope/integrations/edm/utils/edm/edm_uploader/edm_hash_uploader.py#L56)
  - [netskope/integrations/edm/utils/edm/edm_uploader/edm_api_upload.py#L64](netskope/integrations/edm/utils/edm/edm_uploader/edm_api_upload.py#L64)
  - [netskope/integrations/edm/utils/edm/edm_uploader/edm_api_upload.py#L553](netskope/integrations/edm/utils/edm/edm_uploader/edm_api_upload.py#L553)

### 6) Apply status polling and file aging

- Polling task:
  - [netskope/integrations/edm/tasks/poll_edm_apply_status.py#L85](netskope/integrations/edm/tasks/poll_edm_apply_status.py#L85)
  - [netskope/integrations/edm/tasks/poll_edm_apply_status.py#L121](netskope/integrations/edm/tasks/poll_edm_apply_status.py#L121)
- Aged file cleanup:
  - [netskope/integrations/edm/tasks/edm_hashes_aging_task.py#L63](netskope/integrations/edm/tasks/edm_hashes_aging_task.py#L63)
  - [netskope/integrations/edm/tasks/edm_hashes_aging_task.py#L48](netskope/integrations/edm/tasks/edm_hashes_aging_task.py#L48)
  - [netskope/common/models/settings.py#L216](netskope/common/models/settings.py#L216)

## Bug List with Affected Lines and Reproduction Steps

---

### EDM-BUG-001: SSH connection is not guaranteed to close on pull failures

Severity: High

Affected code:

- [netskope/plugins/linux_file_share_edm/main.py#L466](netskope/plugins/linux_file_share_edm/main.py#L466)
- [netskope/plugins/linux_file_share_edm/main.py#L468](netskope/plugins/linux_file_share_edm/main.py#L468)
- [netskope/plugins/linux_file_share_edm/main.py#L485](netskope/plugins/linux_file_share_edm/main.py#L485)

Why this is a bug:

- The SSH connection is opened, then validation and file operations run, but close is not in a finally block.
- If validate_remote_file or SFTP operations fail, close may not run.

Steps to reproduce:

1. Configure a Linux File Share EDM source with valid server_ip, username, password, and port.
2. Set filepath to a non-existent file on the remote host.
3. Trigger pull repeatedly (sync trigger or scheduled runs).
4. Monitor sshd session count on the source host.
5. Observe sessions/channels accumulating until timeout.

Expected result:

- SSH session closes on every failure path.

Actual result:

- Some failure paths skip explicit close.

---

### EDM-BUG-002: Hash artifacts are deleted even when share/upload fails

Severity: High

Affected code:

- [netskope/integrations/edm/tasks/share_data.py#L272](netskope/integrations/edm/tasks/share_data.py#L272)
- [netskope/integrations/edm/tasks/share_data.py#L278](netskope/integrations/edm/tasks/share_data.py#L278)

Why this is a bug:

- cleanup in finally always deletes hash folder regardless of success or failure.
- This blocks retry without re-pull and re-hash.

Steps to reproduce:

1. Configure a valid Linux source and valid sharing mapping.
2. Configure Netskope EDM destination with invalid or expired token.
3. Trigger share (scheduled or sync).
4. Observe share status failed.
5. Check source hash folder path from storage and disk.

Expected result:

- On transient upload/apply failure, hashes should be retained for retry window.

Actual result:

- Hash folder is removed immediately.

---

### EDM-BUG-003: Apply-status polling can remain stuck forever in checking_apply_status

Severity: High

Affected code:

- [netskope/integrations/edm/tasks/poll_edm_apply_status.py#L97](netskope/integrations/edm/tasks/poll_edm_apply_status.py#L97)
- [netskope/integrations/edm/tasks/poll_edm_apply_status.py#L121](netskope/integrations/edm/tasks/poll_edm_apply_status.py#L121)
- [netskope/integrations/edm/tasks/poll_edm_apply_status.py#L122](netskope/integrations/edm/tasks/poll_edm_apply_status.py#L122)
- [netskope/integrations/edm/tasks/poll_edm_apply_status.py#L132](netskope/integrations/edm/tasks/poll_edm_apply_status.py#L132)

Why this is a bug:

- Task sets status to checking_apply_status, then on status API failure uses continue with no retry counter, no fail transition, and no stale-record cleanup.

Steps to reproduce:

1. Complete an upload so a row is inserted into EDM_HASHES_STATUS.
2. Break tenant connectivity or token used by poller.
3. Run the polling task cycle.
4. Inspect task status and EDM_HASHES_STATUS records after multiple cycles.

Expected result:

- After threshold retries, status transitions to failed and stale poll record is cleared.

Actual result:

- Status can remain checking_apply_status indefinitely.

---

### EDM-BUG-004: Manual upload rejects valid CSV files if MIME type is not exactly text/csv

Severity: Medium

Affected code:

- [netskope/integrations/edm/routers/manual_upload.py#L211](netskope/integrations/edm/routers/manual_upload.py#L211)

Why this is a bug:

- Many clients/browsers send CSV as application/vnd.ms-excel.
- Backend checks exact content_type string only.

Steps to reproduce:

1. Run manual upload endpoint.
2. Upload a .csv with content type application/vnd.ms-excel.
3. Example with curl: use multipart file part with type set to application/vnd.ms-excel.

Expected result:

- Backend accepts valid CSV by extension/content parse, not strict single MIME token.

Actual result:

- Returns 400 invalid file type.

---

### EDM-BUG-005: Manual upload has no backend enforcement of the documented 1GB file limit

Severity: Medium

Affected code:

- [netskope/integrations/edm/routers/manual_upload.py#L196](netskope/integrations/edm/routers/manual_upload.py#L196)
- [netskope/integrations/edm/routers/manual_upload.py#L230](netskope/integrations/edm/routers/manual_upload.py#L230)

Why this is a bug:

- Product docs state 1GB max for manual upload.
- Handler has no explicit size guard before writing file to disk.

Steps to reproduce:

1. Prepare a CSV file larger than 1GB.
2. Upload via manual upload endpoint.
3. Observe backend attempts to process/write file instead of immediate size rejection.

Expected result:

- Immediate 400 response with size-limit message.

Actual result:

- No explicit backend size limit check in this path.

---

### EDM-BUG-006: Sanitization uses a shared global processor, creating cross-request race risk

Severity: Medium

Affected code:

- [netskope/integrations/edm/utils/sanitization.py#L491](netskope/integrations/edm/utils/sanitization.py#L491)
- [netskope/integrations/edm/utils/sanitization.py#L500](netskope/integrations/edm/utils/sanitization.py#L500)
- [netskope/integrations/edm/utils/sanitization.py#L501](netskope/integrations/edm/utils/sanitization.py#L501)

Why this is a bug:

- run_sanitizer mutates global gSanitizer.setting for every call.
- Concurrent sanitization calls can overwrite each other state.

Steps to reproduce:

1. Create two source configs with different sanitization_input rules.
2. Trigger /api/edm/edm_sanitization/{plugin_id} for both at the same time.
3. Repeat under load (parallel requests).
4. Compare .good/.bad outputs and behavior per config.

Expected result:

- Each request should be isolated and deterministic.

Actual result:

- Shared mutable state can produce non-deterministic cross-request behavior.

---

### EDM-BUG-007: SSH host key validation is disabled (trust-on-first-use without pinning)

Severity: High (Security)

Affected code:

- [netskope/plugins/linux_file_share_edm/main.py#L245](netskope/plugins/linux_file_share_edm/main.py#L245)

Why this is a bug:

- set_missing_host_key_policy(AutoAddPolicy) accepts unknown host keys automatically.
- This weakens protection against MITM in source pull path.

Steps to reproduce:

1. Place an SSH MITM/proxy or alternate host with different host key on target route.
2. Configure plugin to connect through that path.
3. Trigger validation/pull.

Expected result:

- Unknown or changed host key is rejected unless explicitly trusted/pinned.

Actual result:

- Host key is auto-accepted.

---

### EDM-BUG-008: Action payload is accepted by Netskope EDM push signature but ignored

Severity: Low

Affected code:

- [netskope/plugins/netskope_edm/main.py#L89](netskope/plugins/netskope_edm/main.py#L89)
- [netskope/plugins/netskope_edm/main.py#L255](netskope/plugins/netskope_edm/main.py#L255)

Why this is a bug:

- push method receives action_dict but does not use it.
- This can hide invalid assumptions if action schema is expanded in future.

Steps to reproduce:

1. Trigger /api/edm/business_rules/sync with action body that includes parameters.
2. Repeat with different parameter values.
3. Compare resulting push behavior/logs.

Expected result:

- Either action parameters should be validated and used, or rejected explicitly.

Actual result:

- Behavior is identical because action_dict is ignored.

## Notes for Triage

- Some defects may be partially masked in UI flows due to front-end validation.
- Backend should still enforce key constraints for API safety and operational resilience.
- Status lifecycle aligns with user-facing docs, but failure transitions and retry semantics need hardening.
