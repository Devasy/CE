# MongoDB Migration Pattern (Alembic-like for NoSQL)

This document outlines the pattern used for running database migrations in NoSQL environments, specifically MongoDB, similar to how Alembic works for relational databases.

## Overview
The migration process uses sequential Python scripts (e.g., in `netskope/common/migrations/`) to apply database schema changes or data transformations. A central script (e.g., `database-migrate.py`) orchestrates these migrations by tracking the applied versions in the database itself.

## Key Components

1. **Migration Scripts:**
   - Named sequentially or by version (e.g., `5.1.0-beta.1.py`, `5.1.0-beta.2.py`).
   - Each script contains an `upgrade(db)` function that performs the specific data transformations or index creations.
   - Example structure:
     ```python
     def upgrade(db):
         # Perform MongoDB operations (e.g., db.collection.update_many(...))
         pass
     ```

2. **Version Tracking:**
   - A dedicated collection (e.g., `migration_history`) in MongoDB stores the identifier of each applied migration script.
   - Before executing a script, the orchestrator checks if its identifier exists in this collection.

3. **Orchestrator (`database-migrate.py`):**
   - Connects to the MongoDB instance.
   - Scans the `migrations/` directory for available scripts.
   - Sorts the scripts sequentially based on version semantics.
   - Checks the `migration_history` collection to find which scripts have not been applied.
   - Iterates through the unapplied scripts, calling their `upgrade(db)` function.
   - Upon successful completion of an `upgrade(db)` function, inserts the script's identifier into the `migration_history` collection.

## Benefits
- **Idempotency:** Migrations can be run multiple times safely. The orchestrator ensures a script is only applied once.
- **Version Control:** Database state changes are version-controlled alongside the application code.
- **Reproducibility:** A fresh installation can be brought up to the latest state by running all migrations sequentially.

## Future Considerations
- Implement a `downgrade(db)` function in migration scripts for rollback capabilities.
- Add error handling to stop the migration process and optionally revert changes if an `upgrade(db)` fails.
