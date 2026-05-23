#!/bin/bash
# MongoDB AutoCompact Initialization Script
# This script enables autoCompact after MongoDB is ready
# Works for both standalone and HA (replica set) deployments

set -e

# Source load_env.sh to get decrypted passwords
if [ -f "/opt/load_env.sh" ]; then
    source /opt/load_env.sh
fi

# Configuration from environment variables
AUTOCOMPACT_ENABLED="${MONGODB_AUTOCOMPACT_ENABLED:-true}"
AUTOCOMPACT_FREE_SPACE_TARGET_MB="${MONGODB_AUTOCOMPACT_FREE_SPACE_TARGET_MB:-64}"
AUTOCOMPACT_RUN_ONCE="${MONGODB_AUTOCOMPACT_RUN_ONCE:-false}"
MONGO_WAIT_MAX_ATTEMPTS="${MONGODB_WAIT_MAX_ATTEMPTS:-30}"

MONGO_USER="root"
MONGO_PASS="${MAINTENANCE_PASSWORD_ESCAPED}"
MONGO_AUTH_DB="admin"

# Build mongosh connection string
MONGOSH_CMD="mongosh -u ${MONGO_USER} -p '${MONGO_PASS}' --authenticationDatabase ${MONGO_AUTH_DB} --quiet"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [AutoCompact] $1"
}

# Wait for MongoDB to be ready
wait_for_mongo() {
    local attempt=1
    
    log "Waiting for MongoDB to be ready..."
    while [ $attempt -le $MONGO_WAIT_MAX_ATTEMPTS ]; do
        if eval "${MONGOSH_CMD} --eval \"db.adminCommand('ping')\"" > /dev/null 2>&1; then
            log "MongoDB is ready."
            return 0
        fi
        log "Attempt $attempt/$MONGO_WAIT_MAX_ATTEMPTS: MongoDB not ready yet..."
        sleep 5
        attempt=$((attempt + 1))
    done
    
    log "ERROR: MongoDB did not become ready in time."
    return 1
}

# Wait for replica set member to reach PRIMARY or SECONDARY state.
wait_for_replica_ready() {
    # Check whether this mongod is part of a replica set at all.
    local rs_ok
    rs_ok=$(eval "${MONGOSH_CMD} --eval \"
        try {
            var s = db.adminCommand({replSetGetStatus: 1});
            print(s.ok);
        } catch(e) { print('0'); }
    \"" 2>/dev/null | tail -1)

    if [ "$rs_ok" != "1" ]; then
        log "Not in a replica set (or replica set not yet initialised). Skipping replica state wait."
        return 0
    fi

    log "Waiting for replica set member to reach PRIMARY or SECONDARY state (infinite retry)..."
    while true; do
        local my_state
        my_state=$(eval "${MONGOSH_CMD} --eval \"
            try {
                var s = db.adminCommand({replSetGetStatus: 1});
                print(s.myState);
            } catch(e) { print('-1'); }
        \"" 2>/dev/null | tail -1)

        if [ "$my_state" = "1" ] || [ "$my_state" = "2" ]; then
            log "Replica set member reached state $my_state (PRIMARY=1 / SECONDARY=2). Proceeding."
            return 0
        fi

        log "Node is in state '$my_state' waiting for 30 seconds..."
        sleep 30
    done
}

# Enable autoCompact
enable_autocompact() {
    
    if [ "$AUTOCOMPACT_ENABLED" != "true" ]; then
        log "AutoCompact is disabled via configuration (MONGODB_AUTOCOMPACT_ENABLED=${AUTOCOMPACT_ENABLED})."
        return 0
    fi
    
    log "Enabling MongoDB autoCompact..."
    log "  freeSpaceTargetMB: ${AUTOCOMPACT_FREE_SPACE_TARGET_MB}"
    log "  runOnce: ${AUTOCOMPACT_RUN_ONCE}"
    
    # First, disable any existing autoCompact to avoid "already running" error
    log "Disabling any existing autoCompact..."
    eval "${MONGOSH_CMD} --eval \"
        try {
            db.adminCommand({ autoCompact: false });
            print('Existing autoCompact disabled.');
        } catch(e) {
            print('No existing autoCompact to disable: ' + e.message);
        }
    \"" 2>/dev/null || true
    
    # Small delay to ensure previous compaction is fully stopped
    sleep 2
    
    # Enable autoCompact with configured parameters
    log "Enabling autoCompact with configured parameters..."
    local result
    result=$(eval "${MONGOSH_CMD} --eval \"
        var result = db.adminCommand({
            autoCompact: true,
            freeSpaceTargetMB: ${AUTOCOMPACT_FREE_SPACE_TARGET_MB},
            runOnce: ${AUTOCOMPACT_RUN_ONCE}
        });
        printjson(result);
    \"" 2>&1)
    
    log "AutoCompact command result: $result"
    
    if echo "$result" | grep -qE 'ok[[:space:]]*:[[:space:]]*1'; then
        log "AutoCompact enabled successfully on current host."
        return 0
    else
        log "WARNING: AutoCompact may not have been enabled properly. Check result above."
        return 1
    fi
}

# Check current autoCompact status
check_autocompact_status() {
    
    log "Checking autoCompact status..."
    local status
    status=$(eval "${MONGOSH_CMD} --eval \"
        var serverStatus = db.adminCommand({ serverStatus: 1 });
        if (serverStatus && serverStatus.wiredTiger && 'background-compact' in serverStatus.wiredTiger) {
            printjson(serverStatus.wiredTiger['background-compact']);
        } else {
            print('background-compact info not available');
        }
    \"" 2>&1)
    
    log "AutoCompact status: $status"
}

# Main execution
main() {
    log "=========================================="
    log "MongoDB AutoCompact Initialization Script"
    log "=========================================="
    
    # Wait for MongoDB to be ready
    if ! wait_for_mongo; then
        log "Failed to connect to MongoDB. Exiting."
        exit 1
    fi

    if [ -z "${HA_IP_LIST}" ]; then
        log "HA_IP_LIST is not set normal (non-HA) deployment. Skipping replica state wait."
    else
        log "HA_IP_LIST is set, HA deployment detected."
        wait_for_replica_ready
    fi

    # Enable autoCompact
    if enable_autocompact; then
        log "AutoCompact initialization completed successfully."
    else
        log "AutoCompact initialization completed with warnings."
    fi
    
    # Check and log status
    check_autocompact_status
}

main "$@"
