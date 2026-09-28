"""Constants."""

MAX_IOC_LIMIT = 300000  # 300k
MAX_SIZE_LIMIT = 7340032  # 7 MB
WARN_IOC_LIMIT = MAX_IOC_LIMIT * 0.9  # 270k
WARN_SIZE_LIMIT = MAX_SIZE_LIMIT * 0.9  # 6.3 MB
WARN_RATIO = 0.9
BYTES_IN_MB = 1048576  # 1024 * 1024

# Netskope's MAX_IOC_LIMIT indicator cap applies to every sharing action, but
# the payload limit differs per action and each action consumes only one of the
# two indicator buckets -- so the size is evaluated against that action's own
# bucket and limit rather than against the combined totals.
#
#   bucket     which total the action shares ("url" or "hash")
#   max_size   payload limit in bytes
NETSKOPE_ACTION_LIMITS = {
    # "Add to URL List" -- Policies > Profiles > URL Lists.
    "url": {
        "target": "URL List",
        "item_label": "URL(s)",
        "bucket": "url",
        "max_size": 7 * BYTES_IN_MB,
    },
    # "Add to File Hash List" -- Policies > Profiles > File.
    "file": {
        "target": "File Hash List",
        "item_label": "Filehash(es)",
        "bucket": "hash",
        "max_size": 8 * BYTES_IN_MB,
    },
    # "Add to DNS Profile" -- Policies > Profiles > DNS. Only Domain/FQDN
    # indicators reach the profile, so it consumes the URL bucket.
    "dns_profile": {
        "target": "DNS Profile",
        "item_label": "Domain(s)/FQDN(s)",
        "bucket": "url",
        "max_size": 16 * BYTES_IN_MB,
    },
}
# Actions with no documented payload limit ("Add to Private App", "Add to
# Destination Profile"): only the indicator cap is evaluated. A ``bucket`` of
# None means neither total is tied to the action, so both are checked.
NETSKOPE_DEFAULT_ACTION_LIMITS = {
    "target": None,
    "item_label": "IoC(s)",
    "bucket": None,
    "max_size": None,
}

# Core-level "No Action" pseudo-action for sharing configurations; value is
# namespaced so it can never collide with a plugin-defined action value.
CTE_NO_ACTION_VALUE = "ce_reserved_no_action"
CTE_NO_ACTION_LABEL = "No Action (Generate Alerts Only)"
CTE_ALERT_DISPATCH_BATCH_SIZE = 1000
CTE_FAILED_IOC_QUERY_CHUNK = 10000
# Joined rows converted to indicators per chunk, so a wide unified mapping does
# not hold its whole result set in memory. Mirrors the CRE-side ROW_BATCH_SIZE.
CTE_UM_ROW_CHUNK_SIZE = 1000
