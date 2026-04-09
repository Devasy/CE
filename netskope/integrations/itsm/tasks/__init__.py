"""ITSM related tasks."""

TASKS = [
    "netskope.integrations.itsm.tasks.pull_data_items",
    "netskope.integrations.itsm.tasks.sync_states",
    "netskope.integrations.itsm.tasks.unmute_business_rule",
    "netskope.integrations.itsm.tasks.data_cleanup",
    "netskope.integrations.itsm.tasks.retry",
    "netskope.integrations.itsm.tasks.update_incidents",
    "netskope.integrations.itsm.tasks.audit_requests",
]
