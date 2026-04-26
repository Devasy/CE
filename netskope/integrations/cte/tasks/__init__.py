"""CTE celery tasks."""

TASKS = [
    "netskope.integrations.cte.tasks.indicator_aging_task",
    "netskope.integrations.cte.tasks.plugin_lifecycle_task",
    "netskope.integrations.cte.tasks.share_indicators",
    "netskope.integrations.cte.tasks.unmute_business_rule",
    "netskope.integrations.cte.tasks.ioc_retraction"
]
