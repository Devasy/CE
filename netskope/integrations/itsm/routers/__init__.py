"""ITSM related routers."""

from . import plugins, configurations, alerts, business_rules, tasks, events, webhooks, dashboard, custom_fields

ROUTERS = [
    plugins.router,
    configurations.router,
    alerts.router,
    business_rules.router,
    tasks.router,
    events.router,
    webhooks.router,
    custom_fields.router,
    dashboard.router,
]
