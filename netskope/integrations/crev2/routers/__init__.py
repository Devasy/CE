"""All the routers."""

from . import auto_mapper, business_rules, configurations, entities, records, action_logs, dashboard

ROUTERS = [
    configurations.router,
    entities.router,
    records.router,
    business_rules.router,
    action_logs.router,
    dashboard.router,
    auto_mapper.router,
]
