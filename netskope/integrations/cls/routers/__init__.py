"""Declaration of all routers."""

from . import business_rules, mappings, configurations, dashboard

ROUTERS = [
    configurations.router,
    mappings.router,
    business_rules.router,
    dashboard.router,
]
