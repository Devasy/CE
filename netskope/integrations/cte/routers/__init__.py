"""Declaration of all routers."""

from . import configurations, indicators, tags, business_rule, dashboard

ROUTERS = [
    configurations.router,
    indicators.router,
    tags.router,
    business_rule.router,
    dashboard.router
]
