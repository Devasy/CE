"""Declaration of all routers."""

from . import (
    business_rule,
    configurations,
    edm_sanitization,
    manual_upload,
    nce_upload,
    task_status,
    dashboard
)

ROUTERS = [
    configurations.router,
    business_rule.router,
    nce_upload.router,
    edm_sanitization.router,
    manual_upload.router,
    task_status.router,
    dashboard.router,
]
