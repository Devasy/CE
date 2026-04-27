"""Declaration of all routers."""

from . import (
    business_rule,
    configurations,
    dashboard,
    image_metadata,
    manual_upload,
    sharing,
    task_status,
)

ROUTERS = [
    business_rule.router,
    configurations.router,
    dashboard.router,
    manual_upload.router,
    sharing.router,
    image_metadata.router,
    task_status.router,
]
