"""Starts the FastAPI application."""

import json
import traceback
from netskope.common.api.routers import taskstatus
from fastapi import FastAPI
from pydantic import ValidationError
from starlette.requests import Request
from starlette.responses import JSONResponse

from ..utils import DBConnector, Collections, Logger
from ..models import ErrorMessage
from .routers import (
    auth,
    notifications,
    users,
    logs,
    settings,
    repos,
    plugins,
    tenants,
    status,
    fields,
    healthcheck,
    dashboard,
)
from ...integrations.cte import routers as cte_routers
from ...integrations.itsm import routers as itsm_routers
from ...integrations.cls import routers as cls_routers
from ...integrations.crev2 import routers as crev2_routers
from ...integrations.edm import routers as edm_routers
from ...integrations.cfc import routers as cfc_routers


INTEGRATIONS = {
    "cte": cte_routers,
    "itsm": itsm_routers,
    "cls": cls_routers,
    "edm": edm_routers,
    "cfc": cfc_routers,
    "cre": crev2_routers,
}

API_PREFIX = "/api"
app = FastAPI(
    title="Cloud Threat Exchange API",
    version="3.1.0",
    docs_url="/api/docs",
    openapi_url="/api/openapi.json",
)


@app.exception_handler(ValidationError)
async def validation_exception_handler(request, exc):
    """Handle ValidationError for request."""
    exc_json = json.loads(exc.json())
    detail = []
    for error in exc_json:
        detail.append({"msg": error['msg']})
    return JSONResponse({"detail": detail}, status_code=422)


common_responses = {
    "400": {"model": ErrorMessage},
    "401": {"model": ErrorMessage},
    "403": {"model": ErrorMessage},
    "405": {"model": ErrorMessage},
    "429": {"model": ErrorMessage},
    "500": {"model": ErrorMessage},
}

# including the common routers
app.include_router(auth.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(dashboard.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(notifications.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(users.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(logs.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(repos.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(tenants.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(taskstatus.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(plugins.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(settings.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(status.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(fields.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(healthcheck.router, responses=common_responses, prefix=API_PREFIX)

connector = DBConnector()
logger = Logger()


@app.middleware("http")
async def _is_integration_enabled(request: Request, call_next):
    uri = request.url.path
    try:
        # Examples for request.url.path
        # //<host>/api/itsm/configurations
        # //<host>/api/notifications/
        integration_prefix = request.url.path.split("/")[4]
        uri = "/".join(request.url.path.split("/")[3:])
        if integration_prefix not in INTEGRATIONS.keys():
            try:
                response = await call_next(request)
                return response
            except Exception:  # NOSONAR
                logger.error(
                    f"Error occurred while processing request for {uri}.",
                    error_code="CE_1041",
                    details=traceback.format_exc(),
                )
                message = (
                    "Error occurred while processing request, check logs for more details. "
                    + "Please try again later."
                )
                return JSONResponse(
                    {"detail": message},
                    500,
                )
        settings = connector.collection(Collections.SETTINGS).find_one(
            {f"platforms.{integration_prefix}": True}
        )
        if not settings:
            return JSONResponse(
                {"detail": f"Integration {integration_prefix} is disabled."},
                400,
            )
    except IndexError:
        pass
    try:
        response = await call_next(request)
        return response
    except Exception:  # NOSONAR
        logger.error(
            f"Error occurred while processing request for {uri}.",
            error_code="CE_1049",
            details=traceback.format_exc(),
        )
        message = (
            "Error occurred while processing request, check logs for more details. "
            + "Please try again later."
        )
        return JSONResponse(
            {"detail": message},
            500,
        )


# loading integration specific routers
for prefix, integration in INTEGRATIONS.items():
    for router in integration.ROUTERS:
        app.include_router(
            router, responses=common_responses, prefix=f"{API_PREFIX}/{prefix}"
        )
