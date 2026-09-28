"""Starts the FastAPI application."""

import json
import traceback
from netskope.common.api.routers import taskstatus
from fastapi import FastAPI
from pydantic import ValidationError
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response

from ..utils import DBConnector, Collections, Logger
from ..utils.llm_provider_plugin_base import LLMProviderError
from ..utils.requests_retry_mount import install_api_response_guards
from ..models import ErrorMessage
from .routers import (
    auth,
    notifications,
    users,
    logs,
    llm_providers,
    settings,
    repos,
    plugins,
    tenants,
    status,
    fields,
    healthcheck,
    dashboard,
    unified_mapping,
    unified_mapping_rules,
)
from .routers.ai_copilot import ai_usage, analyze, attention, config_copilot
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

# Bound response decompression for outbound requests made by API handlers
# (connectivity checks, repo sync). Guards only — unlike the Celery workers,
# no default timeout is injected here, so existing API behaviour is unchanged.
install_api_response_guards()

DOCS_URL = "/api/docs"
SWAGGER_UI_EXTRA_CSS_URL = f"{DOCS_URL}/swagger-ui-extra.css"

app = FastAPI(
    title="Cloud Threat Exchange API",
    version="3.1.0",
    docs_url=DOCS_URL,
    openapi_url="/api/openapi.json",
)

# ``/api/auth`` authenticates a password grant from username + password alone
# and ignores client_id / client_secret -- an API token has to go through the
# clientCredentials grant instead. Swagger UI nevertheless renders client_id and
# client_secret inputs on the password form as well: the condition in its Oauth2
# component names every flow and there is no setting to turn it off, so they can
# only be removed from the page itself. On the password form they are dead
# controls that read as though an API token would be accepted there. Hide those
# two rows, plus the "Client credentials location" select that exists only to
# place them, leaving each Authorize form asking for one kind of credential.
#
# Each row is a ``div.wrapper`` holding a ``label`` whose ``for`` names the
# field, then a ``section`` holding the input; ids follow ``client_id_${flow}``,
# so the password form's are suffixed ``_password``. The first block removes the
# whole row where ``:has()`` is available. The second needs only the label's
# ``for`` -- always rendered -- and hides the label plus the element beside it,
# so it works without ``:has()`` and regardless of whether the input keeps its
# id. Should a future Swagger UI rename any of this the fields simply reappear:
# cosmetic only, never a functional break.
SWAGGER_UI_EXTRA_CSS = """.auth-container .wrapper:has(> label[for="client_id_password"]),
.auth-container .wrapper:has(> label[for="client_secret_password"]),
.auth-container .wrapper:has(> label[for="password_type"]) {
    display: none;
}
.auth-container label[for="client_id_password"],
.auth-container label[for="client_id_password"] + *,
.auth-container label[for="client_secret_password"],
.auth-container label[for="client_secret_password"] + *,
.auth-container label[for="password_type"],
.auth-container label[for="password_type"] + * {
    display: none;
}
"""

# Linked rather than inlined in a <style> block on purpose. The UI container's
# nginx sends a Content-Security-Policy whose style-src allows inline CSS only
# under a per-request nonce, which it generates itself and the backend never
# sees -- so an inline block is served but then refused by the browser. That
# same style-src allows 'self', so a same-origin stylesheet applies normally.
SWAGGER_UI_EXTRA_CSS_LINK = (
    '    <link type="text/css" rel="stylesheet" '
    f'href="{SWAGGER_UI_EXTRA_CSS_URL}">\n'
).encode()


@app.get(SWAGGER_UI_EXTRA_CSS_URL, include_in_schema=False)
async def swagger_ui_extra_css() -> Response:
    """Serve the Swagger UI overrides as a same-origin stylesheet."""
    return Response(SWAGGER_UI_EXTRA_CSS, media_type="text/css")


@app.exception_handler(ValidationError)
async def validation_exception_handler(request, exc):
    """Handle ValidationError for request."""
    exc_json = json.loads(exc.json())
    detail = []
    for error in exc_json:
        detail.append({"msg": error["msg"]})
    return JSONResponse({"detail": detail}, status_code=422)


@app.exception_handler(LLMProviderError)
async def llm_provider_exception_handler(request: Request, exc: LLMProviderError):
    """Translate a provider/model failure raised by the LLM gateway into an HTTPresponse.

    Keeps the gateway decoupled from FastAPI — it raises the classified
    domain exception and the API boundary maps it to status + detail here.

    (The triage stream handles LLMProviderError itself, surfacing the message in an
    SSE error event, so it never reaches this handler.)
    """
    return JSONResponse({"detail": exc.message}, status_code=exc.http_status)


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
app.include_router(analyze.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(config_copilot.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(attention.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(llm_providers.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(ai_usage.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(repos.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(tenants.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(taskstatus.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(plugins.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(settings.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(status.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(fields.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(healthcheck.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(unified_mapping_rules.router, responses=common_responses, prefix=API_PREFIX)
app.include_router(unified_mapping.router, responses=common_responses, prefix=API_PREFIX)

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


@app.middleware("http")
async def _style_swagger_ui(request: Request, call_next):
    """Link SWAGGER_UI_EXTRA_CSS into the Swagger UI page.

    Done here rather than by replacing the docs route so that FastAPI keeps
    serving /api/docs itself: the page stays byte-for-byte what FastAPI
    generates, apart from the added stylesheet link.
    """
    response = await call_next(request)
    body_iterator = getattr(response, "body_iterator", None)
    if (
        request.url.path != DOCS_URL
        or response.status_code != 200
        or body_iterator is None
    ):
        return response
    body = b"".join([chunk async for chunk in body_iterator])
    return HTMLResponse(
        body.replace(b"</head>", SWAGGER_UI_EXTRA_CSS_LINK + b"</head>", 1),
        # FastAPI sends the docs page with no caching directives, so a browser
        # is free to reuse a copy from before an upgrade and show a stale page.
        # It is one small document fetched by hand, so never store it.
        headers={"Cache-Control": "no-store"},
    )


# loading integration specific routers
for prefix, integration in INTEGRATIONS.items():
    for router in integration.ROUTERS:
        app.include_router(
            router, responses=common_responses, prefix=f"{API_PREFIX}/{prefix}"
        )
