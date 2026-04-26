"""Handles the settings related endpoints."""

import traceback
import os
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from fastapi import APIRouter, Security, HTTPException, Header

from ...utils import (
    DBConnector,
    Collections,
    Logger,
    UpdateManager,
    UpdateException,
    flatten,
)
from .auth import issue_new_token
from netskope.common.utils import bcrypt_utils
from netskope.common.utils.common_pull_scheduler import (
    schedule_or_delete_common_pull_tasks,
)
from netskope.common.utils.password_validator import (
    validate_password_against_policy,
    get_default_policy,
)
from netskope.common.utils.handle_exception import (
    handle_exception,
    handle_status_code,
)
from netskope.common.utils.integrations_tasks_scheduler import (
    schedule_or_delete_integrations_tasks,
)
from netskope.common.utils.proxy import get_proxy_params
from netskope.common.utils.settings import VALID_INTEGRATIONS_GROUPS
from ...models import User, SettingsOut, SettingsIn, AccountSettingsIn
from .auth import first_time_user, get_current_user
from .. import __version__
from netskope.common.celery.main import APP

router = APIRouter()
db_connector = DBConnector()
logger = Logger()
UI_SERVICE_NAME = os.environ.get("UI_SERVICE_NAME", "ui")
UI_PROTOCOL = os.environ.get("UI_PROTOCOL", "http")


@router.get("/updates", tags=["Settings"], description="Check for updates.")
async def check_updates(user: User = Security(get_current_user, scopes=["admin"])):
    """Check for container updates and get changelogs."""
    try:
        manager = UpdateManager()
        return {
            "core": manager.get_changelog("core"),
            "ui": manager.get_changelog("ui"),
        }
    except UpdateException:
        logger.error(
            "Error occurred while checking for updates.",
            details=traceback.format_exc(),
            error_code="CE_1003",
            resolution="Ensure that, Docker hub(https://hub.docker.com/) is accessible from the Cloud Exchange.",
        )
        raise HTTPException(400, "Error occurred while checking for updates.")


@router.get("/update", tags=["Settings"], description="Update core and ui containers.")
async def update(user: User = Security(get_current_user, scopes=["admin"])):
    """Update core and ui containers."""
    try:
        manager = UpdateManager()
        manager.update()
        return {"success": True}
    except UpdateException:
        logger.error(
            "Error occurred while checking for updates.",
            details=traceback.format_exc(),
            error_code="CE_1048",
        )
        raise HTTPException(400, "Error occurred while checking for updates.")


@router.get(
    "/settings",
    tags=["Settings"],
    description="Get settings.",
)
async def read_settings(
    host: str = Header(None),
    user: User = Security(get_current_user, scopes=[]),
):
    """Read current settings.

    Args:
        user (User, optional): The user object. Defaults to Security(get_current_user, scopes=[]).
    """
    out = {}
    settings = db_connector.collection(Collections.SETTINGS).find_one({})

    if "version" not in settings:
        settings["version"] = f"{__version__}"

    if "settings_read" not in user.scopes:
        out["version"] = settings.get("version", f"{__version__}")
        out["databaseVersion"] = settings.get("databaseVersion")
    if "ssosaml" not in settings:
        settings["ssosaml"] = {}
    if "cre" not in settings["platforms"]:
        settings["platforms"]["cre"] = False
    if "cls" not in settings["platforms"]:
        settings["platforms"]["cls"] = False
    if "edm" not in settings["platforms"]:
        settings["platforms"]["edm"] = False
    if "cfc" not in settings["platforms"]:
        settings["platforms"]["cfc"] = False
    settings_out = SettingsOut(
        **settings,
        columns=getattr(user, "columns", {}),
    )
    out["version"] = settings_out.version
    out["databaseVersion"] = settings_out.databaseVersion
    out["platforms"] = settings_out.platforms
    if "cte_read" in user.scopes:
        out["cte"] = settings_out.cte
    if "edm_read" in user.scopes:
        out["edm"] = settings_out.edm
    if "cfc_read" in user.scopes:
        out["cfc"] = settings_out.cfc
    if "cre_read" in user.scopes:
        out["cre"] = settings_out.cre
    if "cls_read" in user.scopes:
        out["cls"] = settings_out.cls
    if "cto_read" in user.scopes:
        out["alertCleanup"] = settings_out.alertCleanup
        out["eventCleanup"] = settings_out.eventCleanup
        out["ticketsCleanup"] = settings_out.ticketsCleanup
        out["ticketsCleanupMongo"] = settings_out.ticketsCleanupMongo
        out["ticketsCleanupQuery"] = settings_out.ticketsCleanupQuery
        out["notificationsCleanup"] = settings_out.notificationsCleanup
        out["notificationsCleanupUnit"] = settings_out.notificationsCleanupUnit
    if "settings_read" in user.scopes:
        out["proxy"] = settings_out.proxy
        out["ssoEnable"] = settings_out.ssoEnable
        out["ssosaml"] = settings_out.ssosaml
        out["logLevel"] = settings_out.logLevel
        out["logsCleanup"] = settings_out.logsCleanup
        out["dataBatchCleanup"] = settings_out.dataBatchCleanup
        out["enableUpdateChecking"] = settings_out.enableUpdateChecking
        out["tasksCleanup"] = settings_out.tasksCleanup
        out["disk_alarm"] = settings_out.disk_alarm
        out["columns"] = settings_out.columns
        out["sslValidation"] = settings_out.sslValidation
        out["emailAddress"] = settings_out.emailAddress
        out["uid"] = settings_out.uid
        out["forceAuth"] = settings_out.forceAuth
        out["secretsManagerSettings"] = settings_out.secretsManagerSettings
        out["passwordPolicy"] = settings_out.passwordPolicy
    out["analyticsServerConnectivity"] = settings_out.analyticsServerConnectivity
    out["username"] = user.username
    out["tourCompleted"] = settings_out.tourCompleted
    return out


def check_permission(settings: SettingsIn, user: User):
    """Check user permission for perform operation."""
    if settings.cte is not None and "cte_write" not in user.scopes:
        raise HTTPException(403, "You don't have permission to save cte settings.")
    elif settings.edm is not None and "edm_write" not in user.scopes:
        raise HTTPException(403, "You don't have permission to save edm settings.")
    elif settings.cfc is not None and "cfc_write" not in user.scopes:
        raise HTTPException(403, "You don't have permission to save cfc settings.")
    elif settings.cre is not None and "cre_write" not in user.scopes:
        raise HTTPException(403, "You don't have permission to save cre settings.")
    elif settings.cls is not None and "cls_write" not in user.scopes:
        raise HTTPException(403, "You don't have permission to save cls settings.")
    elif (
        (settings.alertCleanup is not None)
        or (settings.eventCleanup is not None)
        or (settings.notificationsCleanup is not None)
        or (settings.notificationsCleanupUnit is not None)
        or (settings.ticketsCleanup is not None)
    ) and "cto_write" not in user.scopes:
        raise HTTPException(403, "You don't have permission to save cto settings.")
    elif "settings_write" not in user.scopes:
        raise HTTPException(403, "You don't have permission to save settings.")


def update_env(token, proxy):
    """
    Update environment variables for proxy settings.

    This function updates the environment variables for HTTP and HTTPS proxies
    by making a PUT request to the management API endpoint.

    Args:
        token (str): Authentication token for API requests
        proxy (dict): Dictionary containing proxy settings with 'http' and 'https' keys

    Raises:
        Exception: If there's an error during the API request
    """
    url = f"{UI_PROTOCOL}://{UI_SERVICE_NAME}:3000/api/management/update-env"
    update_data = {
        "CORE_HTTP_PROXY": proxy.get("http", ""),
        "CORE_HTTPS_PROXY": proxy.get("https", ""),
    }
    proxies = {
        "http": None,
        "https": None,
    }
    headers = {"Authorization": f"Bearer {token}"}

    session = requests.Session()
    retries = Retry(total=3, backoff_factor=0.1)
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))

    success, response = handle_exception(
        session.put,
        custom_message="Could not update environment file.",
        url=url,
        json=update_data,
        headers=headers,
        proxies=proxies,
        timeout=30,
        verify=False,
    )
    if not success:
        logger.error(
            message="Error encountered while updating environment file.",
            details=response.with_traceback(),
            resolution="Make sure management server is active",
        )
        raise HTTPException(
            400, "Error occurred while updating environment file. Check logs."
        )

    response = handle_status_code(
        response,
        custom_message="Error encountered while updating environment file. Make sure management server is active",
        log=True,
    )
    logger.info("Successfully updated the environment file with the proxy settings.")


@router.patch("/settings", tags=["Settings"], description="Update global settings.")
async def update_settings(
    settings: SettingsIn,
    user: User = Security(
        get_current_user,
        scopes=[],
    ),
):
    """Update the settings.

    Args:
        request (Request): The Request object.
        settings (SettingsIn): The settings object.
        user (User, optional): The user object. Defaults to Security(get_current_user, scopes=["write"]).
    """
    if settings.columns is not None and (
        set(
            [
                "cte_write",
                "cto_write",
                "cre_write",
                "cls_write",
                "edm_write",
                "cfc_write",
                "settings_write",
            ]
        )
        & set(user.scopes)
    ):
        for key in settings.columns.keys():
            db_connector.collection(Collections.USERS).update_one(
                {"username": user.username},
                {"$set": {f"columns.{key}": settings.columns[key]}},
            )
        return {"columns": settings.columns}
    else:
        check_permission(settings, user)
    # elif settings.alertCleanup is not None
    set_dict = {}
    if settings.proxy is not None:
        proxy = get_proxy_params(settings=settings)
        if not proxy.get("http"):
            proxy.update({"http": ""})
            os.environ.pop("CORE_HTTP_PROXY", None)
            os.environ.pop("HTTP_PROXY", None)
            os.environ.pop("http_proxy", None)
        else:
            os.environ["CORE_HTTP_PROXY"] = proxy["http"]
            os.environ["HTTP_PROXY"] = proxy["http"]
            os.environ["http_proxy"] = proxy["http"]

        if not proxy.get("https"):
            proxy.update({"https": ""})
            os.environ.pop("CORE_HTTPS_PROXY", None)
            os.environ.pop("HTTPS_PROXY", None)
            os.environ.pop("https_proxy", None)
        else:
            os.environ["CORE_HTTPS_PROXY"] = proxy["https"]
            os.environ["HTTPS_PROXY"] = proxy["https"]
            os.environ["https_proxy"] = proxy["https"]

        APP.control.broadcast("reload_environment_variables", arguments={**proxy})
        token = await issue_new_token(user=user)
        update_env(token, proxy=proxy)
    if settings.ssoEnable is not None:
        logger.debug(f"SSO has been {'enabled' if settings.ssoEnable else 'disabled'}.")
    if settings.ssosaml is not None:
        logger.debug("SSO configuration has been updated.")
    if settings.enableUpdateChecking is not None:
        logger.debug(
            f"Periodic update checking has been "
            f"{'enabled' if settings.enableUpdateChecking else 'disabled'}."
        )
    if settings.platforms is not None:
        enabled_platforms = set([k for k, v in settings.platforms.items() if v])
        if enabled_platforms:
            regex = r"netskope_provider\.main$"
            if db_connector.collection(Collections.NETSKOPE_TENANTS).count_documents(
                {"plugin": {"$regex": regex, "$options": "i"}}
            ) == 0:
                raise HTTPException(
                    400,
                    "You need to configure atleast one Netskope tenant before enabling any module.",
                )
        for valid_group in VALID_INTEGRATIONS_GROUPS:
            # checking that there is no invalid platforms enabled together
            if enabled_platforms.intersection(
                valid_group
            ) and not enabled_platforms.issubset(valid_group):
                raise HTTPException(
                    400,
                    "You can not use EDM and CFC with other Cloud Exchange"
                    " integrations.",
                )
        message = "Module status updated."
        enabled = [p.upper() for p in settings.platforms if settings.platforms[p]]
        disabled = [p.upper() for p in settings.platforms if not settings.platforms[p]]
        if enabled:
            message += f" Enabled: {','.join(enabled)}."
        if disabled:
            message += f" Disabled: {','.join(disabled)}."
        message = message.replace("GRC", "ARE")
        logger.debug(message)
    set_dict = settings.model_dump(
        exclude_none=True,
        exclude={
            "cre": {
                "normalizedScoreMappings",  # prevent direct update of this field
                "normalizedScoreHistory",
            },
            "columns": ...,  # save columns with individual users
        },
    )
    # Only set if idpSsoUrl is set as idpSloUrl can be null
    if settings.ssosaml is not None and settings.ssosaml.idpSsoUrl:
        set_dict["ssosaml"]["idpSloUrl"] = settings.ssosaml.idpSloUrl

    if settings.passwordPolicy is not None:
        if "admin" not in user.scopes:
            raise HTTPException(
                status_code=403,
                detail="You do not have permission to update the password policy.",
            )
        if settings.passwordPolicy == "reset":  # Special case for resetting
            policy_data = get_default_policy()
        else:
            policy_data = settings.passwordPolicy.dict()

        set_dict["passwordPolicy"] = policy_data

    if set_dict != {}:
        db_connector.collection(Collections.SETTINGS).update_one(
            {}, {"$set": flatten(set_dict)}
        )

    if settings.platforms is not None:
        schedule_or_delete_common_pull_tasks()
        schedule_or_delete_integrations_tasks(settings)
    if settings.logLevel is not None:
        logger.update_level()
    if settings.cls is not None:
        APP.control.broadcast("reload_cls_utf_8_encoding_flag")
    user_dict = db_connector.collection(Collections.USERS).find_one(
        {"username": user.username}
    )
    if settings.cre:
        start_time = settings.cre.startTime.strftime("%H:%M:%S")
        end_time = settings.cre.endTime.strftime("%H:%M:%S")
        if start_time > end_time:
            start_time, end_time = end_time, start_time
        days = [day.name.title() for day in settings.cre.maintenanceDays]
        logger.debug(
            f"CRE maintenance window has been set from {start_time} UTC to {end_time} UTC hours on {', '.join(days)}."
        )
        if settings.cre.purgeRecords:
            db_connector.collection(Collections.SCHEDULES).update_one(
                {"task": "cre.delete_records"},
                {
                    "$set": {
                        "_cls": "PeriodicTask",
                        "name": "INTERNAL RECORDS PURGING TASK",
                        "enabled": True,
                        "args": [],
                        "task": "cre.delete_records",
                        "interval": {
                            "every": 12,
                            "period": "hours",
                        },
                    }
                },
                upsert=True,
            )
        else:
            db_connector.collection(Collections.SCHEDULES).delete_one(
                {"task": "cre.delete_records"},
            )
    if settings.cte:
        if settings.cte.iocRetraction:
            db_connector.collection(Collections.SCHEDULES).update_one(
                {"task": "cte.ioc_retraction"},
                {
                    "$set": {
                        "_cls": "PeriodicTask",
                        "name": "CTE IoC Retraction Task",
                        "enabled": True,
                        "args": [],
                        "task": "cte.ioc_retraction",
                        "interval": {
                            "every": settings.cte.iocRetractionInterval,
                            "period": "days",
                        },
                    }
                },
                upsert=True,
            )
        else:
            db_connector.collection(Collections.SCHEDULES).delete_one(
                {"task": "cte.ioc_retraction"}
            )
    return SettingsOut(
        **db_connector.collection(Collections.SETTINGS).find_one({}),
        columns={} if user.fromSSO else user_dict.get("columns", {}),
    )


@router.patch("/account", tags=["Settings"], description="Update account settings.")
async def update_account_settings(
    settings: AccountSettingsIn,
    user: User = Security(
        first_time_user,
        scopes=["me"],
    ),
):
    """Update the settings.

    Args:
        settings (SettingsIn): The settings object.
        user (User, optional): The user object. Defaults to Security(get_current_user, scopes=["write"]).
    """
    if user.fromSSO:
        return {}
    set_dict = {}
    user_dict = db_connector.collection(Collections.USERS).find_one(
        {"username": user.username}
    )
    if user_dict is None:
        raise HTTPException(400, "Could not update the password.")
    if not bcrypt_utils.verify_password(settings.oldPassword, user_dict["password"]):
        raise HTTPException(400, "Incorrect password.")
    if settings.oldPassword == settings.newPassword:
        raise HTTPException(400, "New password can not be same as the old password.")
    # Validate new password using the same function as the password policy API
    is_valid, errors = validate_password_against_policy(
        settings.newPassword, user.username
    )

    if not is_valid:
        error_message = "Password does not meet policy requirements: " + ", ".join(
            errors
        )
        raise HTTPException(400, error_message)

    set_dict["password"] = bcrypt_utils.hash_password(settings.newPassword)
    if set_dict != {}:  # i.e. the password was updated
        if user.firstLogin:
            set_dict["firstLogin"] = False
        db_connector.collection(Collections.USERS).update_one(
            {"username": user.username}, {"$set": set_dict}
        )
        if settings.emailAddress is not None and settings.emailAddress != "":
            db_connector.collection(Collections.SETTINGS).update_one(
                {}, {"$set": {"emailAddress": settings.emailAddress}}
            )
        else:
            if "admin" in user_dict["scopes"]:
                db_connector.collection(Collections.SETTINGS).update_one(
                    {}, {"$set": {"emailAddress": ""}}
                )
        logger.debug(f"Password changed for the {user.username} user.")
    return {}


@router.get(
    "/settingsssosenable",
    description="Get ssoEnable status.",
    tags=["Authentication"],
)
async def get_ssoenable_status():
    """Return sso enable status."""
    try:
        return (
            db_connector.collection(Collections.SETTINGS)
            .find_one({})
            .get("ssoEnable", False)
        )
    except Exception:
        return "false"
