"""Handles the settings related endpoints."""

import os
import traceback
import requests
from datetime import datetime
from requests.packages.urllib3.util.retry import Retry
from fastapi import APIRouter, Security, HTTPException, Header

from ...utils import (
    DBConnector,
    Collections,
    Logger,
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
from netskope.common.utils.requests_retry_mount import _GuardOnlyHTTPAdapter
from netskope.common.utils.settings import VALID_INTEGRATIONS_GROUPS
from netskope.integrations.crev2.utils import THREAT_INDICATORS_ENTITY
from netskope.common.utils.secrets_manager_schemas import (
    get_all_providers,
    get_provider_schema,
    get_secret_path_schema,
)
from ...models import User, SettingsOut, SettingsIn, AccountSettingsIn
from .auth import first_time_user, get_current_user
from .. import __version__
from netskope.common.celery.main import APP

router = APIRouter()
db_connector = DBConnector()
logger = Logger()
UI_SERVICE_NAME = os.environ.get("UI_SERVICE_NAME", "ui")
UI_PROTOCOL = os.environ.get("UI_PROTOCOL", "http")


def disable_cre_entity_business_rules():
    """Mute and lock every CTE business rule that uses a CRE entity.

    Called when the CRE module is being disabled. Each affected rule is muted
    (so it stops sharing) and marked ``disabledByCre`` (so it can't be edited,
    muted/unmuted, synced or deleted). The rule's prior mute state is snapshotted
    into ``creMuteSnapshot`` so it can be restored when CRE is re-enabled.
    """
    result = db_connector.collection(Collections.CTE_BUSINESS_RULES).update_many(
        {
            "entity": {"$nin": [None, THREAT_INDICATORS_ENTITY]},
            "disabledByCre": {"$ne": True},
        },
        [
            {
                "$set": {
                    # Snapshot the prior mute state before overwriting it. Within a
                    # single $set stage all expressions read the pre-update
                    # document, so "$muted"/"$unmuteAt" capture the old values.
                    "creMuteSnapshot": {
                        "muted": {"$ifNull": ["$muted", False]},
                        "unmuteAt": "$unmuteAt",
                    },
                    "disabledByCre": True,
                    "muted": True,
                    "unmuteAt": None,
                }
            }
        ],
    )
    if result.modified_count:
        logger.info(
            f"Disabled {result.modified_count} CTE business rule(s) that use CRE "
            "entities because the CRE module was disabled."
        )


def restore_cre_entity_business_rules():
    """Unlock CTE business rules that were auto-disabled when CRE was turned off.

    Called when the CRE module is being re-enabled. The prior mute state saved in
    ``creMuteSnapshot`` is restored and the lock (``disabledByCre``) is cleared.
    A snapshotted unmute time keeps running while CRE is down, so a mute whose
    deadline already elapsed is restored as unmuted rather than as a mute waiting
    for the next unmute sweep - matching what the rule reported while locked.
    """
    now = datetime.now()
    result = db_connector.collection(Collections.CTE_BUSINESS_RULES).update_many(
        {"disabledByCre": True},
        [
            {
                "$set": {
                    "muteExpired": {
                        "$and": [
                            {
                                "$ne": [
                                    {"$ifNull": ["$creMuteSnapshot.unmuteAt", None]},
                                    None,
                                ]
                            },
                            {"$lte": ["$creMuteSnapshot.unmuteAt", now]},
                        ]
                    }
                }
            },
            {
                "$set": {
                    "muted": {
                        "$cond": [
                            "$muteExpired",
                            False,
                            {"$ifNull": ["$creMuteSnapshot.muted", False]},
                        ]
                    },
                    "unmuteAt": {
                        "$cond": [
                            "$muteExpired",
                            None,
                            {"$ifNull": ["$creMuteSnapshot.unmuteAt", None]},
                        ]
                    },
                    "disabledByCre": False,
                }
            },
            {"$unset": ["creMuteSnapshot", "muteExpired"]},
        ],
    )
    if result.modified_count:
        logger.info(
            f"Re-enabled {result.modified_count} CTE business rule(s) that were "
            "disabled while the CRE module was off."
        )


def disable_ti_business_rules():
    """Mute and lock every CRE business rule that uses the Threat Indicators entity.

    Called when the CTE module is being disabled. Threat Indicators data is owned
    by CTE, so these CRE rules can't function once CTE is off. Each affected rule
    is muted (so it stops evaluating) and marked ``disabledByCte`` (so it can't be
    edited, muted/unmuted, synced or deleted). The prior mute state is snapshotted
    into ``cteMuteSnapshot`` so it can be restored when CTE is re-enabled.
    """
    result = db_connector.collection(Collections.CREV2_BUSINESS_RULES).update_many(
        {
            "entity": THREAT_INDICATORS_ENTITY,
            "disabledByCte": {"$ne": True},
        },
        [
            {
                "$set": {
                    # Snapshot the prior mute state before overwriting it. Within a
                    # single $set stage all expressions read the pre-update
                    # document, so "$muted"/"$unmuteAt" capture the old values.
                    "cteMuteSnapshot": {
                        "muted": {"$ifNull": ["$muted", False]},
                        "unmuteAt": "$unmuteAt",
                    },
                    "disabledByCte": True,
                    "muted": True,
                    "unmuteAt": None,
                }
            }
        ],
    )
    if result.modified_count:
        logger.info(
            f"Disabled {result.modified_count} CRE business rule(s) that use the "
            "Threat Indicators entity because the CTE module was disabled."
        )


def restore_ti_business_rules():
    """Unlock CRE Threat Indicators rules auto-disabled when CTE was turned off.

    Called when the CTE module is being re-enabled. The prior mute state saved in
    ``cteMuteSnapshot`` is restored and the lock (``disabledByCte``) is cleared.
    A snapshotted unmute time keeps running while CTE is down, so a mute whose
    deadline already elapsed is restored as unmuted rather than as a mute waiting
    for the next unmute sweep - matching what the rule reported while locked.
    """
    now = datetime.now()
    result = db_connector.collection(Collections.CREV2_BUSINESS_RULES).update_many(
        {"disabledByCte": True},
        [
            {
                "$set": {
                    "muteExpired": {
                        "$and": [
                            {
                                "$ne": [
                                    {"$ifNull": ["$cteMuteSnapshot.unmuteAt", None]},
                                    None,
                                ]
                            },
                            {"$lte": ["$cteMuteSnapshot.unmuteAt", now]},
                        ]
                    }
                }
            },
            {
                "$set": {
                    "muted": {
                        "$cond": [
                            "$muteExpired",
                            False,
                            {"$ifNull": ["$cteMuteSnapshot.muted", False]},
                        ]
                    },
                    "unmuteAt": {
                        "$cond": [
                            "$muteExpired",
                            None,
                            {"$ifNull": ["$cteMuteSnapshot.unmuteAt", None]},
                        ]
                    },
                    "disabledByCte": False,
                }
            },
            {"$unset": ["cteMuteSnapshot", "muteExpired"]},
        ],
    )
    if result.modified_count:
        logger.info(
            f"Re-enabled {result.modified_count} CRE business rule(s) that were "
            "disabled while the CTE module was off."
        )


def _um_names_using_indicators() -> list:
    """Names of unified mappings that join the CTE indicators collection."""
    return [
        doc["name"]
        for doc in db_connector.collection(Collections.UNIFIED_MAPPING).find(
            {
                "$or": [
                    {"baseTable": Collections.INDICATORS.value},
                    {"joins.rightTable": Collections.INDICATORS.value},
                ]
            },
            {"name": 1},
        )
    ]


_UM_MUTE_SNAPSHOT_FIELD = "moduleMuteSnapshot"


def _disable_um_rules(match_query: dict, own_field: str, other_field: str) -> int:
    """Mute+lock unified mapping rules matching ``match_query``, snapshotting mute state.

    Shared by the CTE- and CRE-disable directions: unlike the classic
    per-collection locks (a CTE_BUSINESS_RULES doc only ever gets
    ``disabledByCre``, a CREV2_BUSINESS_RULES doc only ever ``disabledByCte``),
    a single unified mapping rule can be locked by both flags at once, so
    ``own_field``/``other_field`` swap which is which and share one snapshot
    field (``moduleMuteSnapshot``) — see its callers' docstrings.
    """
    result = db_connector.collection(Collections.UNIFIED_MAPPING_RULES).update_many(
        {**match_query, own_field: {"$ne": True}},
        [
            {
                "$set": {
                    _UM_MUTE_SNAPSHOT_FIELD: {
                        "$cond": [
                            {"$eq": [f"${other_field}", True]},
                            # Already locked by the other module — that lock's
                            # disable call captured the true pre-lock state;
                            # don't clobber it.
                            f"${_UM_MUTE_SNAPSHOT_FIELD}",
                            {
                                "muted": {"$ifNull": ["$muted", False]},
                                "unmuteAt": "$unmuteAt",
                            },
                        ]
                    },
                    own_field: True,
                    "muted": True,
                    "unmuteAt": None,
                }
            }
        ],
    )
    return result.modified_count


def _restore_um_rules(own_field: str, other_field: str) -> int:
    """Unlock unified mapping rules locked by ``own_field``, restoring mute state.

    If ``other_field`` is still set, the rule stays muted and the snapshot is
    kept for that lock's own restore. Otherwise the prior mute state is
    restored — same elapsed-mute handling as restore_ti_business_rules /
    restore_cre_entity_business_rules: a snapshotted unmute time keeps
    running while the module is down, so a mute whose deadline already
    elapsed is restored as unmuted rather than as a mute waiting for the next
    unmute sweep.
    """
    now = datetime.now()
    result = db_connector.collection(Collections.UNIFIED_MAPPING_RULES).update_many(
        {own_field: True},
        [
            {
                "$set": {
                    "muteExpired": {
                        "$and": [
                            {
                                "$ne": [
                                    {"$ifNull": [f"${_UM_MUTE_SNAPSHOT_FIELD}.unmuteAt", None]},
                                    None,
                                ]
                            },
                            {"$lte": [f"${_UM_MUTE_SNAPSHOT_FIELD}.unmuteAt", now]},
                        ]
                    }
                }
            },
            {
                "$set": {
                    "muted": {
                        "$cond": [
                            {"$eq": [f"${other_field}", True]},
                            True,
                            {
                                "$cond": [
                                    "$muteExpired",
                                    False,
                                    {"$ifNull": [f"${_UM_MUTE_SNAPSHOT_FIELD}.muted", False]},
                                ]
                            },
                        ]
                    },
                    "unmuteAt": {
                        "$cond": [
                            {"$eq": [f"${other_field}", True]},
                            None,
                            {
                                "$cond": [
                                    "$muteExpired",
                                    None,
                                    {"$ifNull": [f"${_UM_MUTE_SNAPSHOT_FIELD}.unmuteAt", None]},
                                ]
                            },
                        ]
                    },
                    _UM_MUTE_SNAPSHOT_FIELD: {
                        "$cond": [
                            {"$eq": [f"${other_field}", True]},
                            f"${_UM_MUTE_SNAPSHOT_FIELD}",
                            "$$REMOVE",
                        ]
                    },
                    own_field: False,
                }
            },
            {"$unset": "muteExpired"},
        ],
    )
    return result.modified_count


def disable_ti_unified_mapping_rules():
    """Mute and lock unified mapping rules whose mapping joins CTE indicators.

    Called when the CTE module is disabled. Such a mapping/rule is already
    hidden from the UI (list_unified_mappings/list_unified_mapping_rules), but
    its background schedules are gated per module independently — in
    particular ``cre.um_evaluate_records`` only checks the CRE module, so
    without this it would keep performing CRE actions on CTE-owned rows while
    CTE is off. Mirrors ``disable_ti_business_rules`` for classic CRE rules.
    """
    um_names = _um_names_using_indicators()
    if not um_names:
        return
    modified = _disable_um_rules(
        {"view": {"$in": um_names}}, "disabledByCte", "disabledByCre"
    )
    if modified:
        logger.info(
            f"[Unified Mapping]: Disabled {modified} unified mapping "
            "business rule(s) that use Threat Exchange (Indicators) data because "
            "the CTE module was disabled."
        )


def restore_ti_unified_mapping_rules():
    """Unlock unified mapping rules auto-disabled when CTE was turned off.

    Called when the CTE module is being re-enabled. See ``_restore_um_rules``
    for the restore/elapsed-mute semantics.
    """
    modified = _restore_um_rules("disabledByCte", "disabledByCre")
    if modified:
        logger.info(
            f"[Unified Mapping]: Re-enabled {modified} unified "
            "mapping business rule(s) that were disabled while the CTE module "
            "was off."
        )


def disable_cre_unified_mapping_rules():
    """Mute and lock unified mapping rules whose ``cteShare`` is configured.

    Called when the CRE module is disabled. Every unified mapping joins at
    least one CRE entity collection (the indicators collection cannot join to
    itself), so a rule's ``cteShare`` reads live CRE entity data to build the
    indicators it pushes — same reasoning ``share_cre_entity_rules`` in
    share_indicators.py uses for classic CRE-entity CTE business rules. Rules
    with only ``creActions`` are left alone: ``cre.um_evaluate_records`` is
    already gated by ``@integration("cre")`` and won't run at all.
    """
    modified = _disable_um_rules(
        {"cteShare": {"$exists": True, "$nin": [None, {}]}},
        "disabledByCre",
        "disabledByCte",
    )
    if modified:
        logger.info(
            f"[Unified Mapping]: Disabled {modified} unified mapping "
            "business rule(s) with CTE sharing configured because the CRE "
            "module was disabled."
        )


def restore_cre_unified_mapping_rules():
    """Unlock unified mapping rules auto-disabled when CRE was turned off.

    Called when the CRE module is being re-enabled. See ``_restore_um_rules``
    for the restore/elapsed-mute semantics.
    """
    modified = _restore_um_rules("disabledByCre", "disabledByCte")
    if modified:
        logger.info(
            f"[Unified Mapping]: Re-enabled {modified} unified "
            "mapping business rule(s) that were disabled while the CRE module "
            "was off."
        )


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
        out["aiDataCleanup"] = settings_out.aiDataCleanup
        out["aiStatsCleanup"] = settings_out.aiStatsCleanup
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
    if "settings_read" in user.scopes:
        out["certExpiry"] = settings_out.certExpiry
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
    url = f"{UI_PROTOCOL}://{UI_SERVICE_NAME}:3000/api/management/update-env"  # noqa: E231
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
    session.mount("https://", _GuardOnlyHTTPAdapter(max_retries=retries))
    session.mount("http://", _GuardOnlyHTTPAdapter(max_retries=retries))

    success, response = handle_exception(
        session.put,
        custom_message="Could not update environment file",
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
            error_code="CE_1075",
            details=str(response),
            resolution="The Management Server is not reachable, please verify the following steps:\n Step 1: Check if the Management Server Service is Running by executing below mentioned command. If it's not running, try re-running the setup process to start it again.\n$ systemctl status cloud-exchange\n Step 2: Ensure that port 8000 is allowed in the firewall, as the Management Server runs on this port.",  # noqa
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
            f"Periodic plugin update checking has been "
            f"{'enabled' if settings.enableUpdateChecking else 'disabled'}."
        )
    if settings.platforms is not None:
        enabled_platforms = set([k for k, v in settings.platforms.items() if v])
        if enabled_platforms:
            regex = r"netskope_provider\.main$"
            if (
                db_connector.collection(Collections.NETSKOPE_TENANTS).count_documents(
                    {"plugin": {"$regex": regex, "$options": "i"}}
                )
                == 0
            ):
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
                    "EDM and CFC modules cannot be enabled along with other CE modules (CLS/CTE/CTO/CRE).",
                )
        # DLP modules (EDM/CFC) are not supported on VM-flavour, HA, or Medium profile deployments
        dlp_modules = {"edm", "cfc"}
        # Only block if a DLP module is being *newly* enabled (not when disabling or keeping as-is)
        current_settings = db_connector.collection(Collections.SETTINGS).find_one({})
        current_platforms = current_settings.get("platforms", {}) if current_settings else {}
        currently_enabled_dlp = {m for m in dlp_modules if current_platforms.get(m, False)}
        newly_enabling_dlp = enabled_platforms.intersection(dlp_modules) - currently_enabled_dlp
        if newly_enabling_dlp:
            is_vm_flavour = (
                os.environ.get("CE_AS_VM", "False").strip().strip('"').lower() == "true"
            )
            is_ha_deployment = bool(os.environ.get("HA_IP_LIST"))
            is_medium_profile = (
                os.environ.get("CE_PROFILE", "").strip().strip('"').lower() == "medium"
            )
            if is_vm_flavour or is_ha_deployment or is_medium_profile:
                raise HTTPException(
                    400,
                    "EDM and CFC modules are not supported on containerised HA deployment "
                    "or CE as a VM standalone and HA deployment or medium profile deployment, please switch to "
                    "containerised(Ubuntu and RHEL) standalone deployment with large profile.",
                )

        cre_being_disabled = (
            "cre" in settings.platforms
            and not settings.platforms["cre"]
            and current_platforms.get("cre", False)
        )
        cre_being_enabled = (
            "cre" in settings.platforms
            and settings.platforms["cre"]
            and not current_platforms.get("cre", False)
        )
        cte_being_disabled = (
            "cte" in settings.platforms
            and not settings.platforms["cte"]
            and current_platforms.get("cte", False)
        )
        cte_being_enabled = (
            "cte" in settings.platforms
            and settings.platforms["cte"]
            and not current_platforms.get("cte", False)
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
        # secretsManagerSettings must be saved as a whole document to avoid
        # MongoDB WriteError when the existing params field is null — dot-notation
        # $set cannot traverse null to create sub-fields.
        sm_settings = set_dict.pop("secretsManagerSettings", None)
        update_doc = flatten(set_dict)
        if sm_settings is not None:
            update_doc["secretsManagerSettings"] = sm_settings
        db_connector.collection(Collections.SETTINGS).update_one(
            {}, {"$set": update_doc}
        )

    # AI retention is TTL-driven; when either window changes, reconcile the AI-collection TTL
    # indexes so the new duration takes effect within a TTL sweep (~1 min) instead of on the
    # next migration. Best-effort (never fails the settings save); reads the just-saved doc.
    if settings.aiDataCleanup is not None or settings.aiStatsCleanup is not None:
        try:
            from netskope.common.utils.ai_retention import reconcile_ai_ttls

            reconcile_ai_ttls(db_connector.collection(Collections.SETTINGS).find_one({}))
        except Exception:
            logger.warn(
                "Could not reconcile AI retention TTLs after settings update.",
                details=traceback.format_exc(),
            )

    if settings.platforms is not None:
        if cre_being_disabled:
            disable_cre_entity_business_rules()
            disable_cre_unified_mapping_rules()
        elif cre_being_enabled:
            restore_cre_entity_business_rules()
            restore_cre_unified_mapping_rules()
        if cte_being_disabled:
            disable_ti_business_rules()
            disable_ti_unified_mapping_rules()
        elif cte_being_enabled:
            restore_ti_business_rules()
            restore_ti_unified_mapping_rules()
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


@router.get(
    "/settings/secrets-manager/providers",
    tags=["Settings", "Secrets Manager"],
    description="Get list of available secrets manager providers with their configuration schemas.",
)
async def get_secrets_manager_providers(
    user: User = Security(get_current_user, scopes=["settings_read"]),
):
    """Get all available secrets manager providers and their schemas.

    This endpoint returns the configuration schema for each provider,
    which the UI uses to dynamically render the configuration form.

    Returns:
        dict: {
            "providers": List of provider schemas with fields definitions
        }
    """
    return {"providers": get_all_providers()}


@router.get(
    "/settings/secrets-manager/providers/{provider_id}",
    tags=["Settings", "Secrets Manager"],
    description="Get configuration schema for a specific secrets manager provider.",
)
async def get_secrets_manager_provider_schema(
    provider_id: str,
    user: User = Security(get_current_user, scopes=["settings_read"]),
):
    """Get the configuration schema for a specific provider.

    Args:
        provider_id: Provider identifier (e.g., 'hashicorp', 'azure')

    Returns:
        dict: Provider schema with fields and secret_path_schema

    Raises:
        HTTPException: 404 if provider not found
    """
    schema = get_provider_schema(provider_id)
    if not schema:
        raise HTTPException(404, f"Provider '{provider_id}' not found.")
    return schema


@router.get(
    "/settings/secrets-manager/active-schema",
    tags=["Settings", "Secrets Manager"],
    description="Get the secret path schema for the currently active secrets manager provider.",
)
async def get_active_secret_path_schema(
    user: User = Security(get_current_user, scopes=[]),
):
    """Get the secret path schema for the currently active provider.

    This endpoint is used by plugin configuration and tenant pages to
    determine how to render secret input fields based on the active provider.

    Returns:
        dict: {
            "enabled": bool - whether secrets manager is enabled,
            "provider": str - active provider ID (if enabled),
            "provider_name": str - display name of active provider,
            "schema": dict - secret path schema for the active provider
        }
    """
    settings = db_connector.collection(Collections.SETTINGS).find_one({})
    secrets_settings = settings.get("secretsManagerSettings", {})

    if not secrets_settings.get("enabled", False):
        return {
            "enabled": False,
            "provider": None,
            "provider_name": None,
            "schema": None,
        }

    params = secrets_settings.get("params", {})
    provider_id = params.get("provider")

    if not provider_id:
        return {
            "enabled": False,
            "provider": None,
            "provider_name": None,
            "schema": None,
        }

    provider_schema = get_provider_schema(provider_id)
    secret_path_schema = get_secret_path_schema(provider_id)

    return {
        "enabled": True,
        "provider": provider_id,
        "provider_name": provider_schema.get("name") if provider_schema else provider_id,
        "schema": secret_path_schema,
    }
