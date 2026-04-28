"""Netskope Fields related endpoint."""

from fastapi import APIRouter, Security, HTTPException
from netskope.common.models.other import NetskopeField
from netskope.common.models import User
from netskope.common.utils import DBConnector, Collections, Logger
from netskope.common.utils import RepoManager, PluginBase
from netskope.common.api.routers.auth import get_current_user

router = APIRouter()
logger = Logger()
connector = DBConnector()
manager = RepoManager()
required_optional_scopes = ["cte_read", "cto_read", "cls_read", "cre_read"]


@router.get(
    "/netskopeFields",
    tags=["Netskope Alerts/Events fields"],
    description="Get the supported netskope alerts/events fields",
)
def get_fields(
    user: User = Security(get_current_user, scopes=[]),
):
    """Get the alerts/events fields."""
    return [
        NetskopeField(**field)
        for field in connector.collection(Collections.NETSKOPE_FIELDS).find({})
    ]


@router.get(
    "/subtypes",
    tags=["Netskope Alerts/Events subtypes"],
    description="Get the supported netskope alert/event subtypes",
)
async def get_subtypes(
    user: User = Security(
        get_current_user,
        scopes=[],
    )
):
    """Get the alerts and events subtypes."""
    for scope in required_optional_scopes:
        if scope in user.scopes:
            break
    else:
        raise HTTPException(403, "Not have enough permission to read subtypes.")

    manager.load(cache=True)
    sub_type_result = {"alerts": {}, "events": {}}
    for plugin in PluginBase.plugins["provider"]:
        try:
            sub_type_obj = plugin.supported_subtypes()
            sub_type_result["alerts"].update(sub_type_obj.get("alerts", {}))
            sub_type_result["events"].update(sub_type_obj.get("events", {}))
        except AttributeError:
            pass
    event_sub_type_reverse_map = {
        value: key for key, value in sub_type_result["events"].items()
    }
    supported_types = PluginBase.supported_types
    supported_types_result = {
        "cte": {},
        "itsm": {},
        "cls": {},
        "cre": {},
        "edm": {},
        "cfc": {},
    }
    for integration, types in supported_types.items():
        for type, sub_types in types.items():
            if type == "events":
                supported_types_result[integration][type] = {
                    event_sub_type_reverse_map[sub_type]: sub_type
                    for sub_type in list(
                        set(sub_types).intersection(
                            set(event_sub_type_reverse_map.keys())
                        )
                    )
                }
            elif type == "alerts":
                supported_types_result[integration][type] = {
                    sub_type: sub_type_result[type][sub_type]
                    for sub_type in list(
                        set(sub_types).intersection(
                            set(sub_type_result[type].keys()))
                    )
                }
    sub_type_result["alerts"] = {
        key: sub_type_result["alerts"][key]
        for key in list(set(
            sub_type_result["alerts"].keys()) - set(["c2", "ips"]))
    }
    return {"all": sub_type_result, **supported_types_result}
