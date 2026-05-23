"""Tenant related endpoints."""
import re
from typing import List, Union
from fastapi import APIRouter, HTTPException, Path, Security


from netskope.common.utils.plugin_provider_helper import convert_tenant_format
from ...models import (PollIntervalUnit, TenantDB, TenantIn, TenantOut,
                       TenantUpdate, User, TenantOldIn, TenantOldUpdate)
from ...utils import (Collections, DBConnector, Logger, PluginHelper,
                      Scheduler, SecretDict, get_dynamic_fields_from_plugin)
from netskope.common.utils.common_pull_scheduler import schedule_or_delete_common_pull_tasks
from .auth import get_current_user
from netskope.integrations import trim_space_parameters_fields

router = APIRouter()
connector = DBConnector()
logger = Logger()
scheduler = Scheduler()
plugin_helper = PluginHelper()


def _update_storage(name: str, storage: dict):
    connector.collection(Collections.NETSKOPE_TENANTS).update_one(
        {"name": name}, {"$set": {"storage": storage}}
    )


@router.get("/tenants", tags=["Tenants"])
async def list_tenants(user: User = Security(get_current_user, scopes=[])) -> List:
    """List all the tenants."""
    out = []
    if "settings_read" in user.scopes:
        for tenant in connector.collection(Collections.NETSKOPE_TENANTS).find({}):
            out.append(TenantOut(**tenant))
    elif set(["cte_read", "cto_read", "cls_read", "cre_read", "edm_read", "cfc_read"]) & set(
        user.scopes
    ):
        for tenant in connector.collection(Collections.NETSKOPE_TENANTS).find({}):
            out.append(
                {"name": tenant.get("name")}
            )
    else:
        raise HTTPException(403, "Not have enough permission to read tenants.")
    return out


@router.post("/tenants", tags=["Tenants"])
async def create_tenant(
    tenant: Union[TenantOldIn, TenantIn],
    user: User = Security(get_current_user, scopes=["settings_write"]),
) -> TenantOut:
    """Create new tenant."""
    if isinstance(tenant, TenantOldIn):
        tenant = TenantDB(**convert_tenant_format(tenant.model_dump()))
    else:
        tenant = TenantDB(**tenant.model_dump())
    PluginClass = plugin_helper.find_by_id(tenant.plugin)
    if PluginClass is None:
        logger.error(
            f"Error occurred while creating the tenant. Could not find the provider plugin with id='{tenant.plugin}'.",
        )
        raise ValueError(
            f"Error occurred while creating the tenant. Could not find the provider plugin with id='{tenant.plugin}'."
        )
    trim_space_parameters_fields(tenant.parameters)
    plugin = PluginClass(tenant.name, tenant.parameters, {}, None, logger)
    try:
        validation_result = plugin.validate(SecretDict(tenant.parameters))
    except ValueError as e:
        raise HTTPException(400, str(e))

    if validation_result.success is False:
        raise HTTPException(
            400,
            f"{validation_result.message}",
        )

    if plugin.storage is not None:
        tenant.storage = plugin.storage

    if validation_result.checkpoint is not None:
        tenant.checkpoint = validation_result.checkpoint

    metadata = PluginClass.metadata
    if metadata.get("netskope", False):
        tenant.pollInterval = 30
        tenant.pollIntervalUnit = PollIntervalUnit.SECONDS

    if metadata and not metadata.get("data"):
        raise ValueError("No source provided inside manifest file.")

    # tenant.checkpoint.alert = datetime.now() #### set in plugin in validate method
    # tenant.checkpoint.event = datetime.now() #### set in plugin in validate method
    name_tenant = tenant.name
    name_tenant = name_tenant.replace(" ", "_")
    connector.collection(Collections.NETSKOPE_TENANTS).insert_one(tenant.model_dump())
    logger.debug(f"New {tenant.name} Netskope tenant has been configured.")
    return tenant.model_dump()


@router.patch("/tenants", tags=["Tenants"])
async def update_tenant(
    tenant: Union[TenantUpdate, TenantOldUpdate],
    user: User = Security(get_current_user, scopes=["settings_write"]),
) -> TenantOut:
    """Update existing tenant."""
    tenant_db_dict = connector.collection(Collections.NETSKOPE_TENANTS).find_one(
        {"name": tenant.name}
    )
    if not tenant_db_dict:
        logger.error(
            f"Error occurred while updating the tenant. Tenant with name '{tenant.name}' does not exist."
        )
        raise ValueError(
            f"Error occurred while updating the tenant. Tenant with name '{tenant.name}' does not exist."
        )
    if isinstance(tenant, TenantOldUpdate):
        updated_tenant_dict = convert_tenant_format(tenant.model_dump(exclude_none=True))
    else:
        updated_tenant_dict = tenant.model_dump(exclude_none=True)

    # merge existing and updated fields
    parameters = {
        **tenant_db_dict["parameters"],
        **updated_tenant_dict.get("parameters", {})
    }
    updated_tenant_dict = {
        **tenant_db_dict,
        **updated_tenant_dict,
    }
    updated_tenant_dict["parameters"] = parameters

    tenant = TenantDB(**updated_tenant_dict)
    PluginClass = plugin_helper.find_by_id(tenant.plugin)
    if PluginClass is None:
        logger.info(
            f"Error occurred while updating the tenant. Could not find the provider plugin with id='{tenant.plugin}'.",
        )
        raise ValueError(
            f"Error occurred while updating the tenant. Could not find the provider plugin with id='{tenant.plugin}'."
        )

    plugin = PluginClass(tenant.name, tenant.parameters, tenant_db_dict.get("storage", {}), None, logger)

    try:
        validation_result = plugin.validate(SecretDict(tenant.parameters))
    except ValueError as e:
        raise HTTPException(400, str(e))

    if validation_result.success is False:
        raise HTTPException(
            400,
            f"{validation_result.message}",
        )

    if plugin.storage is not None:
        tenant.storage = plugin.storage

    if validation_result.checkpoint is not None:
        tenant.checkpoint = validation_result.checkpoint

    metadata = PluginClass.metadata
    if metadata.get("netskope", False):
        tenant.pollInterval = 30
        tenant.pollIntervalUnit = PollIntervalUnit.SECONDS

    if metadata and not metadata.get("data"):
        raise ValueError("No source provided inside manifest file.")

    connector.collection(Collections.NETSKOPE_TENANTS).update_one(
        {"name": tenant.name}, {"$set": tenant.model_dump()}
    )
    schedule_or_delete_common_pull_tasks(tenant.name)
    name_tenant = tenant.name
    name_tenant = name_tenant.replace(" ", "_")
    logger.debug(f"Netskope tenant {tenant.name} has been updated.")
    return tenant


@router.delete("/tenants/{name}", tags=["Tenants"])
async def delete_tenant(
    name: str = Path(...),
    user: User = Security(get_current_user, scopes=["settings_write"]),
):
    """Delete existing tenant."""
    if (
        connector.collection(Collections.ITSM_CONFIGURATIONS).find_one(
            {"tenant": name}
        )
        is not None
        or connector.collection(Collections.CONFIGURATIONS).find_one(
            {"tenant": name}
        )
        or connector.collection(Collections.CLS_CONFIGURATIONS).find_one(
            {"tenant": name}
        )
        is not None
        or connector.collection(Collections.CREV2_CONFIGURATIONS).find_one(
            {"tenant": name}
        )
        is not None
        or connector.collection(Collections.CFC_CONFIGURATIONS).find_one(
            {"tenant": name}
        )
        is not None
        or connector.collection(Collections.EDM_CONFIGURATIONS).find_one(
            {"tenant": name}
        )
        is not None
    ):
        raise HTTPException(400, "This tenant is in use by one of the configurations.")

    if (
        connector.collection(Collections.EDM_HASHES_STATUS).find_one(
            {"fileUploadedAtTenant": name}
        )
        is not None
    ):
        raise HTTPException(
            400, "One of the EDM hash upload status checks is pending for this tenant."
        )

    tenant = connector.collection(Collections.NETSKOPE_TENANTS).find_one({"name": name})
    regex = (r"netskope_provider\.main$")
    if tenant.get("plugin") and re.search(regex, str(tenant.get("plugin")), re.IGNORECASE):
        tenants_count = connector.collection(Collections.NETSKOPE_TENANTS).count_documents({
            "plugin": {"$regex": regex, "$options": "i"}
        })
        if tenants_count == 1:
            settings = connector.collection(Collections.SETTINGS).find_one({})
            if settings and "platforms" in settings:
                for platform_data in settings["platforms"].values():
                    if platform_data is True:
                        raise HTTPException(
                            400,
                            "You cannot delete the tenant. Please disable all the modules before deleting the tenant.",
                        )
    tenant = TenantDB(**tenant)
    PluginClass = plugin_helper.find_by_id(tenant.plugin)
    if PluginClass is None:
        logger.error(
            f"Error occurred while deleting the tenant. Could not find the provider plugin with id='{tenant.plugin}'.",
        )
        raise ValueError(
            f"Error occurred while deleting the tenant. Could not find the provider plugin with id='{tenant.plugin}'."
        )

    plugin = PluginClass(tenant.name, None, tenant.storage, None, logger)

    plugin.cleanup(tenant.parameters)

    metadata = PluginClass.metadata
    if metadata and metadata.get("data"):
        for data in metadata["data"]:
            task_name = f"tenant.{name}.{data}"
            scheduler.delete(task_name)

    connector.collection(Collections.NETSKOPE_TENANTS).delete_one({"name": name})
    connector.collection(Collections.WEBTX_METRICS).delete_one({"name": name})
    logger.debug(f"Netskope tenant {name} has been deleted.")
    return {"success": True}


@router.post(
    "/get_dynamic_fields/{plugin_id}",
    tags=["Plugins Dynamic fields"],
    description="Get the dynamic fields from plugin based on other fields.",
)
async def get_dynamic_fields(
    plugin_id: str,
    config_details: dict,
    user: User = Security(get_current_user, scopes=["settings_write"])
):
    """Get the dynamic fields from plugin."""
    return get_dynamic_fields_from_plugin(plugin_id, config_details)
