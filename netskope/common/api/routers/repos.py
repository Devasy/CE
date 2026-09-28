"""Plugin repository related endpoints."""

import os
import json
import traceback
import zipfile as zip
import tempfile
import tarfile as tar
from os.path import dirname, join
from datetime import datetime
from typing import List
from fastapi import (
    APIRouter,
    Security,
    Path,
    HTTPException,
    Query,
    UploadFile,
    File,
)
from jsonschema import validate, ValidationError


import netskope.repos
from .auth import get_current_user
from ...models import (
    PluginRepo,
    PluginRepoOut,
    PluginRepoIn,
    User,
    PluginRepoUpdate,
)
from ...utils import (
    RepoManager,
    Logger,
    DBConnector,
    Collections,
    PluginStatus,
    PluginHelper,
    SecretDict,
    Notifier,
)
from ... import api

import netskope.plugins as dir_plugins
from netskope.common.api.routers.tenants import (
    delete_tenant as delete_tenant_configuration
)

from netskope.integrations.cte.routers.configurations import (
    update_configuration as update_cte_configuration,
    delete_configuration as delete_cte_configuration,
)
from netskope.integrations.cte.models import (
    ConfigurationUpdate as CTEConfigurationUpdate,
    ConfigurationDelete as CTEConfigurationDelete,
)
from netskope.integrations.itsm.routers.configurations import (
    update_configuration as update_itsm_configuration,
    delete_configuration as delete_itsm_configuration,
)
from netskope.integrations.itsm.models import (
    ConfigurationUpdate as ITSMConfigurationUpdate,
    ConfigurationDelete as ITSMConfigurationDelete,
)
from netskope.integrations.cls.models import (
    ConfigurationDelete as CLSConfigurationDelete,
    ConfigurationUpdate as CLSConfigurationUpdate,
)
from netskope.integrations.cls.routers.configurations import (
    delete_configuration as delete_cls_configuration,
    update_configuration as update_cls_configuration,
)
from netskope.integrations.crev2.models.configurations import (
    ConfigurationUpdate as CREv2ConfigurationUpdate,
)
from netskope.integrations.crev2.routers.configurations import (
    delete_configuration as delete_crev2_configuration,
    update_configuration as update_crev2_configuration,
)
from netskope.integrations.edm.routers.configurations import (
    update_configuration as update_edm_configuration,
    delete_configuration as delete_edm_configuration
)
from netskope.integrations.edm.models import (
    ConfigurationUpdate as EDMConfigurationUpdate,
    ConfigurationDelete as EDMConfigurationDelete,
)

from netskope.integrations.cfc.routers.configurations import (
    update_configuration as update_cfc_configuration,
    delete_configuration as delete_cfc_configuration
)

from netskope.integrations.cfc.models import (
    ConfigurationUpdate as CFCConfigurationUpdate,
    ConfigurationDelete as CFCConfigurationDelete,
)

from netskope.common.api.routers.llm_providers import (
    update_llm_provider_configuration, delete_llm_provider_configuration
)
from netskope.common.models.llm_provider import (
    LLMProviderUpdate
)


MANIFEST_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "name": {"type": "string", "minLength": 1},
        "id": {"type": "string", "minLength": 1},
        "version": {"type": "string", "minLength": 1},
        "description": {"type": "string", "minLength": 1},
    },
    "required": ["name", "id", "version", "description"],
}


router = APIRouter()
manager = RepoManager()
logger = Logger()
connector = DBConnector()
helper = PluginHelper()
notifier = Notifier()

REPO_STORAGE_PATH = netskope.repos.__path__[0]
PLUGIN_PATH = netskope.plugins.__path__[0]


def update_plugins_updated_at():
    """Update pluginsUpdatedAt to current date and time."""
    connector.collection(Collections.SETTINGS).update_one(
        {}, {"$set": {"pluginsUpdatedAt": datetime.now()}}
    )


@router.get("/repos", tags=["Plugin Repositories"])
async def list_repos(
    user: User = Security(get_current_user, scopes=[])
) -> List[PluginRepoOut]:
    """List all the plugin repos."""
    manager.load(cache=True)
    out = []
    if "settings_read" in user.scopes:
        for repo in manager.repos:
            repo_dict = connector.collection(
                Collections.PLUGIN_REPOS
            ).find_one({"name": repo.name})
            out.append(
                PluginRepoOut(
                    **repo.model_dump(),
                    hasUpdates=repo_dict.get("hasUpdate", False),
                )
            )
    elif set(
        ["cte_read", "cto_read", "cre_read", "cls_read", "edm_read", "cfc_read"]
    ) & set(user.scopes):
        for repo in manager.repos:
            out.append({"name": repo.name})
    else:
        raise HTTPException(403, "Not have enough permission to read repos.")
    return out


@router.get("/repos/{name}/diff", tags=["Plugin Repositories"])
async def get_changelogs(
    name: str = Path(...),
    user: User = Security(get_current_user, scopes=["settings_read"]),
):
    """Get repo diff."""
    repo = list(filter(lambda i: i.name == name, manager.repos))
    if not repo:
        return
    repo = repo.pop()
    response = {"changelogs": manager.get_diff(repo), "plugin_migrates": []}
    try:
        repo_data = connector.collection(Collections.PLUGIN_REPOS).find_one({"name": repo.name})
        if repo_data and "plugin_migrates" in repo_data:
            response["plugin_migrates"] = repo_data.get("plugin_migrates")
    except Exception:
        logger.error(
            f"Error occurred while getting {repo.name} plugin repository.",
            error_code="CE_1137",
            details=traceback.format_exc(),
        )
    return response


@router.post("/repos", tags=["Plugin Repositories"])
async def create_repo(
    repo: PluginRepoIn,
    user: User = Security(
        get_current_user, scopes=["settings_read", "settings_write"]
    ),
) -> PluginRepoOut:
    """Create new plugin repo."""
    try:
        repo_exists = connector.collection(Collections.PLUGIN_REPOS).find_one({"name": repo.name})
        repo_exists = repo_exists is not None

        # Validate the new configuration before applying it
        is_accessible, error_msg, resolution = manager.validate_repo_accessibility(repo)
        if not is_accessible:
            logger.error(
                f"Repository accessibility validation failed for {repo.name}.",
                details=error_msg,
                resolution=resolution,
            )
            raise HTTPException(400, error_msg)

        # Now update the remote URL with validated credentials
        if manager.add_repo(repo, repo_exists=repo_exists):
            connector.collection(Collections.PLUGIN_REPOS).insert_one(
                repo.model_dump()
            )
            update_plugins_updated_at()
            return repo.model_dump()
        else:
            raise HTTPException(
                400,
                "Error occurred while adding plugin repository. Check logs.",
            )
    except HTTPException:
        raise
    except Exception:
        logger.error(
            f"Error occurred while adding {repo.name} plugin repository.",
            error_code="CE_1136",
            details=traceback.format_exc()
        )
        raise HTTPException(
            400, "Error occurred while adding plugin repository. Check logs."
        )


@router.patch("/repos", tags=["Plugin Repositories"])
async def update_repo(
    repo: PluginRepoUpdate,
    user: User = Security(
        get_current_user, scopes=["settings_read", "settings_write"]
    ),
) -> PluginRepoOut:
    """Update existing plugin repo."""
    plugin_repo = PluginRepo(
        **connector.collection(Collections.PLUGIN_REPOS).find_one(
            {"name": repo.name}
        )
    )
    if repo.url != plugin_repo.url:
        raise HTTPException(403, "Repository url cannot be updated.")

    # Validate the new configuration before applying it
    is_accessible, error_msg, resolution = manager.validate_repo_accessibility(repo)
    if not is_accessible:
        logger.error(
            f"Repository accessibility validation failed for {repo.name}.",
            details=error_msg,
            resolution=resolution,
        )
        raise HTTPException(400, error_msg)

    # Now update the remote URL with validated credentials
    origin_update, _ = manager.update(repo, validate_creds=False)
    if not origin_update:
        raise HTTPException(
            400, "Error occurred while updating plugin repository. Check logs."
        )

    updates = repo.model_dump()
    updates["updates"] = {"action": "update_repo"}
    connector.collection(Collections.PLUGIN_REPOS).update_one(
        {"name": repo.name}, {"$set": updates}
    )
    update_plugins_updated_at()
    return repo


@router.delete("/repos/{name}", tags=["Plugin Repositories"])
async def delete_repo(
    name: str = Path(...),
    user: User = Security(get_current_user, scopes=["settings_write"]),
):
    """Delete a plugin repo."""
    if name == "Default":
        raise HTTPException(400, "Can not delete the Default repo.")
    repo = list(filter(lambda i: i.name == name, manager.repos))
    if not repo:
        return {"success": False}
    repo = repo.pop()
    connector.collection(Collections.PLUGIN_REPOS).delete_one(
        {"name": repo.name}
    )
    for configuration in connector.collection(
        Collections.CONFIGURATIONS
    ).find():
        if f".{repo.name}." in configuration["plugin"]:
            logger.debug(
                f"Deleting CTE configuration {configuration['name']} as the plugin is removed."
            )
            await delete_cte_configuration(
                CTEConfigurationDelete(name=configuration["name"]), user
            )
    for configuration in connector.collection(
        Collections.ITSM_CONFIGURATIONS
    ).find():
        if f".{repo.name}." in configuration["plugin"]:
            logger.debug(
                f"Deleting CTE configuration {configuration['name']} as the plugin is removed."
            )
            await delete_itsm_configuration(
                ITSMConfigurationDelete(name=configuration["name"]), user
            )
    for configuration in connector.collection(
        Collections.CREV2_CONFIGURATIONS
    ).find():
        if f".{repo.name}." in configuration["plugin"]:
            logger.debug(
                f"Deleting CRE configuration {configuration['name']} as the plugin is removed."
            )
            await delete_crev2_configuration(
                name=configuration["name"], user=user
            )
    for configuration in connector.collection(
        Collections.CLS_CONFIGURATIONS
    ).find():
        if f".{repo.name}." in configuration["plugin"]:
            logger.debug(
                f"Deleting CLS configuration {configuration['name']} as the plugin is removed."
            )
            await delete_cls_configuration(
                CLSConfigurationDelete(name=configuration["name"]), user
            )

    for configuration in connector.collection(
        Collections.EDM_CONFIGURATIONS
    ).find():
        if f".{repo.name}." in configuration["plugin"]:
            logger.debug(
                f"Deleting EDM configuration {configuration['name']} as the plugin is removed."
            )
            await delete_edm_configuration(
                EDMConfigurationDelete(name=configuration["name"]), user
            )

    for configuration in connector.collection(
        Collections.CFC_CONFIGURATIONS
    ).find():
        if f".{repo.name}." in configuration["plugin"]:
            logger.debug(
                f"Deleting CFC configuration {configuration['name']} as the plugin is removed."
            )
            await delete_cfc_configuration(
                CFCConfigurationDelete(name=configuration["name"]), user
            )

    for tenant in connector.collection(
        Collections.NETSKOPE_TENANTS
    ).find():
        if f".{repo.name}." in tenant["plugin"]:
            logger.debug(
                f"Deleting Netskope Tenant configuration {tenant['name']} as the repo is removed."
            )
            await delete_tenant_configuration(
                tenant['name'], user
            )

    for llm_provider in connector.collection(
        Collections.LLM_PROVIDER_CONFIGURATIONS
    ).find():
        if f".{repo.name}." in llm_provider["plugin"]:
            logger.debug(
                f"Deleting LLM Provider configuration {llm_provider['name']} as the repo is removed."
            )
            await delete_llm_provider_configuration(
                name=llm_provider['name'], user=user
            )
    # Delete Mapping file
    for mapping_files in connector.collection(
        Collections.CLS_MAPPING_FILES
    ).find({"repo": name}):
        logger.info(
            f"Deleting CLS mapping files {mapping_files['name']} as the {name} repo is removed."
        )
        cls_config = connector.collection(
            Collections.CLS_CONFIGURATIONS
        ).find_one(
            {
                "attributeMappingRepo": name,
                "attributeMapping": mapping_files["name"],
            }
        )
        if not cls_config:
            connector.collection(Collections.CLS_MAPPING_FILES).delete_one(
                {"name": mapping_files["name"], "repo": mapping_files["repo"]}
            )
        else:
            logger.info(
                f"Could not delete CLS mapping files {mapping_files['name']} of the {name} repo "
                "as it is used by one of the configurations."
            )
            connector.collection(Collections.CLS_MAPPING_FILES).update_one(
                {"name": mapping_files["name"], "repo": mapping_files["repo"]},
                {"$set": {"isDefault": False}},
            )
    manager.delete_repo(repo)
    update_plugins_updated_at()
    return {"success": True}


def _has_value_in_other_step(
    existing_config: dict, key: str, current_step: dict, field_keys: set = None
) -> bool:
    """Return True if ``key`` is a non-empty field of some step other than ``current_step``.

    Used to decide whether an absent field was previously in use and has merely
    moved location across versions (e.g. ``client_secret`` from the old ``params``
    step to the new ``auth`` step). Only direct fields of sibling step dicts are
    considered, so a coincidental same-named key nested elsewhere is not matched,
    and the current step is excluded.
    """
    if not isinstance(existing_config, dict):
        return False
    field_keys = field_keys or set()
    for name, value in existing_config.items():
        if name in field_keys:
            continue
        if (
            isinstance(value, dict)
            and value is not current_step
            and value.get(key) not in (None, "", [], {})
        ):
            return True
    return False


def _is_field_visible(item: dict, params: dict) -> bool:
    """Honor a field's ``condition`` ({key, values}) against the stored params.

    Mirrors the UI's field-visibility rule. Fields without a ``condition`` are
    always visible; conditional fields are visible only when the stored value of
    ``condition.key`` matches ``condition.values``.
    """
    condition = item.get("condition")
    if not condition:
        return True
    actual = params.get(condition.get("key"))
    values = condition.get("values")
    if isinstance(values, list):
        return actual in values
    return actual == values


def _get_config_diff(
    params: dict,
    expected: dict,
    plugin_id: str,
    existing_config: dict = None,
    field_keys: set = None,
) -> list:
    """Get diff if any between existing configuration and the new one.

    ``existing_config`` is the full stored configuration threaded through the
    recursion. It lets an absent field be surfaced only when it is mandatory or
    was previously populated somewhere (i.e. it moved location), so optional
    fields the user never used are not prompted on upgrade.

    ``field_keys`` is the set of top-level leaf-field keys, computed once from
    the top-level schema and threaded unchanged through the recursion. It lets
    ``_has_value_in_other_step`` tell a sibling step apart from a field whose
    stored value is itself a dict.
    """
    if existing_config is None:
        existing_config = params
    if field_keys is None:
        field_keys = {
            item["key"]
            for item in expected
            if item.get("key") and item.get("type") not in ("step", "dynamic_step")
        }
    diff = []
    for item in expected:
        is_already_added = False
        if not _is_field_visible(item, params):
            continue
        key = item.get("key")
        key_absent = key is not None and key not in params
        type_mismatch = (
            key is not None
            and key in params
            and (
                (
                    item["type"] in ["password", "text", "choice"]
                    and type(params[key]) is not str
                )
                or (
                    item["type"] == "number"
                    and params[key] != ""
                    and type(params[key]) is not int
                )
                or (
                    item["type"] == "multichoice"
                    and (
                        type(params[key]) is not list
                        or (
                            item.get("choices")
                            and any(
                                value not in {c.get("value") for c in item["choices"]}
                                for value in params[key]
                            )
                        )
                    )
                )
                or (
                    item["type"] == "choice"
                    and item.get("choices")
                    and params[key]
                    and params[key] not in {c.get("value") for c in item["choices"]}
                )
            )
        )
        if key_absent:
            # Surface an absent field only when it is mandatory or was
            # previously populated in another step (i.e. it has moved location
            # and must be re-entered). This avoids prompting for optional fields
            # the user never used under their chosen settings.
            if item.get("mandatory") or _has_value_in_other_step(
                existing_config, key, params, field_keys
            ):
                is_already_added = True
                diff.append(item)
        elif type_mismatch:
            is_already_added = True
            diff.append(item)
        elif item["type"] == "step":
            expected_in_step = _get_config_diff(
                params.get(item["name"], {}),
                item["fields"],
                plugin_id,
                existing_config,
                field_keys,
            )
            if expected_in_step:
                item = item.copy()
                item["fields"] = expected_in_step
                diff.append(item)
        elif item["type"] == "dynamic_step":
            if not params.get(item["name"], {}):
                item = item.copy()
                diff.append(item)
            else:
                PluginClass = helper.find_by_id(plugin_id)  # NOSONAR S117
                plugin = PluginClass(
                    None,
                    SecretDict(params),
                    {},
                    None,
                    logger,
                )
                fields = None
                try:
                    fields = plugin.get_fields(item["name"], SecretDict(params))
                except NotImplementedError:
                    logger.error(
                        f"Plugin has not implemented get_fields method with id='{plugin_id}'.",
                        details=traceback.format_exc(),
                        error_code="CE_1060",
                    )
                except Exception:
                    logger.error(
                        f"Error occurred while getting fetching plugin fields with plugin id='{plugin_id}'.",
                        details=traceback.format_exc(),
                        error_code="CE_1061",
                    )
                # On upgrade the stored credentials may still be in their old
                # location, so get_fields can fail or return nothing. Surface
                # the dynamic step for reconfiguration regardless; the UI
                # re-fetches its live fields after the user re-enters auth.
                if not fields:
                    item = item.copy()
                    diff.append(item)
                else:
                    expected_in_step = _get_config_diff(
                        params.get(item["name"], {}),
                        fields,
                        plugin_id,
                        existing_config,
                        field_keys,
                    )
                    if expected_in_step:
                        item = item.copy()
                        diff.append(item)
        if not is_already_added and "has_api_call" in item and item["has_api_call"] and "payload_fields" in item:
            PluginClass = helper.find_by_id(plugin_id)  # NOSONAR S117
            if PluginClass is None:
                continue
            config_details = {}
            for field in item["payload_fields"]:
                config_details[field] = params.get(field, None)
            plugin = PluginClass(
                None,
                SecretDict(config_details),
                None,
                None,
                logger
            )
            try:
                dynamic_fields = plugin.get_dynamic_fields()

                expected_in_step = _get_config_diff(
                    params, dynamic_fields, plugin_id, existing_config, field_keys
                )
                if expected_in_step:
                    diff.append(item)
            except NotImplementedError:
                logger.error(
                    f"Plugin has not implemented get_dynamic_fields method with id='{plugin_id}'.",
                    details=traceback.format_exc(),
                    error_code="CE_1058",
                )
            except Exception:
                logger.error(
                    f"Error occurred while getting dependent fields with plugin id='{plugin_id}'.",
                    details=traceback.format_exc(),
                    error_code="CE_1059",
                )
                raise HTTPException(400, "Error occurred while dependent getting fields. Check logs.")
    return diff


def _get_entities_changed(plugin_id: str, parameters: dict, mapped_entities: list) -> bool:
    """Detect whether an upgrade broke a CRE config's existing entity mapping.

    The upgrade entity-mapping step only exists to reconcile a *stored* mapping
    that would now fail validation. So this only inspects entities the user
    actually mapped (``mapped_entities``) and compares each against the upgraded
    plugin's fresh ``get_entities()``. Returns ``True`` when a now-required field
    is unmapped, a previously-mapped field was removed, or a mapped entity no
    longer exists.
    """
    # Nothing was mapped → nothing to reconcile on upgrade.
    if not mapped_entities:
        return False
    try:
        PluginClass = helper.find_by_id(plugin_id)
        if PluginClass is None:
            return True
        plugin = PluginClass(
            None,
            SecretDict(parameters),
            {},
            None,
            logger,
        )
        entities = plugin.get_entities()
        if not entities:
            return True
        plugin_entities_by_name = {entity.name: entity for entity in entities}
        # Only inspect entities the user actually mapped.
        for mapping in mapped_entities:
            entity = plugin_entities_by_name.get(mapping.get("entity"))
            # A mapped entity that no longer exists in the plugin is a change.
            if entity is None:
                return True
            mapped_sources = {
                field.get("source")
                for field in mapping.get("fields", [])
                if field.get("source")
            }
            plugin_field_names = {field.name for field in entity.fields}
            # Newly-required field that is not part of the stored mapping.
            for field in entity.fields:
                if field.required and field.name not in mapped_sources:
                    return True
            # Previously-mapped field that no longer exists in the plugin.
            for source in mapped_sources:
                if source not in plugin_field_names:
                    return True
        return False
    except Exception:
        logger.error(
            f"Error occurred while diffing entities for plugin id='{plugin_id}'. "
            "Defaulting to surfacing the entity-mapping step.",
            details=traceback.format_exc(),
            error_code="CE_1059",
        )
        return True


async def _disable_configurations(
    plugin,
    metadata,
    diffs,
    user,
    migrated_plugin_id=None,
):
    for configuration in connector.collection(Collections.CONFIGURATIONS).find(
        {"plugin": plugin}
    ):
        diffs.append(
            {
                "name": configuration["name"],
                "plugin": plugin if migrated_plugin_id is None else migrated_plugin_id,
                "category": "CTE",
                "upgrade_notes": metadata.get("upgrade_notes"),
                "existingConfiguration": configuration["parameters"],
                "configuration": _get_config_diff(
                    configuration["parameters"],
                    metadata.get("configuration"),
                    plugin
                ),
                "active": configuration["active"],
                "tenant": configuration["tenant"],
            }
        )
        configuration["active"] = False
        await update_cte_configuration(
            CTEConfigurationUpdate(
                **configuration,
            )
        )
    for configuration in connector.collection(
        Collections.ITSM_CONFIGURATIONS
    ).find({"plugin": plugin}):
        diffs.append(
            {
                "name": configuration["name"],
                "plugin": plugin if migrated_plugin_id is None else migrated_plugin_id,
                "category": "CTO",
                "upgrade_notes": metadata.get("upgrade_notes"),
                "existingConfiguration": configuration["parameters"],
                "configuration": _get_config_diff(
                    configuration["parameters"],
                    metadata.get("configuration"),
                    plugin
                ),
                "active": configuration["active"],
                "tenant": configuration["tenant"],
            }
        )
        configuration["active"] = False
        await update_itsm_configuration(
            ITSMConfigurationUpdate(
                **configuration,
            )
        )
    for configuration in connector.collection(
        Collections.CREV2_CONFIGURATIONS
    ).find({"plugin": plugin}):
        diffs.append(
            {
                "name": configuration["name"],
                "plugin": plugin if migrated_plugin_id is None else migrated_plugin_id,
                "category": "CRE",
                "upgrade_notes": metadata.get("upgrade_notes"),
                "existingConfiguration": configuration["parameters"],
                "configuration": _get_config_diff(
                    configuration["parameters"],
                    metadata.get("configuration"),
                    plugin
                ),
                "mappedEntities": configuration.get("mappedEntities", []),
                "entitiesChanged": _get_entities_changed(
                    plugin,
                    configuration["parameters"],
                    configuration.get("mappedEntities", []),
                ),
                "active": configuration["active"],
                "tenant": configuration["tenant"],
            }
        )
        configuration["active"] = False
        await update_crev2_configuration(
            CREv2ConfigurationUpdate(
                **configuration,
            )
        )
    for configuration in connector.collection(
        Collections.CLS_CONFIGURATIONS
    ).find({"plugin": plugin}):
        diffs.append(
            {
                "name": configuration["name"],
                "plugin": plugin if migrated_plugin_id is None else migrated_plugin_id,
                "attributeMapping": configuration["attributeMapping"],
                "attributeMappingRepo": configuration.get(
                    "attributeMappingRepo"
                ),
                "category": "CLS",
                "upgrade_notes": metadata.get("upgrade_notes"),
                "existingConfiguration": configuration["parameters"],
                "configuration": _get_config_diff(
                    configuration["parameters"],
                    metadata.get("configuration"),
                    plugin
                ),
                "active": configuration["active"],
                "tenant": configuration["tenant"],
            }
        )
        configuration["active"] = False
        await update_cls_configuration(
            CLSConfigurationUpdate(
                **configuration,
            )
        )
    for configuration in connector.collection(
        Collections.EDM_CONFIGURATIONS
    ).find({"plugin": plugin}):
        diffs.append(
            {
                "name": configuration["name"],
                "plugin": plugin,
                "category": "EDM",
                "upgrade_notes": metadata.get("upgrade_notes"),
                "existingConfiguration": configuration["parameters"],
                "configuration": _get_config_diff(
                    configuration["parameters"],
                    metadata.get("configuration"),
                    plugin
                ),
                "active": configuration["active"],
                "tenant": configuration["tenant"],
            }
        )
        await update_edm_configuration(
            EDMConfigurationUpdate(
                name=configuration["name"],
                plugin=configuration["plugin"],
                active=False,
            ),
            user
        )

    for configuration in connector.collection(
        Collections.CFC_CONFIGURATIONS
    ).find({"plugin": plugin}):
        diffs.append(
            {
                "name": configuration["name"],
                "plugin": plugin,
                "category": "CFC",
                "upgrade_notes": metadata.get("upgrade_notes"),
                "existingConfiguration": configuration["parameters"],
                "configuration": _get_config_diff(
                    configuration["parameters"],
                    metadata.get("configuration"),
                    plugin
                ),
                "active": configuration["active"],
                "tenant": configuration["tenant"],
            }
        )
        await update_cfc_configuration(
            CFCConfigurationUpdate(
                name=configuration["name"],
                plugin=configuration["plugin"],
                active=False,
            ),
            user
        )

    for tenant in connector.collection(
        Collections.NETSKOPE_TENANTS
    ).find({"plugin": plugin}):
        diffs.append(
            {
                "name": tenant["name"],
                "plugin": plugin if migrated_plugin_id is None else migrated_plugin_id,
                "category": "Provider",
                "upgrade_notes": metadata.get("upgrade_notes"),
                "existingConfiguration": tenant["parameters"],
                "configuration": _get_config_diff(
                    tenant["parameters"],
                    metadata.get("configuration"),
                    plugin
                ),
            }
        )

    for configuration in connector.collection(
        Collections.LLM_PROVIDER_CONFIGURATIONS
    ).find({"plugin": plugin, "active": True}):
        diffs.append(
            {
                "name": configuration["name"],
                "plugin": plugin,
                "category": "LLMProvider",
                "upgrade_notes": metadata.get("upgrade_notes"),
                "existingConfiguration": configuration["parameters"],
                "configuration": _get_config_diff(
                    configuration["parameters"],
                    metadata.get("configuration"),
                    plugin
                ),
                "active": configuration["active"],
            }
        )
        await update_llm_provider_configuration(
            LLMProviderUpdate(
                plugin=configuration["plugin"],
                active=False,
            ),
            configuration['name'],
            user
        )


async def _delete_configurations(plugin, user):
    for configuration in connector.collection(Collections.CONFIGURATIONS).find(
        {"plugin": plugin}
    ):
        await delete_cte_configuration(
            CTEConfigurationDelete(name=configuration["name"]), user=user
        )
        notifier.acknowledge_config_banners(configuration["name"])
    for configuration in connector.collection(
        Collections.ITSM_CONFIGURATIONS
    ).find({"plugin": plugin}):
        await delete_itsm_configuration(
            ITSMConfigurationDelete(name=configuration["name"]), user=user
        )
        notifier.acknowledge_config_banners(configuration["name"])
    for configuration in connector.collection(
        Collections.CREV2_CONFIGURATIONS
    ).find({"plugin": plugin}):
        await delete_crev2_configuration(name=configuration["name"], user=user)
        notifier.acknowledge_config_banners(configuration["name"])
    for configuration in connector.collection(
        Collections.CLS_CONFIGURATIONS
    ).find({"plugin": plugin}):
        await delete_cls_configuration(
            CLSConfigurationDelete(name=configuration["name"]), user=user
        )
        notifier.acknowledge_config_banners(configuration["name"])
    for configuration in connector.collection(
        Collections.NETSKOPE_TENANTS
    ).find({"plugin": plugin}):
        await delete_tenant_configuration(
            configuration['name'], user
        )
        notifier.acknowledge_config_banners(configuration["name"])
    for configuration in connector.collection(
        Collections.EDM_CONFIGURATIONS
    ).find({"plugin": plugin}):
        await delete_edm_configuration(
            EDMConfigurationDelete(name=configuration["name"]), user=user
        )
        notifier.acknowledge_config_banners(configuration["name"])
    for configuration in connector.collection(
        Collections.CFC_CONFIGURATIONS
    ).find({"plugin": plugin}):
        await delete_cfc_configuration(
            CFCConfigurationDelete(name=configuration["name"]), user=user
        )
        notifier.acknowledge_config_banners(configuration["name"])
    for configuration in connector.collection(
        Collections.LLM_PROVIDER_CONFIGURATIONS
    ).find({"plugin": plugin}):
        await delete_llm_provider_configuration(
            name=configuration["name"], user=user
        )
        notifier.acknowledge_config_banners(configuration["name"])


def check_version_dependency(repo_name, manifest, plugin):
    """Check version dependency for a plugin based on the minimum version requirements.

    Parameters:
        repo_name (str): The name of the repository.
        manifest (dict): The manifest of the plugin containing version information.
        plugin (str): The id of the plugin

    Raises:
        HTTPException:
            If the core version or provider version is not compatible for the plugin update.
            If the fields: "name" and "version" is missing in the plugin manifest file
    """
    if "minimum_version" in manifest:
        if manifest["minimum_version"] > str(api.__version__):
            raise HTTPException(
                400,
                f"Plugin {manifest['name']} can't be updated as the core version is not compatible.",
            )

    if "minimum_provider_version" in manifest:
        provider_path = os.path.join(
            PLUGIN_PATH if repo_name == "custom_plugins" else REPO_STORAGE_PATH,
            repo_name,
            manifest["provider_id"],
            "manifest.json",
        )
        provider_manifest = None
        if os.path.exists(provider_path):
            provider_manifest = json.load(open(provider_path))
        if not provider_manifest:
            raise HTTPException(
                400,
                f"Plugin {manifest['name']} can't be updated as the provider plugin "
                f"does not exist. Please add the provider plugin first.",
            )
        if manifest["minimum_provider_version"] > provider_manifest["version"]:
            raise HTTPException(
                400,
                f"Plugin {manifest['name']} can't be updated as the provider version is not compatible.",
            )

    require_fields = ["name", "version"]
    for field in require_fields:
        if field not in manifest:
            raise HTTPException(
                400,
                f"Plugin {plugin} can't be updated as the {field} is missing in the manifest.",
            )


@router.get("/repos/{name}/migrate_plugin", tags=["Plugin Repositories"])
async def migrate_plugins(
    name: str = Path(...),
    plugins: List[str] = Query(...),
    user: User = Security(get_current_user, scopes=["settings_read"]),
):
    """Migrate specified plugins."""
    source_repo = next(filter(lambda i: i.name == name, manager.repos))
    default_repo = next(filter(lambda i: i.name == "Default", manager.repos))
    if not source_repo or not default_repo:
        return {"success": False}

    diffs = []
    for plugin in plugins:
        try:
            start_index = len(diffs)
            default_plugin_id = plugin.split(".")
            default_plugin_id[2] = default_repo.name
            default_plugin_id = ".".join(default_plugin_id)
            package = plugin.split(".")[-2]
            default_manifest = None
            default_repo_path = os.path.join(manager.get_dir(default_repo), package)
            default_manifest_path = os.path.join(default_repo_path, "manifest.json")
            if os.path.exists(default_manifest_path):
                default_manifest = json.load(open(default_manifest_path))
                check_version_dependency(name, default_manifest, plugin)

            manager.update_plugin(default_repo, default_plugin_id)
            manager._remove_plugin(source_repo, package)
            PluginClass = helper.find_by_id(default_plugin_id)  # NOSONAR
            if PluginClass is None:
                continue
            if not default_manifest:
                continue
            await _disable_configurations(
                plugin,
                default_manifest,
                diffs,
                user,
                migrated_plugin_id=default_plugin_id,
            )

            integration_map = {
                "cls": Collections.CLS_CONFIGURATIONS,
                "cte": Collections.CONFIGURATIONS,
                "itsm": Collections.ITSM_CONFIGURATIONS,
                "crev2": Collections.CREV2_CONFIGURATIONS,
            }
            integration = helper.find_integration_by_id(plugin)
            collection = integration_map.get(integration)
            if collection is None:
                continue
            response = connector.collection(collection).update_many(
                {"plugin": plugin}, {"$set": {"plugin": default_plugin_id}}
            )
            if (
                response.modified_count > 0
                and collection == Collections.CLS_CONFIGURATIONS
            ):
                plugin_path = os.path.join(manager.get_plugin_dir(default_repo), package)
                mapping_path = os.path.join(plugin_path, "mappings.json")
                default_mapping_file_name = None
                if os.path.exists(mapping_path):
                    file = open(mapping_path)
                    mapping_file = json.load(file)
                    if isinstance(mapping_file, dict):
                        mapping_file = [mapping_file]
                    default_mapping_file_name = mapping_file[0]["name"]

                plugin_path = os.path.join(manager.get_plugin_dir(source_repo), package)
                mapping_path = os.path.join(plugin_path, "mappings.json")
                source_mapping_file_name = None
                if os.path.exists(mapping_path):
                    file = open(mapping_path)
                    mapping_file = json.load(file)
                    if isinstance(mapping_file, dict):
                        mapping_file = [mapping_file]
                    source_mapping_file_name = mapping_file[0]["name"]

                if default_mapping_file_name and source_mapping_file_name and diffs:
                    for data in diffs[start_index:]:
                        if (
                            data["attributeMappingRepo"] == source_repo.name and
                            data["attributeMapping"] == source_mapping_file_name
                        ):
                            data["attributeMapping"] = default_mapping_file_name
                            data["attributeMappingRepo"] = default_repo.name
                            connector.collection(collection).update_one(
                                {
                                    "name": data["name"]
                                },
                                {
                                    "$set": {
                                        "attributeMapping": default_mapping_file_name,
                                        "attributeMappingRepo": default_repo.name,
                                    }
                                },
                            )

            connector.collection(Collections.PLUGIN_REPOS).update_many(
                {"name": source_repo.name},
                {"$pull": {"plugin_migrates": {"id": plugin}}},
            )

        except Exception:
            logger.error(
                "Error occurred while migrating plugin.",
                details=traceback.format_exc(),
                error_code="CE_1014",
            )

    connector.collection(Collections.PLUGIN_REPOS).update_one(
        {"name": source_repo.name},
        {
            "$set": {
                "updates": {
                    "action": "update_plugins",
                    "metadata": {"plugins": plugins},
                },
                "hasUpdate": True if manager.get_diff(source_repo) else False,
            }
        },
    )
    update_plugins_updated_at()
    return {"success": True, "changes": diffs}


@router.get("/repos/{name}/update", tags=["Plugin Repositories"])
async def update_plugins(
    name: str = Path(...),
    plugins: List[str] = Query(...),
    user: User = Security(get_current_user, scopes=["settings_read"]),
):
    """Update specified plugins."""
    repo = list(filter(lambda i: i.name == name, manager.repos))
    if not repo:
        return {"success": False}
    repo = repo.pop()
    diffs = []
    for plugin in plugins:
        package = plugin.split(".")[-2]
        manifest_path = os.path.join(
            os.path.join(REPO_STORAGE_PATH, repo.name),
            package,
            "manifest.json",
        )
        manifest = None
        if os.path.exists(manifest_path):
            manifest = json.load(open(manifest_path))
        if manifest:
            check_version_dependency(name, manifest, plugin)

        # Resolve the disposition before touching the disk. update_plugin() deletes
        # the package for a removal, after which find_by_id() can no longer resolve
        # the plugin class and every delete handler skips its cleanup(), stranding
        # whatever the plugin owns (banners included).
        status = manager.get_plugin_status(repo, plugin)
        if status == PluginStatus.REMOVED:
            await _delete_configurations(plugin, user)
            manager.update_plugin(repo, plugin)
            continue

        PluginClass = helper.find_by_id(plugin)  # NOSONAR
        manager.update_plugin(repo, plugin)
        if PluginClass is None:
            continue
        if status == PluginStatus.MODIFIED:
            await _disable_configurations(plugin, manifest, diffs, user)
    connector.collection(Collections.PLUGIN_REPOS).update_one(
        {"name": repo.name},
        {
            "$set": {
                "updates": {
                    "action": "update_plugins",
                    "metadata": {"plugins": plugins},
                },
                "hasUpdate": True if manager.get_diff(repo) else False,
            }
        },
    )
    update_plugins_updated_at()
    return {"success": True, "changes": diffs}


@router.get("/repos/{name}/fetch", tags=["Plugin Repositories"])
async def fetch_repo_contents(
    name: str = Path(...),
    user: User = Security(get_current_user, scopes=["settings_read"]),
):
    """Fetch new repo updates."""
    repo = list(filter(lambda i: i.name == name, manager.repos))
    if not repo:
        return {"success": False}
    repo = repo.pop()
    success = manager.pull_updates(repo)
    if not success:
        raise HTTPException(400, "Error occurred while fetching updates.")

    plugin_info = manager.get_diff(repo)
    error_msg = [error.get("error_msg") for error in plugin_info if "error_msg" in error]

    if error_msg:
        raise HTTPException(400, error_msg[0])

    connector.collection(Collections.PLUGIN_REPOS).update_one(
        {"name": repo.name},
        {
            "$set": {
                "updates": {"action": "pull_updates"},
                "hasUpdate": True if plugin_info else False,
            }
        },
    )
    return {"success": True}


def _read_archived_file(archive, file: str) -> str:
    """Get contents of a file from an archive."""
    if isinstance(archive, zip.ZipFile):
        openfile = archive.open
    elif isinstance(archive, tar.TarFile):
        openfile = archive.extractfile
    else:
        raise TypeError("Unsupported file format.")
    return openfile(file).read()


def _get_root_files(archive) -> List[str]:
    """Get all files at the root level in the archive."""
    if isinstance(archive, zip.ZipFile):
        names = archive.namelist()
    elif isinstance(archive, tar.TarFile):
        names = archive.getnames()
    else:
        raise TypeError("Unsupported file format.")
    root = list(
        set([name.split("/")[0].strip("/") for name in names])
        - set(["__MACOSX"])
    )
    return root


def _exists_in_archive(archive, path) -> bool:
    """Check if the given file exists in the package."""
    if isinstance(archive, zip.ZipFile):
        return path in archive.namelist()
    elif isinstance(archive, tar.TarFile):
        return path in archive.getnames()
    else:
        raise TypeError("Unsupported file format.")


@router.post(
    "/repos/{name}/upload",
    tags=["Plugin Repositories"],
    status_code=201,
    description="Upload a new plugin.",
)
async def upload_plugin(
    name: str = Path(...),
    file: UploadFile = File(...),
    user: User = Security(get_current_user, scopes=["settings_write"]),
):
    """Apply an update.

    Args:
        file (UploadFile, optional): Plugin file to be uploaded.
        Defaults to File(...).

    Raises:
        HTTPException: In case of validation failures.

    Returns:
        Plugin: The uploaded plugin.
    """
    try:
        # Check if repo exists
        repo = list(filter(lambda i: i.name == name, manager.repos))
        if not repo and name != "custom_plugins":
            return {"success": False}
        if name == "custom_plugins":
            repo = [PluginRepo(name=name, url="", username="", password="")]
        repo = repo.pop()
        # create a temp file to store the uploaded file
        (_, temp_name) = tempfile.mkstemp("cte")
        with open(temp_name, "wb") as fh:
            fh.write(await file.read())
        try:
            plugin_file = zip.ZipFile(temp_name)
        except zip.BadZipFile:
            try:
                plugin_file = tar.open(temp_name)
            except tar.ReadError:
                raise HTTPException(
                    400, "Uploaded file is not a valid .zip or .tar.gz file."
                )
        # validate that only one plugin dir is in the archive
        root_dirs = _get_root_files(plugin_file)
        if len(root_dirs) != 1:
            raise HTTPException(
                400,
                (
                    "More than one root directoris found in the package. "
                    "Packages with only one plugin are allowed."
                ),
            )
        root_dir = root_dirs.pop()
        plugin_id = root_dir.strip("/")

        # validate that required files are there
        for file in ["main.py", "manifest.json", "icon.png"]:
            if not _exists_in_archive(plugin_file, join(root_dir, file)):
                raise HTTPException(
                    400, f"Missing {file} in the plugin package."
                )
        # validate the manifest.json
        manifest = _read_archived_file(
            plugin_file, join(root_dir, "manifest.json")
        )
        manifest = json.loads(manifest)
        validate(instance=manifest, schema=MANIFEST_SCHEMA)

        check_version_dependency(name, manifest, plugin_id)

        diffs = []
        # apply the plugin update
        files_to_extract = None
        if isinstance(plugin_file, zip.ZipFile):
            files_to_extract = [
                name
                for name in plugin_file.namelist()
                if "__MACOSX" not in name
            ]
        plugin_file.extractall(
            join(dirname(dir_plugins.__file__), repo.name), files_to_extract
        )
        plugin_file.close()
        mapping_file_path = join(
            dirname(dir_plugins.__file__),
            repo.name,
            root_dir,
            "mappings.json",
        )
        try:
            if os.path.exists(mapping_file_path):
                manager.import_mapping_file(mapping_file_path, repo.name)
        except Exception:
            logger.error(
                "Error occurred while importing mapping file.",
                details=traceback.format_exc(),
                error_code="CE_1015",
            )
        manager.helper.refresh()
        await _disable_configurations(
            f"netskope.plugins.{repo.name}.{root_dir}.main", manifest, diffs, user
        )
        update_plugins_updated_at()
        return {"success": True, "changes": diffs}
    except ValidationError as ex:  # had a jsonschema validation error
        raise HTTPException(
            400, f"Could not validate manifest.json. Cause: {ex.message}."
        )
    except HTTPException as ex:  # pass along the HTTPException
        logger.error(
            "Could not load the uploaded plugin.",
            details=traceback.format_exc(),
            error_code="CE_1002",
            resolution="""\nEnsure that,\n        1. The uploaded file is a valid .zip or .tar.gz file.\n        2. The uploaded file should contains manifest.json, main.py and icon.png files.\n"""  # noqa
        )
        raise ex
    except json.JSONDecodeError:  # invalid json
        raise HTTPException(
            400, "Invalid manifest.json in the plugin package."
        )
    except Exception:  # some other exception
        logger.error(
            "Could not load the uploaded plugin.",
            details=traceback.format_exc(),
            error_code="CE_1027",
        )
        raise HTTPException(500, "Could not load the uploaded plugin.")


@router.post("/repos/{name}/branch/{branch}", tags=["Plugin Repositories"])
async def switch_repo_branch(
    name: str = Path(...),
    branch: str = Path(...),
    user: User = Security(get_current_user, scopes=["admin"]),
):
    """Fetch new repo updates."""
    repo = list(filter(lambda i: i.name == name, manager.repos))
    if not repo:
        return {"success": False}
    repo = repo.pop()
    success = manager.switch_branch(repo, branch)
    if not success:
        return {"success": False}
    helper.refresh()
    return {"success": success}
