"""Plugin related endpoints."""
import json
import os
import re
import shutil
import tarfile
import tempfile
import traceback
from tarfile import TarFile
from os.path import dirname, join
from typing import List
from zipfile import BadZipFile, ZipFile

from fastapi import APIRouter, File, HTTPException, Security, UploadFile
from jsonschema import ValidationError, validate

from .repos import _get_root_files, _exists_in_archive, update_plugins_updated_at
import netskope.plugins.custom_plugins as dir_custom_plugins
from netskope.integrations.cls.models import (
    ConfigurationDelete as CLSConfigurationDelete,
)
from netskope.integrations.cls.routers.configurations import (
    delete_configuration as delete_cls_configuration,
)
from netskope.integrations.crev2.routers.configurations import (
    delete_configuration as delete_crev2_configuration,
)
from netskope.integrations.cte.models import (
    ConfigurationDelete as CTEConfigurationDelete,
)
from netskope.integrations.cte.routers.configurations import (
    delete_configuration as delete_cte_configuration,
)
from netskope.integrations.itsm.models import (
    ConfigurationDelete as ITSMConfigurationDelete,
)
from netskope.integrations.itsm.routers.configurations import (
    delete_configuration as delete_itsm_configuration,
)

from netskope.common.api.routers.tenants import delete_tenant

from ...models import ErrorMessage, User
from ...utils import Collections, DBConnector, Logger, PluginHelper, RepoManager
from .auth import get_current_user

router = APIRouter()
helper = PluginHelper()
logger = Logger()
db_connector = DBConnector()
manager = RepoManager()

MANIFEST_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "name": {"type": "string", "minLength": 1},
        "id": {"type": "string", "minLength": 1},
        "version": {"type": "string", "minLength": 1},
        "description": {"type": "string", "minLength": 1},
        "patch_supported": {"type": "boolean"},
        "push_supported": {"type": "boolean"},
        "configuration": {
            "type": "array",
            "items": {
                "oneOf": [
                    {
                        "type": "object",
                        "properties": {
                            "label": {"type": "string"},
                            "key": {"type": "string"},
                            "mandatory": {"type": "boolean"},
                            "default": {
                                "anyOf": [
                                    {"type": "string"},
                                    {"type": "number"},
                                ]
                            },
                            "description": {"type": "string"},
                            "type": {
                                "type": "string",
                                "enum": ["text", "password", "number"],
                            },
                        },
                        "required": [
                            "label",
                            "key",
                            "type",
                            "default",
                            "description",
                            "mandatory",
                        ],
                    },
                    {
                        "type": "object",
                        "properties": {
                            "label": {"type": "string"},
                            "key": {"type": "string"},
                            "mandatory": {"type": "boolean"},
                            "default": {
                                "anyOf": [
                                    {"type": "string"},
                                    {"type": "number"},
                                ]
                            },
                            "description": {"type": "string"},
                            "type": {"type": "string", "enum": ["choice"]},
                            "choices": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "key": {"type": "string"},
                                        "value": {"type": "string"},
                                    },
                                    "required": ["key", "value"],
                                },
                            },
                        },
                        "required": [
                            "label",
                            "key",
                            "type",
                            "choices",
                            "default",
                            "description",
                            "mandatory",
                        ],
                    },
                    {
                        "type": "object",
                        "properties": {
                            "label": {"type": "string"},
                            "key": {"type": "string"},
                            "mandatory": {"type": "boolean"},
                            "default": {
                                "type": "array",
                                "items": {
                                    "anyOf": [
                                        {"type": "string"},
                                        {"type": "number"},
                                    ]
                                },
                            },
                            "description": {"type": "string"},
                            "type": {
                                "type": "string",
                                "enum": ["multichoice"],
                            },
                            "choices": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "key": {"type": "string"},
                                        "value": {"type": "string"},
                                    },
                                    "required": ["key", "value"],
                                },
                            },
                        },
                        "required": [
                            "label",
                            "key",
                            "type",
                            "choices",
                            "default",
                            "description",
                            "mandatory",
                        ],
                    },
                ]
            },
        },
    },
    "required": [
        "name",
        "id",
        "version",
        "description",
        "configuration",
        "patch_supported",
    ],
}

SCOPES_NEEDED_PROVIDER = {"cte_read", "cto_read", "cls_read", "cre_read"}


@router.get("/plugins", tags=["Plugins"])
async def list_plugins(
    user: User = Security(get_current_user, scopes=[])
) -> List[dict]:
    """List all the plugins."""
    manager.load(cache=True)
    out = []
    netskope_plugins = []

    if SCOPES_NEEDED_PROVIDER & set(user.scopes):
        for plugin_cls in helper.plugins["provider"]:
            metadata = plugin_cls.metadata
            plugin = {
                "name": metadata["name"],
                "netskope": metadata.get("netskope", False),
                "id": plugin_cls.__module__,
                "version": metadata["version"],
                "configuration": metadata["configuration"],
                "description": metadata["description"],
                "icon": metadata["icon"],
                "category": "Provider",
                "repo": metadata.get("repo_name"),
                "provider_id": metadata.get("provider_id"),
            }
            if metadata.get("netskope", False):
                netskope_plugins.append(plugin)
            else:
                out.append(plugin)

    if "cte_read" in user.scopes:
        for plugin_cls in helper.plugins["cte"]:
            metadata = plugin_cls.metadata
            plugin = {
                "name": metadata["name"],
                "netskope": metadata.get("netskope", False),
                "id": plugin_cls.__module__,
                "version": metadata["version"],
                "pushSupported": metadata.get("push_supported", True),
                "configuration": metadata["configuration"],
                "description": metadata["description"],
                "icon": metadata["icon"],
                "category": "CTE",
                "repo": metadata.get("repo_name"),
                "provider_id": metadata.get("provider_id"),
            }
            if metadata.get("netskope", False):
                netskope_plugins.append(plugin)
            else:
                out.append(plugin)

    if "cto_read" in user.scopes:
        for plugin_cls in helper.plugins["itsm"]:
            metadata = plugin_cls.metadata
            plugin = {
                "name": metadata["name"],
                "netskope": metadata.get("netskope", False),
                "id": plugin_cls.__module__,
                "version": metadata["version"],
                "description": metadata["description"],
                "configuration": metadata["configuration"],
                "icon": metadata["icon"],
                "receivingSupported": metadata.get("receiving_supported", True),
                "sharingSupported": metadata.get("sharing_supported", False),
                "category": "CTO",
                "repo": metadata.get("repo_name"),
                "provider_id": metadata.get("provider_id"),
            }
            if metadata.get("netskope", False) or plugin["id"].split(".")[-2] == "logs_itsm":
                netskope_plugins.append(plugin)
            else:
                out.append(plugin)

    if "cre_read" in user.scopes:
        for plugin_crev2 in helper.plugins["cre"]:
            metadata = plugin_crev2.metadata
            plugin = {
                "name": metadata["name"],
                "netskope": metadata.get("netskope", False),
                "id": plugin_crev2.__module__,
                "version": metadata["version"],
                "repo": metadata.get("repo_name"),
                "description": metadata["description"],
                "configuration": metadata["configuration"],
                "icon": metadata["icon"],
                "category": "CRE",
                "types": metadata.get("types", []),
                "provider_id": metadata.get("provider_id"),
            }
            if metadata.get("netskope", False):
                netskope_plugins.append(plugin)
            else:
                out.append(plugin)

    if "cls_read" in user.scopes:
        for plugin_cls in helper.plugins["cls"]:
            metadata = plugin_cls.metadata
            plugin = {
                "name": metadata["name"],
                "netskope": metadata.get("netskope", False),
                "id": plugin_cls.__module__,
                "version": metadata["version"],
                "mapping": metadata.get("mapping"),
                "repo": metadata.get("repo_name"),
                "description": metadata["description"],
                "configuration": metadata["configuration"],
                "icon": metadata["icon"],
                "category": "CLS",
                "types": metadata.get("types", []),
                "provider_id": metadata.get("provider_id"),
                "push_supported": metadata.get("push_supported", True),
                "pull_supported": metadata.get("pull_supported", False),
                "format_options": metadata.get("format_options", None),
            }
            if metadata.get("netskope", False):
                netskope_plugins.append(plugin)
            else:
                out.append(plugin)

    if "edm_read" in user.scopes:
        for plugin_edm in helper.plugins["edm"]:
            metadata = plugin_edm.metadata
            plugin = {
                "name": metadata["name"],
                "netskope": metadata.get("netskope", False),
                "id": plugin_edm.__module__,
                "version": metadata["version"],
                "pushSupported": metadata.get("push_supported", False),
                "pullSupported": metadata.get("pull_supported", False),
                "configuration": metadata["configuration"],
                "description": metadata["description"],
                "icon": metadata["icon"],
                "category": "EDM",
                "repo": metadata.get("repo_name"),
                "provider_id": metadata.get("provider_id"),
            }
            if metadata.get("netskope", False):
                netskope_plugins.append(plugin)
            else:
                out.append(plugin)

    if "cfc_read" in user.scopes:
        for plugin_cfc in helper.plugins["cfc"]:
            metadata = plugin_cfc.metadata
            plugin = {
                "name": metadata["name"],
                "netskope": metadata.get("netskope", False),
                "id": plugin_cfc.__module__,
                "version": metadata["version"],
                "pushSupported": metadata.get("push_supported", False),
                "pullSupported": metadata.get("pull_supported", False),
                "configuration": metadata["configuration"],
                "description": metadata["description"],
                "icon": metadata["icon"],
                "category": "CFC",
                "repo": metadata.get("repo_name"),
                "provider_id": metadata.get("provider_id"),
            }
            if metadata.get("netskope", False):
                netskope_plugins.append(plugin)
            else:
                out.append(plugin)

    out = sorted(out, key=lambda x: x.get("name").lower())
    return netskope_plugins + out


def validate_plugin(plugin):
    """Validate the given plugin archive.

    Args:
        plugin (object): ZipFile or TarFile plugin archive.

    Returns:
        object: True if all validation passes; str message otherwise.
    """
    # extract info depending on filetype
    if isinstance(plugin, ZipFile):
        openfile = plugin.open
    elif isinstance(plugin, TarFile):
        openfile = plugin.extractfile
    else:
        return "Unsupported file format."
    root = _get_root_files(plugin)
    # has 0 or more files at root level
    if len(root) != 1:
        return "No or more than one root directories."
    root = root.pop()
    if re.match(r"^[_a-zA-Z]\w*$", root[:-1]) is None:
        return f"'{root[:-1]}' is not a valid package name."
    # make sure manifest.json exists
    if not _exists_in_archive(plugin, join(root, "__init__.py")):
        return "No __init__.py file found."
    if not _exists_in_archive(plugin, join(root, "manifest.json")):
        return "No manifest.json file found."
    if not _exists_in_archive(plugin, join(root, "main.py")):
        return "No main.py file found."
    if not _exists_in_archive(plugin, join(root, "icon.png")):
        return "No icon.png file found."
    manifest = openfile(join(root, "manifest.json"))
    try:
        manifest_content = manifest.read().decode("utf-8")
        manifest_parsed = json.loads(manifest_content)
        # validate manifest.json schema
        validate(instance=manifest_parsed, schema=MANIFEST_SCHEMA)
        plugin_id = manifest_parsed["id"]
        if plugin_id != root.strip("/"):  # make sure directory name is unique
            return "Root directory name must be same as id in the manifest.json."
        # make sure id is unique
        if (
            helper.find_by_id(".".join([dir_custom_plugins.__package__, plugin_id, "main"]))
            is not None
        ):
            return f"Plugin with id='{plugin_id}' already exists."
    except ValidationError as ex:  # had a jsonschema validation error
        return f"Could not validate manifest.json. Cause: {ex.message}."
    except Exception:  # could not parse the manifest.json file
        return "Could not parse the manifest.json file."
    return True


@router.post(
    "/plugins",
    tags=["Plugins"],
    status_code=201,
    responses={"422": {"model": ErrorMessage}},
    description="Upload a new plugin.",
)
async def upload_plugin(
    file: UploadFile = File(...),
    user: User = Security(get_current_user, scopes=["settings_write"]),
):
    """Upload a plugin to CTE.

    Args:
        file (UploadFile, optional): Plugin file to be uploaded.
        Defaults to File(...).

    Raises:
        HTTPException: In case of validation failures.

    Returns:
        Plugin: The uploaded plugin.
    """
    try:
        existing_plugins = set(
            [plugin_cls.__module__ for plugin_cls in helper.plugins["cte"]]
        )
        (fd, name) = tempfile.mkstemp("cte")
        with open(name, "wb") as fh:
            fh.write(await file.read())
        try:
            plugin_file = ZipFile(name)
        except BadZipFile:
            try:
                plugin_file = tarfile.open(name)
            except tarfile.ReadError:
                raise HTTPException(
                    400, "Uploaded file is not a valid .zip or .tar.gz file."
                )
        validation_result = validate_plugin(plugin_file)
        if validation_result is not True:
            plugin_file.close()
            raise HTTPException(400, f"Invalid plugin. {validation_result}")
        plugin_file.extractall(dirname(dir_custom_plugins.__file__))
        plugin_file.close()
        helper.refresh()
        new_plugin = list(
            set(
                [plugin_cls.__module__ for plugin_cls in helper.plugins["cte"]]
            ).difference(existing_plugins)
        ).pop()
        new_plugin = helper.find_by_id(new_plugin)
        metadata = new_plugin.metadata
        logger.debug(f"New plugin uploaded with id '{metadata['id']}'.")
        return {
            "name": metadata["name"],
            "id": new_plugin.__module__,
            "version": metadata["version"],
            "description": metadata["description"],
            "configuration": metadata["configuration"],
            "icon": metadata["icon"],
            "receivingSupported": metadata.get("receiving_supported", True),
            "category": "CTE",
        }
    except HTTPException as ex:  # pass along the HTTPException
        logger.error(
            "Could not load the uploaded plugin.",
            details=traceback.format_exc(),
            error_code="CE_1002",
            resolution="""\nEnsure that,\n        1. The uploaded file is a valid .zip or .tar.gz file.\n        2. The uploaded file should contains manifest.json, main.py and icon.png files.\n"""  # noqa
        )
        raise ex
    except Exception:  # some other exception
        logger.error(
            "Could not load the uploaded plugin.",
            details=traceback.format_exc(),
            error_code="CE_1027",
        )
        raise HTTPException(500, "Could not load the uploaded plugin.")


@router.delete(
    "/plugins",
    tags=["Plugins"],
    responses={"422": {"model": ErrorMessage}},
    description="Delete a Plugin.",
)
async def delete_plugin(
    plugin_id: str,
    category: str,
    user: User = Security(get_current_user, scopes=["settings_write"]),
):
    """Delete a plugin from Netskope Cloud Exchange.

    Args:
        plugin_id: id of the plugin to be deleted.

    Raises:
        HTTPException: If plugin repo isn't deleted.

    Returns:
        Plugin: Message of successfull deletion of plugin.
    """
    custom_plugin_path = dir_custom_plugins.__path__[0]
    dir_name = plugin_id.split(".")
    plugin_path = os.path.join(custom_plugin_path, dir_name[3])
    is_plugin_deleted = False
    if helper.find_by_id(plugin_id):
        if category.lower() == "cte":
            plugins = db_connector.collection(Collections.CONFIGURATIONS).find(
                {"plugin": plugin_id}
            )
            for plugin in plugins:
                await delete_cte_configuration(
                    CTEConfigurationDelete(name=plugin["name"]), user
                )
            shutil.rmtree(plugin_path)
            logger.debug(f"Plugin {dir_name[3]} Deleted Successfully.")
            is_plugin_deleted = True
        elif category.lower() == "cre":
            plugins = db_connector.collection(
                Collections.CREV2_CONFIGURATIONS
            ).find({"plugin": plugin_id})
            for plugin in plugins:
                await delete_crev2_configuration(
                    name=plugin["name"], user=user
                )
            shutil.rmtree(plugin_path)
            logger.debug(f"Plugin {dir_name[3]} Deleted Successfully.")
            is_plugin_deleted = True
        elif category.lower() == "cls":
            plugins = db_connector.collection(
                Collections.CLS_CONFIGURATIONS
            ).find({"plugin": plugin_id})
            for plugin in plugins:
                await delete_cls_configuration(
                    CLSConfigurationDelete(name=plugin["name"]), user
                )
            shutil.rmtree(plugin_path)
            logger.debug(f"Plugin {dir_name[3]} Deleted Successfully.")
            is_plugin_deleted = True
        elif category.lower() in ["itsm", "cto"]:
            plugins = db_connector.collection(Collections.ITSM_CONFIGURATIONS).find(
                {"plugin": plugin_id}
            )
            for plugin in plugins:
                await delete_itsm_configuration(
                    ITSMConfigurationDelete(name=plugin["name"]), user
                )
            shutil.rmtree(plugin_path)
            logger.debug(f"Plugin {dir_name[3]} Deleted Successfully.")
            is_plugin_deleted = True
        elif category.lower() == "provider":
            tenants = db_connector.collection(Collections.NETSKOPE_TENANTS).find(
                {"plugin": plugin_id}
            )
            for tenant in tenants:
                await delete_tenant(tenant["name"], user)
            shutil.rmtree(plugin_path)
            logger.debug(f"Provider {dir_name[3]} Deleted Successfully.")
            is_plugin_deleted = True
        if is_plugin_deleted:
            helper.refresh()
            update_plugins_updated_at()
        return {"Success": True}
    else:
        raise HTTPException(400, "Plugin not Found.")
