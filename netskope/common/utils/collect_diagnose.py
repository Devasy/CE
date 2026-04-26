"""Collect diagnose data."""

from datetime import datetime
from bson import json_util
import asyncio
import re
import sys
import itertools
from netskope.common.utils import (
    Logger,
    PluginHelper,
    DBConnector,
    SecretDict,
    Collections,
)
from netskope.common.api.routers.dashboard import get_status
from netskope.common.api.routers.status import cluster_status
from netskope.integrations.cte.routers.dashboard import (
    pull_statistics,
    sharing_statistics,
)

plugin_helper = PluginHelper()
db_connector = DBConnector()
logger = Logger()


def collect_plugin_parameters(plugin, collection=None):
    """Collect plugin parameters."""
    if not plugin:
        return "Plugin not found or unavailable."

    exclude_key_regex = (
        r"^(id_|api_|server_|key_|auth_|host|api|ip_|"
        r".*(_id|_arn|_key|_assigne|_url|_email|_username|_host|_address|_server|_file|_uri|_auth|_ip)$|"
        r".*(hostname|address|servername|uri|server|tenantName|arn|email|id|file|key|auth|username|url|host).*)"
    )
    PluginClass = plugin_helper.find_by_id(plugin.get("plugin"))
    if PluginClass is None:
        return "Plugin not found or unavailable."

    metadata = PluginClass.metadata
    plugin_name = metadata.get("name")
    plugin["pluginName"] = plugin_name
    filtered_configuration = [
        item
        for item in metadata["configuration"]
        if (
            item.get("type") == "password"
            or item.get("type") == "textarea"
            or (
                re.match(exclude_key_regex, item.get("key", ""), re.IGNORECASE)
                and item.get("key") != "authentication_method22"
            )
        )
    ]
    password_keys = [entry["key"] for entry in filtered_configuration]
    for key in password_keys:
        if "parameters" in plugin and key in plugin["parameters"]:
            plugin["parameters"].pop(key)

    filtered_dynamic_steps = [
        item for item in metadata["configuration"] if item.get("type") == "dynamic_step"
    ]
    plug_obj = PluginClass(
        plugin.get("name"),
        SecretDict(plugin.get("parameters")),
        {},
        None,
        logger,
        use_proxy=plugin.get("useProxy"),
    )
    for dynamic_step in filtered_dynamic_steps:
        if dynamic_step and plugin.get("pluginType", "") != "receiver":
            main = plug_obj.get_fields(
                dynamic_step.get("name"), plugin.get("parameters")
            )
            find_sensitive_data(dynamic_step, main, metadata, plugin, exclude_key_regex)
    filtered_steps = [
        item for item in metadata["configuration"] if item.get("type") == "step"
    ]
    for step in filtered_steps:
        if step:
            find_sensitive_data(step, None, metadata, plugin, exclude_key_regex)
    # Check for has_api_call
    has_api_call = any(
        item.get("has_api_call") is True for item in metadata["configuration"]
    )
    if has_api_call and plugin.get("pluginType", "") != "receiver":
        api_call = plug_obj.get_dynamic_fields()
        find_sensitive_data(api_call, api_call, metadata, plugin, exclude_key_regex)
    plugin.pop("_id"),
    plugin.pop("storage"),
    process_date_fields(plugin)
    return plugin


def diagnose(collect):
    """Diagnose."""
    data = []
    try:
        plugins = db_connector.collection(collect).find()
        for plugin in plugins:
            processed_plugin = collect_plugin_parameters(plugin, collect)
            data.append(processed_plugin)
        resp = json_util.dumps(data, indent=2)
        return resp
    except Exception as e:
        logger.error(f"Diagnose : Error occurred while getting the plugins. {e}")
        return "Diagnose : Error occurred while getting the plugins."


def process_date_fields(plugin):
    """Recursively process date fields in dictionaries, skipping parameters field."""
    if isinstance(plugin, dict):
        for key, value in list(plugin.items()):
            if key == "parameters":
                continue
            if isinstance(value, dict):
                process_date_fields(value)
            elif value and isinstance(value, (str, int, float, datetime)):
                try:
                    plugin[key] = date_format(value)
                except (ValueError, TypeError):
                    pass
    return plugin


def find_sensitive_data(step, main, metadata, plugin, exclude_key_regex):
    """Find sensitive data."""
    if step:
        fields = get_all_fields(metadata)
        choice_fields = [field for field, _ in get_choice_fields(metadata)]
        if main is None:
            all_fields_iter = itertools.chain(fields, choice_fields)
        else:
            all_fields_iter = itertools.chain(fields, main, choice_fields)
        filtered_type = [
            item
            for item in all_fields_iter
            if item.get("type") == "password"
            or item.get("type") == "textarea"
            or re.match(exclude_key_regex, item.get("key", ""), re.IGNORECASE)
        ]
        keys = [entry["key"] for entry in filtered_type]
        remove_sensitive_data(plugin.get("parameters"), keys)


def date_format(checkpoint):
    """Date format."""
    if checkpoint is None:
        return None
    if isinstance(checkpoint, datetime):
        return checkpoint.isoformat()
    elif isinstance(checkpoint, dict):
        formated_dict = {}
        for key, val in checkpoint.items():
            if val is None:
                formated_dict[key] = None
            if isinstance(val, datetime):
                formated_dict[key] = val.isoformat()
            elif isinstance(val, dict):
                formated_dict[key] = date_format(val)
            else:
                formated_dict[key] = val
        return formated_dict
    return checkpoint


def get_all_fields(plugin_metadata: dict):
    """Return a list of every field object in every step."""
    fields = []
    for step in plugin_metadata.get("configuration", []):
        fields.extend(step.get("fields", []))
    return fields


def get_choice_fields(plugin_metadata: dict):
    """Return (field_dict, choices_list) for each choice-type field."""
    for field in get_all_fields(plugin_metadata):
        if field.get("type") == "choice":
            yield field, field.get("choices", [])


def remove_sensitive_data(data: dict | list, keys_to_remove: list[str]):
    """Remove sensitive data."""
    if isinstance(data, dict):
        for key in list(data.keys()):
            if key in keys_to_remove:
                data.pop(key)
            else:
                remove_sensitive_data(data[key], keys_to_remove)
    elif isinstance(data, list):
        for item in data:
            remove_sensitive_data(item, keys_to_remove)


async def pull_indicators():
    """Pull indicators."""
    data = await pull_statistics()
    return data


async def share_indicators():
    """Share indicators."""
    rules = list(db_connector.collection(Collections.CTE_BUSINESS_RULES).find())
    results = []
    for rule in rules:
        sharedWith: dict = rule.get("sharedWith", {})
        for source, destinations in sharedWith.items():
            for destination in destinations:
                data = await sharing_statistics(
                    rule=rule.get("name"),
                    sourceConfiguration=source,
                    destinationConfiguration=destination,
                )
                results.append(
                    {
                        "source": source,
                        "destination": destination,
                        "rule": rule.get("name"),
                        "indicators": data,
                    }
                )
    return results


def ha_connection():
    """HA connection."""
    return get_status()


def cluster_diagnose():
    """Cluster diagnose."""
    return cluster_status()


if __name__ == "__main__":
    collect = sys.argv[1].strip()
    if collect == "indicators":
        pull_data = asyncio.run(pull_indicators())
        result = json_util.dumps(pull_data, indent=2)

        share_data = asyncio.run(share_indicators())
        resp = json_util.dumps(share_data, indent=2)
        print(result)
        print("\n------- Indicators Share Count With Type --------\n", resp)
    elif collect == "standalone_ha":
        result = ha_connection()
        print("\n------- Monitoring Diagnose --------\n")
        print(json_util.dumps(json_util.loads(result.body.decode()), indent=2))
        data = cluster_diagnose()
        print("\n------- Cluster Diagnose --------\n")
        print(json_util.dumps(data, indent=2))
    else:
        result = diagnose(collect)
        print(result)
