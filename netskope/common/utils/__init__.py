# flake8: noqa
"""Common utils."""

import re
import os
import time
import collections
import inspect
import dateutil.parser
import traceback
from datetime import datetime, UTC
from typing import List, Optional
import json
import gzip
import pandas as pd
import numpy as np
from io import StringIO
from uuid import uuid4

from .db_connector import DBConnector, Collections
from .logger import Logger, LogType
from .notifier import Notifier
from .singleton import Singleton
from .task_decorator import integration, track, get_lock_params, log_mem, release_lock
from .status_decorator import status
from .plugin_base import PluginBase
from .plugin_helper import PluginHelper
from .scheduler import Scheduler

from .repo_manager import RepoManager, PluginStatus
from .update_manager import UpdateManager, UpdateException
from .alerts_helper import AlertsHelper
from .secrets_manager import SecretDict, resolve_secret
from .const import API_MAX_LIMIT
from .data_batch import DataBatchManager

from netskope.common.utils.db_connector import check_mongo_service
from .password_validator import PasswordValidator, get_default_policy
from .requests_retry_mount import MaxRetryExceededException
from .installation import get_installation_id
from netskope.common.api import __version__
from fastapi import HTTPException
from netskope.common.models import FieldDataType
from .const import DB_LOOKUP_INTERVAL

connector = DBConnector()
helper = PluginHelper()
logger_ = Logger()

MODULES = [
    ("cte", Collections.CONFIGURATIONS),
    ("cls", Collections.CLS_CONFIGURATIONS),
    ("itsm", Collections.ITSM_CONFIGURATIONS),
    ("cre", Collections.CREV2_CONFIGURATIONS),
]

FILTER_TYPES = {
    "dateFilters": {
        "anyOf": [
            {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "$gt": {"type": "string"},
                    "$lt": {"type": "string"},
                    "$gte": {"type": "string"},
                    "$lte": {"type": "string"},
                    "$ne": {"type": "string"},
                    "$not": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "$gt": {"type": "string"},
                            "$lt": {"type": "string"},
                            "$gte": {"type": "string"},
                            "$lte": {"type": "string"},
                            "$ne": {"type": "string"},
                        },
                    },
                },
            },
            {"type": "string"},
        ]
    },
    "stringFilters": {
        "anyOf": [
            {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "$eq": {"type": "string"},
                    "$regex": {"type": "string"},
                    "$in": {"type": "array", "items": {"type": ["string", "null"]}},
                    "$nin": {"type": "array", "items": {"type": ["string", "null"]}},
                    "$not": {"type": "string"},
                    "$ne": {"type": "string"},
                },
            },
            {"type": "string"},
        ]
    },
    "numberFilters": {
        "anyOf": [
            {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "$gt": {"type": ["integer", "number"]},
                    "$lt": {"type": ["integer", "number"]},
                    "$gte": {"type": ["integer", "number"]},
                    "$lte": {"type": ["integer", "number"]},
                    "$ne": {"type": ["integer", "number", "null"]},
                    "$eq": {"type": ["integer", "number", "null"]},
                },
            },
            {"type": ["integer", "number", "null"]},
        ]
    },
    "booleanFilters": {
        "anyOf": [
            {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "$eq": {"type": "boolean"},
                    "$ne": {"type": "boolean"},
                },
            },
            {"type": "boolean"},
        ]
    },
    "arrayFilters": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "$in": {"type": "array", "items": {"type": "string"}},
            "$nin": {"type": "array", "items": {"type": "string"}},
        },
    },
}


def _get_regex(regex: str, ignore_regex: bool, exact: bool = True):
    """Get usable regex from a string value."""
    if regex is None:
        regex = ""
    if ignore_regex:
        if exact:
            return f"/^{re.escape(regex)}$/i"
        else:
            return f"/{re.escape(regex)}/i"
    if exact:
        return re.compile(f"^{re.escape(regex)}$", re.IGNORECASE)
    else:
        return re.compile(f"{re.escape(regex)}", re.IGNORECASE)


def add_legacy_prefix(key: str, nested: bool = False) -> str:
    """Rename prefix from filter."""
    if key.startswith("rawAlert_"):
        return f"rawAlert.{key[9:]}"
    if key.startswith("alert_") and not nested:
        return f"alert.{key[6:]}"
    if key.startswith("rawData_"):
        return f"rawData.{add_legacy_prefix(key[8:])}"
    if key.startswith("dataItem_"):
        return f"dataItem.{add_legacy_prefix(key[9:])}"
    return key


def parse_datetime(value: str) -> Optional[datetime]:
    try:
        if type(value) is str and not value.isdigit():
            return dateutil.parser.parse(value).replace(tzinfo=None)
        else:
            return None
    except (ValueError, TypeError):
        return None


def flatten(d, parent_key="", sep="."):
    """Flatten a dictionary."""
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.abc.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def parse_dates(
    pair: dict,
    string_fields: List[str] = [],
    ignore_regex=False,
    add_legecy_prefix=True,
    parse_datetime=parse_datetime,
) -> dict:
    """Parse datetime strings."""
    out = {}
    for key, value in pair.items():
        if add_legecy_prefix:
            key = add_legacy_prefix(key)
        key = key.replace(
            "%", "."
        )  # using `%` as `.` because parsing related issues in UI
        if datetime := parse_datetime(value):
            out[key] = datetime
        elif type(value) is int:  # no need to do anything with numbers
            out[key] = value
        elif key in ["$ne", "$not"] and type(value) is str:
            out["$not"] = {
                "$regex": _get_regex(value, ignore_regex, exact=key == "$ne")
            }
        elif key in ["$eq", "$regex"] and type(value) is str:
            out["$regex"] = _get_regex(value, ignore_regex, exact=key != "$regex")
        elif type(value) is str:
            out[key] = {
                "$regex": _get_regex(value, ignore_regex, exact=key != "$regex")
            }
        elif type(value) is list:
            out[key] = list(map(lambda v: None if v == "null" else v, value))
        else:
            out[key] = value
    return out


def add_user_agent(header=None) -> dict:
    if header is None:
        header = {"User-Agent": "netskope-ce-" + __version__}
    else:
        if "User-Agent" not in header:
            header["User-Agent"] = "netskope-ce-" + __version__
    return header


def update_analytics_shared_at() -> None:
    try:
        date = datetime.now(UTC)
        connector.collection(Collections.SETTINGS).update_one(
            {}, {"$set": {"analyticsSharedAt": date}}
        )
    except Exception as e:
        Logger().debug(f"Failed to update analytics sharedat, {e}")


def add_installation_id(header=None) -> dict:
    """Add installation id to the header dict.

    Args:
        header (dict, optional): Existing headers dict. Defaults to None.

    Returns:
        dict: Headers dict with installation id.
    """
    installation_id = get_installation_id()
    if header is None:
        return {"X-CE-Installation-Id": installation_id}
    else:
        if "X-CE-Installation-Id" not in header:
            header["X-CE-Installation-Id"] = installation_id
    return header


def delete_duplicate_indicators():
    try:
        duplicates = connector.collection(Collections.INDICATORS).aggregate(
            [
                {
                    "$group": {
                        "_id": "$value",
                        "duplicates": {"$push": "$_id"},
                        "count": {"$sum": 1},
                    }
                },
                {"$match": {"count": {"$gt": 1}}},
            ],
            allowDiskUse=True,
        )
        for data in duplicates:
            duplicate_ids = data.get("duplicates", [])[1:]
            if duplicate_ids:
                connector.collection(Collections.INDICATORS).delete_many(
                    {"_id": {"$in": duplicate_ids}}
                )
        return True
    except Exception:
        print("Error occurred while deleting duplicates values", traceback.format_exc())
        return False


def get_sub_type_config_mapping(
    tenant_name, data_type, latest_checked=None, sub_type_configuration_mapping=None
):
    now = datetime.now()
    if (
        not latest_checked
        or (now - latest_checked).total_seconds() >= DB_LOOKUP_INTERVAL
    ):
        settings = connector.collection(Collections.SETTINGS).find_one({})
        latest_checked = datetime.now()

        sub_type_configuration_mapping = collections.defaultdict(set)

        for module, collection in MODULES:
            if not settings.get("platforms", {}).get(module, False):
                continue
            for configuration in connector.collection(collection).find(
                {"tenant": tenant_name}
            ):
                try:
                    PluginClass = helper.find_by_id(configuration["plugin"])  # NOSONAR
                    if not PluginClass or not configuration["active"]:
                        continue

                    plugin = PluginClass(
                        configuration["name"],
                        SecretDict(configuration["parameters"]),
                        configuration.get(
                            "storage", {}
                        ),  # No need to update storage as it get call
                        None,
                        logger_,
                    )
                    if configuration.get("mappedEntities"):
                        plugin.mappedEntities = configuration.get("mappedEntities")

                    plugin_sub_types = plugin.get_types_to_pull(data_type)

                    for sub_type in plugin_sub_types:
                        sub_type_configuration_mapping[sub_type].add(
                            configuration["name"]
                        )
                except Exception:
                    logger_.error(
                        "Error occurred while getting sub type from configuration "
                        f"{configuration.get('name')} with id={configuration.get('plugin')}.",
                        error_code="CE_1132",
                        details=traceback.format_exc(),
                    )
                    continue

    return sub_type_configuration_mapping, latest_checked


def validate_limit(limit: int = 10):
    """Validate the provided limit.

    Parameters:
        limit (int): The limit to validate. Defaults to 10.
    Raises:
        HTTPException: If the limit is greater than 100.
    Returns:
        None
    """
    if limit > API_MAX_LIMIT:
        raise HTTPException(
            400,
            f"Limit cannot be greater than {API_MAX_LIMIT}. Provided limit: {limit}.",
        )


def get_database_fields_schema():
    """Get database fields schema.

    Returns:
        dict: Dict for fields schema.
    """
    # Database fields dictionary to store the schema definitions
    DATABASE_FIELDS = {}

    # Mapping data types to their corresponding filters schema
    FIELD_SCHEMAS = {
        FieldDataType.NUMBER: {"$ref": "#/definitions/numberFilters"},
        FieldDataType.DATETIME: {"$ref": "#/definitions/dateFilters"},
        FieldDataType.BOOLEAN: {"$ref": "#/definitions/booleanFilters"},
        FieldDataType.TEXT: {"$ref": "#/definitions/stringFilters"},
    }

    # Iterate through the list of fields and process them
    for field in connector.collection(Collections.NETSKOPE_FIELDS).find({}):
        field_datatype = FieldDataType(field.get("dataType", FieldDataType.TEXT))
        schema = FIELD_SCHEMAS.get(field_datatype, FIELD_SCHEMAS[FieldDataType.TEXT])

        if field.get("name") in DATABASE_FIELDS:
            # If field already exists, add the new schema to 'anyOf' if needed
            current_schema = DATABASE_FIELDS[field["name"]].get(
                "anyOf", [DATABASE_FIELDS[field["name"]]]
            )
            DATABASE_FIELDS[field.get("name")] = {"anyOf": [schema] + current_schema}
        else:
            # If field doesn't exist, simply add the schema
            DATABASE_FIELDS[field.get("name")] = schema
    return DATABASE_FIELDS


def deep_stringify(obj):
    """_summary_

    Args:
        obj (_type_): _description_

    Returns:
        _type_: _description_
    """
    if isinstance(obj, dict):
        return {
            k: json.dumps(v) if isinstance(v, (dict, list)) else v
            for k, v in obj.items()
        }
    elif isinstance(obj, list):
        return [deep_stringify(item) for item in obj]
    return obj


def convert_numpy_types(obj):
    if isinstance(obj, dict):
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(v) for v in obj]
    elif isinstance(obj, np.generic):  # catches np.int64, np.float64, etc.
        return obj.item()
    else:
        return obj


def parse_csv_response(df: pd.DataFrame):
    """Parse complex datatype in CSV response."""

    # Step 1: Group columns by root key
    column_groups = collections.defaultdict(list)
    for col in df.columns:
        if "." in col:
            root = col.split(".")[0]
            column_groups[root].append(col)

    # Step 2: Create a new dataframe column-wise
    result = pd.DataFrame()

    # Direct (non-nested) columns and check for list conversion
    for col in df.columns:
        if "." not in col:
            result[col] = df[col]

    # Nested columns
    for root, cols in column_groups.items():
        nested_values = []
        print(len(df))
        for i in range(len(df)):
            temp = {}
            for col in cols:
                keys = col.split(".")[1:]  # exclude root
                val = df.at[i, col]
                cur = temp
                for k in keys[:-1]:
                    cur = cur.setdefault(k, {})
                cur[keys[-1]] = val

            nested_values.append(convert_numpy_types(temp))

        result[root] = nested_values
    return result.to_dict(orient="records")


def get_dynamic_fields_from_plugin(plugin_id: str, config_details: dict) -> dict:
    """Get dynamic fields for a given plugin.

    Args:
        plugin_id (str): Plugin id.
        config_details (dict): configuration parameters.

    Returns:
        dict: List of dynamic fields.
    """
    PluginClass = helper.find_by_id(plugin_id)  # NOSONAR S117
    if PluginClass is None:
        raise HTTPException(400, f"Plugin with id='{plugin_id}' does not exist.")
    plugin = PluginClass(None, SecretDict(config_details), None, None, logger_)
    try:
        return plugin.get_dynamic_fields()
    except NotImplementedError:
        raise HTTPException(400, "Plugin does not implement dynamic fields.")
    except Exception:
        logger_.error(
            f"Error occurred while getting dynamic fields with plugin id='{plugin_id}'.",
            details=traceback.format_exc(),
            error_code="CE_1053",
        )
        raise HTTPException(
            400, "Error occurred while getting dynamic fields. Check logs."
        )


def parse_events(
    events: bytes,
    tenant_config_name: str = None,
    configuration: object = None,
    data_type: str = None,
    sub_type: str = None,
) -> list[dict]:
    """Parse event bytes into list of dictionaries.

    Args:
        events (bytes): Event bytes. May be JSON or CSV.

    Returns:
        list[dict]: List of events.
    """
    try:
        if tenant_config_name:
            from netskope.common.models.tenant import TenantDB

            tenant = TenantDB(
                **connector.collection(Collections.NETSKOPE_TENANTS).find_one(
                    {"name": tenant_config_name}
                )
            )
            ProviderClass = helper.find_by_id(tenant.plugin)
            provider = ProviderClass(
                tenant.name,
                SecretDict(tenant.parameters),
                tenant.storage,
                datetime.now(),
                logger_,
            )
            try:
                return provider.parse_data(events, data_type, sub_type)
            except NotImplementedError:
                pass
        if configuration:
            PluginClass = helper.find_by_id(configuration.plugin)  # NOSONAR
            plugin = PluginClass(
                configuration.name,
                SecretDict(configuration.parameters),
                (
                    configuration.storage
                    if "storage" in configuration.model_fields_set
                    else {}
                ),
                None,
                logger_,
            )
            try:
                return plugin.parse_data(events, data_type, sub_type)
            except NotImplementedError:
                pass

        decompressed_events = gzip.decompress(events)
        try:
            return json.loads(decompressed_events)
        except json.decoder.JSONDecodeError:
            return parse_csv_response(
                pd.read_csv(
                    StringIO(decompressed_events.decode("utf-8")), keep_default_na=False
                )
            )
    except Exception:
        file_path = None
        try:
            default_base_path = "/opt/netskope/plugins/custom_plugins"
            base_path = os.getenv("FAILED_EVENTS_PATH", default_base_path)
            os.makedirs(base_path, exist_ok=True)
            fname = f"failed_events_{str(uuid4())}_at_{int(time.time())}.gz"
            file_path = os.path.join(base_path, fname)
            with open(file_path, "wb") as f:
                logger_.debug(
                    f"Received an unparsable data. Writing it to file '{fname}'."
                )
                f.write(events)
        except Exception as e:
            logger_.error(
                f"Error occurred while writing compressed data to file.",
                details=traceback.format_exc(),
                error_code="CE_1059",
            )
        logger_.error(
            f"Error occurred while parsing pulled data."
            + (f" Pulled data will be stored in '{file_path}'." if file_path else ""),
            error_code="CE_1058",
            details=traceback.format_exc(),
        )


def has_source_info_args(cls, method_name: str, args: list):
    """Check wether method has required args for sharing source labeling information."""
    method = getattr(cls, method_name, None)
    if method is None:
        return False
    # check for args in method.
    signature = inspect.signature(method)
    for arg in args:
        if arg not in signature.parameters:
            return False
    return True


def get_change_log_till_version(md_content, current_version, target_version):
    """
    Extracts change log from a Markdown string between two versions.
    This function supports semantic versioning with suffixes like -hotfix, -beta, etc.

    The function handles several scenarios:
    1. If target version changelog is not present: returns the whole changelog
    2. If upgrading one version: returns only that specific changelog section
    3. If upgrading multiple versions: returns all relevant changelog sections

    Args:
        md_content (str): The string content of the Markdown file.
        current_version (str): The current plugin version.
        target_version (str): The target version to upgrade to.

    Returns:
        str: The change log sections relevant to the upgrade from current_version
            to target_version. Returns the entire changelog if the target version
            is not found in the changelog.
    """
    current_version_match = re.match(r"(\d+\.\d+\.\d+)", current_version)
    if current_version_match:
        current_version = current_version_match.group(1)

    target_version_match = re.match(r"(\d+\.\d+\.\d+)", target_version)
    if target_version_match:
        target_version = target_version_match.group(1)

    regex = r"^#\s(\d+\.\d+\.\d+(?:-[a-zA-Z0-9.-]+)?)"

    # Use regex to find all version headers and their starting positions
    versions = list(re.finditer(regex, md_content, re.MULTILINE))

    # Find the index of the target version in the list of matches
    current_index = -1
    target_index = -1
    for i, match in enumerate(versions):
        # group(1) captures the full version string (e.g., "3.0.0-hotfix")
        if match.group(1) == current_version:
            current_index = i
            break
        elif match.group(1) == target_version:
            target_index = i

    # If the target version is the last one in the file, return everything
    if current_index == -1 or current_index == len(versions) - 1 or target_index == -1:
        return md_content.strip()

    # Get the match object for the version *after* our target
    next_version_header_match = versions[current_index]

    # Get the starting character position of that next version's header
    end_position = next_version_header_match.start()

    # Slice the original content from the beginning to that position
    return md_content[:end_position].strip()
