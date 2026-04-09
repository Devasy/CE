"""Utility methods and objects."""
import os
from netskope.common.utils import (
    FILTER_TYPES,
    DBConnector,
    Collections,
    get_database_fields_schema,
    Scheduler,
    Logger,
    Notifier,
)
from netskope.common.utils.plugin_provider_helper import PluginProviderHelper
from netskope.common.models import SettingsDB
from netskope.common.utils.plugin_helper import PluginHelper

from ..models import BusinessRuleDB, ConfigurationDB


connector = DBConnector()
scheduler = Scheduler()
logger = Logger()
notifier = Notifier()
plugin_provider_helper = PluginProviderHelper()


ALERTS_EVENTS_QUERY_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "definitions": {
        **FILTER_TYPES,
        "searchRoot": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                **get_database_fields_schema(),
                "$and": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/searchRoot"},
                },
                "$or": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/searchRoot"},
                },
                "$nor": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/searchRoot"},
                },
            },
        },
    },
    "type": "object",
    "$ref": "#/definitions/searchRoot",
    "additionalProperties": False,
}


def set_utf8_encoding_flag():
    """Set UTF-8 encoding flag."""
    try:
        connector = DBConnector()
        settings = connector.collection(Collections.SETTINGS).find_one({})
        settings = SettingsDB(**settings)
        os.environ["CLS_ENABLE_UTF_8_ENCODING"] = str(settings.cls.utf8Encoding)
    except Exception:
        pass


def schedule_or_delete_third_party_pull_task() -> None:
    """Schedule or delete 3rd party pull tasks."""
    pull_tasks_to_be_removed = [
        schedule["name"]
        for schedule in connector.collection(Collections.SCHEDULES).find(
            {"task": "cls.pull"}
        )
    ]
    for rule in connector.collection(Collections.CLS_BUSINESS_RULES).find({}):
        rule = BusinessRuleDB(**rule)
        for source, destinations in rule.siemMappings.items():
            if not destinations:
                continue
            configuration = connector.collection(
                Collections.CLS_CONFIGURATIONS
            ).find_one({"name": source})
            if configuration is None:
                continue
            configuration = ConfigurationDB(**configuration)
            if (
                configuration.tenant
            ):  # it has a tenant i.e. not a 3rd party plugin
                continue
            if PluginHelper.is_syslog_service_plugin(configuration.plugin):
                # special case for Cloud Exchange Logs plugin
                continue
            if not configuration.active:
                continue
            if f"cls.pull.{source}" in pull_tasks_to_be_removed:
                # task must be kept
                pull_tasks_to_be_removed.remove(f"cls.pull.{source}")
            scheduler.upsert(
                name=f"cls.pull.{source}",
                task_name="cls.pull",
                poll_interval=configuration.pollInterval,
                poll_interval_unit=configuration.pollIntervalUnit,
                args=[source],
            )
    for task in pull_tasks_to_be_removed:
        # remove all the pull tasks that were not encountered in SIEM mappings
        scheduler.delete(task)
