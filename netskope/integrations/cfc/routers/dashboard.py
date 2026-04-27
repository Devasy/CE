"""Provides CFC dashboard related endpoints."""
import traceback

from fastapi import APIRouter, HTTPException, Security

from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User
from netskope.common.utils import DBConnector, Logger, Collections, PluginHelper
from netskope.integrations.cfc.models import ConfigurationDB, CFCStatisticsDB

router = APIRouter(prefix="/dashboard", tags=["CFC Dashboard"])
logger = Logger()
db_connector = DBConnector()
plugin_helper = PluginHelper()


def _check_push_supported_plugin(plugin_id: str):
    """Check the plugin is push supported."""
    metadata = plugin_helper.find_by_id(plugin_id).metadata
    return bool(metadata.get("push_supported", False))


@router.get(
    "/summary",
)
async def get_summary(
    _: User = Security(get_current_user, scopes=["cfc_read"])
):
    """
    Retrieve the summary data.

    Args:
        _(User): The current user, obtained from the security provider.

    Returns:
        dict: A dictionary containing the summary data.

    Raises:
        HTTPException: If an error occurs while retrieving the summary data.
    """
    try:
        source_configuration_count = 0
        configurations = db_connector.collection(Collections.CFC_CONFIGURATIONS).find({})
        for configuration in configurations:
            metadata = plugin_helper.find_by_id(configuration["plugin"]).metadata
            if metadata.get("pull_supported", True):
                source_configuration_count = source_configuration_count + 1

        # Write logic to include manual upload in the source config count

        cfc_statistics = db_connector.collection(Collections.CFC_STATISTICS).find_one({})
        trainedClassifiers = list(db_connector.collection(Collections.CFC_IMAGES_METADATA).aggregate(
            [
                {"$match": {"sharedWith": {"$elemMatch": {"lastShared": {"$exists": True}}}}},
                {"$unwind": "$sharedWith"},
                {"$group": {
                    "_id": {"destination": "$sharedWith.destinationPlugin", "classifier": "$sharedWith.classifierName"},
                }}
            ],
            allowDiskUse=True,
        ))

        if cfc_statistics:
            statistics_details = CFCStatisticsDB(**cfc_statistics)
            return {
                "sourceCount": source_configuration_count,
                "sentCount": statistics_details.sentImages,
                "trainedClassifierCount": len(list(trainedClassifiers)),
            }
        return {
            "sourceCount": source_configuration_count,
            "sentCount": 0,
            "receivedCount": 0
        }
    except Exception as error:
        error_message = "Error occurred while retrieving the summary data."
        logger.error(
            error_message,
            details=traceback.format_exc(),
            error_code="CFC_1038",
        )
        raise HTTPException(
            500, error_message
        ) from error


@router.get(
    "/plugin_update_details",
)
async def get_plugin_update_details(
    limit: int = 10,
    _: User = Security(get_current_user, scopes=["cfc_read"])
):
    """
    Retrieve the plugin update details.

    Args:
        limit (int): The number of records to return. Defaults to 10.
        _(User): The current user, obtained from the security provider.

    Returns:
        dict: A dictionary containing the plugin update details.

    Raises:
        HTTPException: If an error occurs while retrieving the plugin update details.
    """
    plugin_updates = []
    try:
        configurations = db_connector.collection(Collections.CFC_CONFIGURATIONS).find({})
        for configuration_data in configurations:
            configuration = ConfigurationDB(**configuration_data)

            plugin_name = configuration.name
            plugin_update_user = configuration.createdBy
            plugin_update_time = configuration.createdAt

            if configuration.lastUpdatedBy and configuration.lastUpdatedAt:
                plugin_update_user = configuration.lastUpdatedBy
                plugin_update_time = configuration.lastUpdatedAt

            plugin_updates.append({
                "pluginName": plugin_name,
                "pluginUpdateUser": plugin_update_user,
                "pluginUpdateTime": plugin_update_time,
            })
        plugin_updates = sorted(plugin_updates, key=lambda data: data["pluginUpdateTime"], reverse=True)
        return plugin_updates[:limit]
    except Exception as error:
        error_message = "Error occurred while retrieving the plugin update details."
        logger.error(
            error_message,
            details=traceback.format_exc(),
            error_code="CFC_1027",
        )
        raise HTTPException(
            500, error_message
        ) from error


@router.get(
    "/destination_configuration_distribution",
)
async def get_destination_configuration_distribution(
    _: User = Security(get_current_user, scopes=["cfc_read"])
):
    """
    Retrieve the destination configuration distribution.

    Args:
        _(User): The current user, obtained from the security provider.

    Returns:
        dict: A dictionary containing the destination configuration distribution.

    Raises:
        HTTPException: If an error occurs while retrieving the destination configuration distribution.
    """
    try:
        destination_plugins = db_connector.collection(Collections.CFC_CONFIGURATIONS).find({})
        destination_plugins = list(
            map(
                lambda plugin: plugin["name"], filter(
                    lambda plugin: _check_push_supported_plugin(plugin["plugin"]),
                    destination_plugins
                )
            )
        )
        groups = db_connector.collection(Collections.CFC_SHARING).aggregate(
            [
                {"$match": {}},
                {"$group": {
                    "_id": "$destinationConfiguration",
                    "count": {"$sum": 1}
                }}
            ],
            allowDiskUse=True,
        )
        destination_distribution = {group["_id"]: group["count"] for group in groups}
        for plugin in destination_plugins:
            destination_distribution.setdefault(plugin, 0)
        return destination_distribution
    except Exception as error:
        error_message = "Error occurred while retrieving the destination configuration distribution."
        logger.error(
            error_message,
            details=traceback.format_exc(),
            error_code="CFC_1039",
        )
        raise HTTPException(
            500, error_message
        ) from error


@router.get(
    "/shared_images_distribution/{destinationConfigurationName}",
)
async def get_shared_images_distribution(
    destinationConfigurationName: str,
    _: User = Security(get_current_user, scopes=["cfc_read"]),
):
    """
    Retrieve the shared images distribution.

    Args:
        destinationConfigurationName(str): Destination configuration for which to prepare the distribution.
        _(User): The current user, obtained from the security provider.

    Returns:
        dict: A dictionary containing the shared images distribution.

    Raises:
        HTTPException: If an error occurs while retrieving the shared images distribution.
    """
    try:
        if not db_connector.collection(Collections.CFC_CONFIGURATIONS).find_one(
            {"name": destinationConfigurationName}
        ):
            raise HTTPException(400, f"No configuration with name='{destinationConfigurationName}' exists.")
        groups = list(db_connector.collection(Collections.CFC_IMAGES_METADATA).aggregate(
            [
                {
                    "$match": {
                        "sharedWith": {
                            "$elemMatch": {
                                "destinationPlugin": destinationConfigurationName,
                                "lastShared": {"$exists": True}
                            }
                        }
                    }
                },
                {"$unwind": "$sharedWith"},
                {"$group": {
                    "_id": "$sharedWith.classifierName",
                    "destinations": {"$push": "$sharedWith"}
                }}
            ],
            allowDiskUse=True,
        ))
        result = {}
        for group in groups:
            files_by_classifier = len(list(filter(
                lambda item: item["destinationPlugin"] == destinationConfigurationName and item["lastShared"],
                group["destinations"]
            )))
            if files_by_classifier:
                result[group["_id"]] = files_by_classifier
        return result
    except Exception as error:
        error_message = "Error occurred while retrieving the shared images distribution."
        logger.error(
            error_message,
            details=traceback.format_exc(),
            error_code="CFC_1040",
        )
        raise HTTPException(
            500, error_message
        ) from error
