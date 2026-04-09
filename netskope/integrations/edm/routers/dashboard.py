"""Provides EDM dashboard related endpoints."""
import traceback

from fastapi import APIRouter, HTTPException, Security

from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User
from netskope.common.utils import DBConnector, Logger, Scheduler, Collections
from netskope.common.utils.plugin_helper import PluginHelper
from netskope.integrations.edm.models import ConfigurationDB, EDMStatisticsDB, EDMActions

router = APIRouter(prefix="/dashboard", tags=["EDM Dashboard"])
scheduler = Scheduler()
plugin_helper = PluginHelper()
logger = Logger()
db_connector = DBConnector()


def _get_sent_data_details(action_name: EDMActions) -> list:
    """
    Retrieve the details of sent data based on the specified action name.

    Args:
        action_name (EDMActions): The name of the action to search for.

    Returns:
        list: A list of dictionaries containing the details of the sent data.
            Each dictionary includes the following keys:
            - filename (str): The name of the file.
            - configurationName (str): The name of the destination configuration.
            - lastSharedTime (datetime): The last shared time of the data.
    """
    business_rules = db_connector.collection(Collections.EDM_BUSINESS_RULES).find({})
    sent_data = []

    for business_rule in business_rules:
        sharing_details = business_rule.get("sharedWith", {})
        source_configuration_name, sharing_configuration_details = next(iter(sharing_details.items()), (None, {}))
        destination_configuration_name, action_details = next(iter(sharing_configuration_details.items()), (None, {}))

        # Check if the specified action is present in the action details
        if destination_configuration_name and any(action_name.value in action["value"] for action in action_details):

            # Retrieve configuration details
            configuration_details = db_connector.collection(Collections.EDM_CONFIGURATIONS).find_one(
                {"name": destination_configuration_name}
            )
            configuration = ConfigurationDB(**configuration_details)
            filename = configuration.storage.get(source_configuration_name, {}).get("file_name")
            last_shared_time = business_rule.get("sharedAt")

            # Check if filename and last shared time are available
            if filename and last_shared_time:
                sent_data.append({
                    "filename": filename,
                    "configurationName": destination_configuration_name,
                    "lastSharedTime": last_shared_time
                })

    manual_upload_configurations = db_connector.collection(Collections.EDM_MANUAL_UPLOAD_CONFIGURATIONS).find({})
    for configuration in manual_upload_configurations:
        sharing_details = configuration.get("sharedWith", {})
        destination_configuration_name, action_details = next(iter(sharing_details.items()), (None, {}))
        # Check if the specified action is present in the action details
        if destination_configuration_name and any(action_name.value in action["value"] for action in action_details):
            filename = configuration["fileName"]
            shared_time = configuration.get("sharedAt")
            if filename and shared_time:
                sent_data.append({
                    "filename": filename,
                    "configurationName": destination_configuration_name,
                    "lastSharedTime": shared_time
                })

    return sorted(sent_data, key=lambda data: data["lastSharedTime"], reverse=True)


@router.get(
    "/sent/{action_name}"
)
async def get_sent_details(
    action_name: EDMActions,
    limit: int = 10,
    _: User = Security(get_current_user, scopes=["edm_read"])
):
    """
    Retrieve the details of the data sent to another CE/Tenant for a specific action.

    Args:
        action_name (EDMActions): The name of the action to search for.
        limit (int): The number of records to return. Defaults to 10.
        _(User): The current user, obtained from the security provider.

    Returns:
        list: A list of dictionaries containing the details of the sent data.

    Raises:
        HTTPException: If an error occurs while retrieving the data.
    """
    try:
        send_data_details = _get_sent_data_details(action_name)
        return send_data_details[:limit]
    except Exception as error:
        error_message = ("Error occurred while retrieving the data sent to another CE/Tenant "
                         f"for action '{action_name.value}'.")
        logger.error(
            error_message,
            details=traceback.format_exc(),
            error_code="EDM_1024",
        )
        raise HTTPException(
            500, error_message
        ) from error


@router.get(
    "/received/{action_name}",
)
async def get_received_details(
    action_name: EDMActions,
    limit: int = 10,
    _: User = Security(get_current_user, scopes=["edm_read"])
):
    """
    Retrieve the details of the data received from another CE for a specific action.

    Args:
        action_name (EDMActions): The name of the action to search for.
        limit (int): The number of records to return. Defaults to 10.
        _(User): The current user, obtained from the security provider.

    Returns:
        list: A list of dictionaries containing the details of the received data.

    Raises:
        HTTPException: If an error occurs while retrieving the data.
    """
    try:
        # Retrieve receiver configurations
        receiver_configurations = db_connector.collection(Collections.EDM_CONFIGURATIONS).find(
            {"pluginType": "receiver"}
        )
        received_data_details = []
        # Iterate through receiver configurations to collect received data details
        for configuration in receiver_configurations:
            configuration_dict = ConfigurationDB(**configuration)
            for _, received_details in configuration_dict.storage.items():
                if (
                    isinstance(received_details, dict)
                    and "edm_hash_folder" in received_details
                ):
                    filename = received_details.get("file_name")
                    modification_time = received_details.get("last_received_time")
                    if filename and modification_time:
                        received_data_details.append({
                            "filename": filename,
                            "configurationName": configuration_dict.name,
                            "lastReceivedTime": modification_time
                        })
        received_data_details = sorted(received_data_details, key=lambda data: data["lastReceivedTime"], reverse=True)
        return received_data_details[:limit]
    except Exception as error:
        error_message = (
            "Error occurred while retrieving the received data "
            f"from another CE for action '{action_name.value}'."
        )
        logger.error(
            error_message,
            details=traceback.format_exc(),
            error_code="EDM_1025",
        )
        raise HTTPException(
            500, error_message
        ) from error


@router.get(
    "/summary/{action_name}",
)
async def get_summary(
    action_name: EDMActions,
    _: User = Security(get_current_user, scopes=["edm_read"])
):
    """
    Retrieve the summary data for a given action name.

    Args:
        action_name (EDMActions): The name of the action to retrieve the summary data for.
        _(User): The current user, obtained from the security provider.

    Returns:
        dict: A dictionary containing the summary data for the specified action.

    Raises:
        HTTPException: If an error occurs while retrieving the summary data.
    """
    try:
        source_configuration_count = 0
        configurations = db_connector.collection(Collections.EDM_CONFIGURATIONS).find({})
        for configuration in configurations:
            PluginClass = plugin_helper.find_by_id(configuration["plugin"])  # NOSONAR S117
            if not PluginClass:
                raise HTTPException(
                    400,
                    f"Plugin with id='{configuration['plugin']}' does not exist."
                )
            metadata = PluginClass.metadata
            if metadata.get("pull_supported", True) and configuration.get("pluginType", "") != "forwarder":
                source_configuration_count = source_configuration_count + 1

        manual_upload_configurations = db_connector.collection(Collections.EDM_MANUAL_UPLOAD_CONFIGURATIONS).find({})
        for configuration in manual_upload_configurations:
            source_configuration_count = source_configuration_count + 1

        edm_statistics = db_connector.collection(Collections.EDM_STATISTICS).find_one({})

        if edm_statistics:
            statistics_details = EDMStatisticsDB(**edm_statistics)
            return {
                "sourceCount": source_configuration_count,
                "sentCount": statistics_details.sentHashes,
                "receivedCount": statistics_details.receivedHashes
            }
        return {
            "sourceCount": source_configuration_count,
            "sentCount": 0,
            "receivedCount": 0
        }
    except Exception as error:
        error_message = f"Error occurred while retrieving the summary data for action '{action_name.value}'."
        logger.error(
            error_message,
            details=traceback.format_exc(),
            error_code="EDM_1026",
        )
        raise HTTPException(
            500, error_message
        ) from error


@router.get(
    "/upload_distribution/{action_name}",
)
async def get_upload_data_distribution(
    action_name: EDMActions,
    _: User = Security(get_current_user, scopes=["edm_read"])
):
    """
    Retrieve the upload data distribution for a given action name.

    Args:
        action_name (EDMActions): The name of the action to retrieve the upload data distribution for.
        _(User): The current user, obtained from the security provider.

    Returns:
        dict: A dictionary containing the upload data distribution for the specified action.

    Raises:
        HTTPException: If an error occurs while retrieving the upload data distribution.
    """
    try:
        upload_data_details = _get_sent_data_details(action_name)
        upload_data_distribution = {}
        for upload_data in upload_data_details:
            configuration_name = upload_data["configurationName"]
            if configuration_name in upload_data_distribution:
                upload_data_distribution[configuration_name] = (
                    upload_data_distribution[configuration_name] + 1
                )
            else:
                upload_data_distribution[configuration_name] = 1
        return upload_data_distribution
    except Exception as error:
        error_message = (
            "Error occurred while retrieving the upload data "
            f"distribution for action '{action_name.value}'."
        )
        logger.error(
            error_message,
            details=traceback.format_exc(),
            error_code="EDM_1027",
        )
        raise HTTPException(
            500, error_message
        ) from error


@router.get(
    "/plugin_update_details",
)
async def get_plugin_update_details(
    limit: int = 10,
    _: User = Security(get_current_user, scopes=["edm_read"])
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
        configurations = db_connector.collection(Collections.EDM_CONFIGURATIONS).find({})
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
            error_code="EDM_1028",
        )
        raise HTTPException(
            500, error_message
        ) from error
