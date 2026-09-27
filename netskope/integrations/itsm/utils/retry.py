"""Retry mechanism for ITSM."""
from typing import Union

from netskope.integrations.itsm.models import (
    Alert,
    BusinessRuleDB,
    ConfigurationDB,
    DataType,
    Event,
)
from netskope.common.utils import (
    DBConnector,
    Collections,
    PluginHelper,
    Logger,
    SecretDict,
)
from netskope.integrations.itsm.utils.tickets import create_tickets_or_requests
import traceback


connector = DBConnector()
logger = Logger()
helper = PluginHelper()


def _create_tickets_for_failed_task(
    data_item: Union[Alert, Event],
    rule: str,
    id,
    configuration=None,
    data_type: DataType = DataType.ALERT,
):
    """Retry to Create tickets for alerts/events for a specific business rule."""
    configuration_ = connector.collection(Collections.ITSM_CONFIGURATIONS).find_one({"name": configuration})
    if configuration_ is None:          # if configuration is deleted then return.
        logger.error(
            "Error occurred while retrying ticket creation. "
            f"Ticket Orchestrator configuration {configuration} no longer exists.",
            error_code="CTO_1026",
        )
        return
    business_rule = connector.collection(Collections.ITSM_BUSINESS_RULES).find_one({"name": rule})
    if business_rule is None:       # if business rule deleted then return.
        logger.error(
            "Error occurred while retrying ticket creation. "
            f"Ticket Orchestrator business rule {rule} no longer exists.",
            error_code="CTO_1024",                              # update the error code.
        )
        return
    business_rule = BusinessRuleDB(**business_rule)
    if not business_rule.queues:    # if queue is deleted then return.
        logger.error(
            "Error occurred while retrying ticket creation. "
            f"Queue for Ticket Orchestrator business rule {rule} no longer exists.",
            error_code="CTO_1025",                              # update the error code.
        )
        return
    for name, queues in (
        business_rule.queues.items()
        if configuration is None
        else [(configuration, business_rule.queues[configuration])]
    ):
        configuration = connector.collection(Collections.ITSM_CONFIGURATIONS).find_one({"name": name})
        if configuration is None:
            logger.error(
                "Error occurred while retrying ticket creation. "
                f"Ticket Orchestrator configuration {configuration} no longer exists.",
                error_code="CTO_1026",
            )
            continue
        try:
            configuration = ConfigurationDB(**configuration)
            PluginClass = helper.find_by_id(configuration.plugin)  # NOSONAR S117
            plugin = PluginClass(
                configuration.name,
                SecretDict(configuration.parameters),
                configuration.storage,
                configuration.checkpoint,
                logger,
            )
            result = create_tickets_or_requests(
                plugin=plugin,
                data_item=data_item,
                rule=business_rule,
                queues=queues,
                configuration=configuration,
                data_type=data_type,
                retry=True,
                _id=id,
            )
            if not result["failed"]:
                logger.info(
                    "Successfully created/updated task for "
                    f"{data_type.value} with ID {data_item.id} "
                    f"for configuration {configuration.name} from the request."
                )
            connector.collection(Collections.ITSM_CONFIGURATIONS).update_one(
                {"name": configuration.name},
                {"$set": {"storage": plugin.storage}},
            )
        except Exception as ex:
            logger.error(
                f"Could not create/update task for {data_type.value} "
                f"with ID {data_item.id} for configuration {configuration.name}. {ex}",
                error_code="CTO_1027",
                details=traceback.format_exc()
            )
    return {"success": True}
