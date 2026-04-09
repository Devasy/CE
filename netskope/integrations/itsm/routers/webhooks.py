"""Webhook related endpoints."""
import traceback
from netskope.common.utils import (
    Logger,
    DBConnector,
    Collections,
    SecretDict,
    PluginHelper
)
from fastapi import APIRouter, HTTPException, Request, Body
from netskope.integrations.itsm.models import ConfigurationDB
from netskope.integrations.itsm.tasks.sync_states import _update_tasks, _end_life
from netskope.integrations.itsm.utils.constants import MAX_BODY_SIZE
from netskope.integrations.itsm.utils.custom_mapping_utils import plugin_to_ce_task_map


helper = PluginHelper()
router = APIRouter()
logger = Logger()
connector = DBConnector()


@router.api_route("/webhook/{webhook_id}", tags=["Webhook listeners"], methods=["POST", "PATCH"])
async def accept_webhook(
    webhook_id: str,
    request: Request,
    body: object = Body(...)
):
    """Accept incoming webhooks."""
    # search for webhook_id field at any level of parameters.

    query = {
        "$expr": {
            "$function": {
                "body": """
                    function(parameters, target) {
                        function search(obj) {
                            for (let key in obj) {
                                if (typeof obj[key] === 'object' && obj[key] !== null) {
                                    if (search(obj[key])) return true;
                                }
                                if (key === 'webhook_id' && typeof obj[key] === 'string') {
                                    let expectedSuffix = "webhook/" + target;
                                    return obj[key].includes(expectedSuffix) && obj[key].endsWith(expectedSuffix);
                                }
                            }
                            return false;
                        }
                        return search(parameters);
                    }
                """,
                "args": ["$parameters", webhook_id],
                "lang": "js"
            }
        }
    }
    webhook_plugin = connector.collection(Collections.ITSM_CONFIGURATIONS).find_one(
        query
    )
    if webhook_plugin is None:
        raise HTTPException(404, "Webhook not found.")
    webhook_config = ConfigurationDB(**webhook_plugin)
    if not webhook_config.active:
        raise HTTPException(404, f"Webhook plugin {webhook_config.name} is not active.")

    PluginClass = helper.find_by_id(webhook_config.plugin)
    if PluginClass is None:
        logger.error(
            f"Could not find the plugin with id='{webhook_config.plugin}'. Skipping the incoming webhook request.",
            error_code="CE_1000",
        )
        return {
            "success": False,
            "message": f"Could not find the plugin with id='{webhook_config.plugin}'."
        }
    current_size = 0
    webhook_payload = b""

    # Stream and check size
    async for chunk in request.stream():
        current_size += len(chunk)

        if (
            current_size > PluginClass.metadata.get("max_webhook_size", MAX_BODY_SIZE)
            if PluginClass and PluginClass.metadata
            else MAX_BODY_SIZE
        ):
            raise HTTPException(
                413,
                "Large Webhook Payload detected, rejecting incoming request. Max allowed size is 10MB."
            )
        webhook_payload += chunk

    try:
        # webhook_payload = await request.body()

        logger.info(
            f"Processing incoming webhook request for ITSM configuration '{webhook_config.name}'."
        )
        plugin = PluginClass(
            webhook_config.name,
            SecretDict(webhook_config.parameters),
            webhook_config.storage if "storage" in webhook_config.model_fields_set else {},
            webhook_config.checkpoint,
            logger,
        )
        tasks, webhook_response = plugin.process_webhooks(request.query_params, request.headers, webhook_payload)

        if not tasks:
            logger.info(
                f"No tasks to update for configuration {webhook_config.name}."
            )
            _ = _end_life(webhook_config.name, True)
            return webhook_response
        updated_tasks = [
            plugin_to_ce_task_map(task, webhook_config)
            for task in tasks
        ]
        _update_tasks(updated_tasks, webhook_config.name)
        logger.info(
            f"Completed processing of incoming webhook request, "
            f"{len(updated_tasks)} task(s) successfully updated for configuration {webhook_config.name}."
        )
        _ = _end_life(webhook_config.name, True)
        return webhook_response
    except HTTPException as e:
        raise HTTPException(
            status_code=e.status_code if e and hasattr(e, "status_code") else 500,
            detail=(
                e.detail
                if e and hasattr(e, "detail")
                else "Unknown error occurred while processing incoming webhook request."
            )
        )
    except NotImplementedError:
        logger.error(
            f"Could not accept incoming webhook with configuration "
            f"'{webhook_config.name}'. process_webhooks method not implemented.",
            details=traceback.format_exc(),
            error_code="CE_1000",
        )
        raise HTTPException(500, "Error occurred while processing webhook, Please check CE platform logs.")
    except Exception:
        logger.error(
            f"Error occurred while processing incoming webhook for {webhook_id}.",
            error_code="CE_1000",
            details=traceback.format_exc()
        )
        raise HTTPException(500, "Error occurred while processing webhook, Please check CE platform logs.")
