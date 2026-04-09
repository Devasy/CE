"""CLS WebTX Metrics Collector."""
import traceback
from typing import Dict
from netskope.common.utils import (
    Logger,
    Collections,
    DBConnector,
)
from netskope.common.utils.plugin_provider_helper import PluginProviderHelper

logger = Logger()
plugin_provider_helper = PluginProviderHelper()
connector = DBConnector()


def get_webtx_metrics() -> Dict:
    """Get webtx metrics."""
    data = {}
    try:
        tenants = plugin_provider_helper.list_tenants()
        for tenant in tenants:
            cls_config = connector.collection(Collections.CLS_CONFIGURATIONS).find_one(
                {"tenant": tenant.get("name"), "plugin": {"$regex": "netskope_webtx"}}
            )
            if cls_config:
                webtx_data, status_code = next(plugin_provider_helper.get_provider(
                    tenant.get("name")).pull("webtx_metrics"))
                if status_code == 200 and webtx_data:
                    data = webtx_data
                    break
    except Exception:
        logger.error(
            "Error while getting webtx metrics",
            error_code="CLS_1013",
            details=traceback.format_exc()
        )
    return data
