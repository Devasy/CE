"""Plugin provider helper class."""
from typing import List
from netskope.common.utils import (
    Singleton,
    Collections,
    Logger,
    DBConnector,
    PluginHelper,
)


class WebTxPluginHelper(metaclass=Singleton):
    """Netskope webtx plugin helper class."""

    def __init__(self):
        """Initialize webtx plugin helper."""
        from netskope.common.utils.back_pressure import back_pressure_mechanism
        self.logger: Logger = Logger()
        self.connector: DBConnector = DBConnector()
        self.plugin_helper: PluginHelper = PluginHelper()
        self.back_pressure_mechanism = back_pressure_mechanism

    def find_cls_configuraions(self, condition={}, many=False):
        """Find cls configurations."""
        if many:
            return self.connector.collection(Collections.CLS_CONFIGURATIONS).find(condition)
        return self.connector.collection(Collections.CLS_CONFIGURATIONS).find_one(condition)

    def find_cls_business_rules(self, condition={}, many=False):
        """Find cls business rules."""
        if many:
            return self.connector.collection(Collections.CLS_BUSINESS_RULES).find(condition)
        return self.connector.collection(Collections.CLS_BUSINESS_RULES).find_one(condition)

    def get_webtx_destination_plugin_ids(self):
        """Get list of plugin ids that support webtx."""
        results = self.connector.collection(Collections.CLS_CONFIGURATIONS).find({})
        for config in results:
            _ = self.plugin_helper.find_by_id(config.get("plugin"))
        plugins = []
        for plugin in self.plugin_helper.plugins["cls"]:
            if not plugin.metadata.get(
                "netskope", False
            ) and "webtx" in plugin.metadata.get("types", []):
                plugins.append(str(plugin.__module__))
        return plugins

    def execute_task(self, task_function, task_name, *args, **kwargs):
        """Schedule task."""
        from netskope.common.celery.scheduler import execute_celery_task
        execute_celery_task(
            task_function, task_name, args=args, **kwargs
        )

    def execute_cls_ingest_task(self, args=[], **kwargs):
        """Execute cls ingest task."""
        from netskope.integrations.cls.tasks.plugin_lifecycle_task import ingest
        self.execute_task(ingest.apply_async, "cls.ingest", *args, **kwargs)

    def execute_cls_parse_and_ingest_task(self, args=[], **kwargs):
        """Execute cls parse and ingest task."""
        from netskope.integrations.cls.tasks.plugin_lifecycle_task import parse_and_ingest_webtx
        self.execute_task(parse_and_ingest_webtx.apply_async, "cls.parse_and_ingest_webtx",
                          *args, **kwargs)

    def get_webtx_destination_configurations(self, plugins: List[str]):
        """Get configurations that support webtx."""
        return list(
            self.connector.collection(Collections.CLS_CONFIGURATIONS).find(
                {"plugin": {"$in": plugins}, "active": True}
            )
        )

    def get_configured_plugins(self, source: str, configurations: List) -> List:
        """Get configured plugins."""
        out = []
        for configuration in configurations:
            rule = self.connector.collection(Collections.CLS_BUSINESS_RULES).find_one(
                {f"siemMappings.{source}": {"$in": [configuration.get("name")]}}
            )
            if rule:
                out.append((rule["name"], configuration))
        return out
