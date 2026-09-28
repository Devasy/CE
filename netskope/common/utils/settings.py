"""Settings utils."""

from .db_connector import Collections, DBConnector
from .logger import Logger

VALID_INTEGRATIONS_GROUPS = [
    set(["cte", "itsm", "cls", "cre"]),
    set(["edm", "cfc"]),
]

_connector = DBConnector()
_logger = Logger()

# settings.platforms stores the CTO module toggle under its ITSM name.
CTO_PLATFORM_KEY = "itsm"


def is_platform_enabled(name: str) -> bool:
    """Return True if the given platform/module is currently enabled.

    Args:
        name: Platform key stored under ``settings.platforms`` (e.g. "cte", "cre").
    """
    settings = _connector.collection(Collections.SETTINGS).find_one({}) or {}
    return bool(settings.get("platforms", {}).get(name, False))


def cto_alerts_enabled(settings) -> bool:
    """Return True if module-generated alerts can be stored on CTO.

    ``itsm.store_cte_alerts``/``itsm.store_cre_alerts`` are no-ops while the CTO
    module is disabled, so callers must not build or report alerts either.

    Args:
        settings: The already loaded ``SettingsDB``.
    """
    return bool((settings.platforms or {}).get(CTO_PLATFORM_KEY, False))


def log_cto_alerts_skipped(context: str) -> None:
    """Record that CTO alert generation was skipped for something that wanted it.

    Disabling a module is an intentional admin action, not a failure, so this is
    debug level and callers emit it once per run and only when an action
    actually asked for an alert — never on every share cycle.

    Args:
        context: What the alerts would have been generated for (e.g. a
            destination configuration or unified mapping).
    """
    _logger.debug(
        f"Skipped alert generation on CTO for {context} because the CTO module "
        "is disabled. Enable the CTO module to resume generating alerts."
    )
