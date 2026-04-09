"""Proxy related utility methods."""
from netskope.common.models.settings import SettingsDB
import urllib.parse
import os


def get_proxy_params(settings: SettingsDB) -> dict:
    """Get proxy dictionary from the database.

    Args:
        settings (SettingsDB): Settings object.

    Returns:
        dict: The proxy dict.
    """
    if settings is None or settings.proxy is None or len(settings.proxy.server) == 0:
        # check is required for initial CE boot, when proxy is set in DB from environment.
        if not os.environ.get("CORE_HTTP_PROXY", "") or not os.environ.get("CORE_HTTPS_PROXY", ""):
            os.environ.pop('HTTP_PROXY', None)
            os.environ.pop('http_proxy', None)
            os.environ.pop('HTTPS_PROXY', None)
            os.environ.pop('https_proxy', None)
        return {"http": "", "https": ""}

    proxy = {}
    if len(settings.proxy.username) > 0 and len(settings.proxy.password) > 0:
        proxy["http"] = (
            f"{settings.proxy.scheme.value}://{urllib.parse.quote_plus(settings.proxy.username)}:"
            f"{urllib.parse.quote_plus(settings.proxy.password)}@{settings.proxy.server}"
        )
        proxy["https"] = (
            f"{settings.proxy.scheme.value}://{urllib.parse.quote_plus(settings.proxy.username)}:"
            f"{urllib.parse.quote_plus(settings.proxy.password)}@{settings.proxy.server}"
        )
    else:
        proxy["http"] = f"{settings.proxy.scheme.value}://{settings.proxy.server}"
        proxy["https"] = f"{settings.proxy.scheme.value}://{settings.proxy.server}"

    if not proxy.get("http"):
        os.environ.pop("CORE_HTTP_PROXY", None)
        os.environ.pop("HTTP_PROXY", None)
        os.environ.pop("http_proxy", None)
    else:
        os.environ["CORE_HTTP_PROXY"] = proxy["http"]
        os.environ["HTTP_PROXY"] = proxy["http"]
        os.environ["http_proxy"] = proxy["http"]

    if not proxy.get("https"):
        os.environ.pop("CORE_HTTPS_PROXY", None)
        os.environ.pop("HTTPS_PROXY", None)
        os.environ.pop("https_proxy", None)
    else:
        os.environ["CORE_HTTPS_PROXY"] = proxy["https"]
        os.environ["HTTPS_PROXY"] = proxy["https"]
        os.environ["https_proxy"] = proxy["https"]

    return proxy
