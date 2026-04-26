"""Secrets manager related utils."""
import hvac
import requests
import traceback
from urllib.parse import unquote

from netskope.common.utils.db_connector import DBConnector, Collections
from netskope.common.utils.singleton import Singleton
from netskope.common.utils.logger import Logger
from netskope.common.utils.proxy import get_proxy_params
from netskope.common.models.settings import (
    SettingsDB,
    SecretsManagerSettings,
    SecretsManagerHashicorpParams,
    HashicorpAuthMethod,
    SECRET_PREFIX,
    PLAINTEXT_PREFIX,
)


class SecretsManagerDisabledException(Exception):
    """Disabled secrets manager exception."""

    pass


class CouldNotResolve(Exception):
    """Could not resolve secrets manager exception."""

    pass


class SecretManager(metaclass=Singleton):
    """Secret manager class."""

    settings = None
    client = None

    @staticmethod
    def load_settings() -> SettingsDB:
        """Load settings from database."""
        return SettingsDB(**connector.collection(Collections.SETTINGS).find_one({}))

    def initialize(self, settings: SecretsManagerSettings, proxy: dict = {}) -> None:
        """Initialize."""
        self.settings = settings
        if not settings.enabled:
            return self
        if settings.params.provider == "hashicorp":
            self.client = self._build_hashicorp_client(settings.params, proxy=proxy)
        return self

    def _build_hashicorp_client(
        self, settings: SecretsManagerHashicorpParams, proxy: dict = {}
    ) -> hvac.Client:
        """Build the hashicorp client.

        Args:
            settings (SecretsManagerHashicorpParams): Parameters.
            proxy (dict, defaults to {}): Proxy to use.

        Returns:
            hvac.Client: Initialized client.
        """
        client = hvac.Client(
            url=settings.clusterURL, namespace=settings.namespace, proxies=proxy
        )
        if settings.token:
            client.token = settings.token
        if client.is_authenticated() or (
            settings.authMethod == HashicorpAuthMethod.TOKEN
        ):
            return client
        elif settings.authMethod != HashicorpAuthMethod.TOKEN:
            logger.debug("Vault token does not exist. Generating a new token.")
            try:
                client.token = None
                if settings.authMethod == HashicorpAuthMethod.USERNAME_PASSWORD:
                    client.auth.userpass.login(
                        username=settings.username,
                        password=settings.password,
                        mount_point=settings.path,
                    )
                elif settings.authMethod == HashicorpAuthMethod.APPROLE:
                    client.auth.approle.login(
                        role_id=settings.roleId,
                        secret_id=settings.secretId,
                        mount_point=settings.path,
                    )
                else:
                    raise NotImplementedError("Unsupported auth method.")
                if client.is_authenticated():
                    # store the generated token
                    connector.collection(Collections.SETTINGS).update_one(
                        {},
                        {"$set": {"secretsManagerSettings.params.token": client.token}},
                    )
            except Exception as ex:
                logger.error(
                    f"Error occurred while generating Vault token. {repr(ex)}",
                    details=traceback.format_exc(),
                )
            return client

    def resolve(self, path: str) -> str:
        """Resolve a secret.

        Args:
            path (str): Path of the secret to resolve.

        Raises:
            SecretsManagerDisabledException: Raised if the secrets manager is disabled.
            CouldNotResolve: Raised if the secret can not be resolved.
            NotImplementedError: Raised if the provider is unsupported.

        Returns:
            str: Resolved secret value.
        """
        if not self.settings.enabled:
            settings = SecretManager.load_settings()
            self.initialize(
                settings.secretsManagerSettings, proxy=get_proxy_params(settings)
            )
            if not self.settings.enabled:
                raise SecretsManagerDisabledException()
        if self.settings.params.provider == "hashicorp":
            try:
                path_split, key = path.split(":", 1)
                path_split, key = unquote(path_split), unquote(key)
                secret = self.client.read(path_split)
                if secret is None:
                    raise CouldNotResolve()
                secret = secret.get("data", {}).get("data", {}).get(key)
                if secret is None:
                    raise CouldNotResolve()
                return secret
            except ValueError:
                logger.error(
                    f"Could not resolve HashiCorp path {path}. Invalid format."
                )
            except (
                hvac.exceptions.Forbidden,
                hvac.exceptions.InvalidPath,
                hvac.exceptions.Unauthorized,
                hvac.exceptions.RateLimitExceeded,
                hvac.exceptions.ParamValidationError,
                hvac.exceptions.InvalidRequest,
                SecretsManagerDisabledException,
                CouldNotResolve,
            ):
                settings = SecretManager.load_settings()
                self.initialize(
                    settings.secretsManagerSettings, proxy=get_proxy_params(settings)
                )
                try:
                    secret = self.client.read(path_split)
                    if secret is None:
                        raise CouldNotResolve()
                    secret = secret.get("data", {}).get("data", {}).get(key)
                    if secret is None:
                        raise CouldNotResolve()
                    return secret
                except (
                    hvac.exceptions.Forbidden,
                    hvac.exceptions.Unauthorized,
                    hvac.exceptions.InvalidRequest,
                ):
                    logger.error(
                        (
                            "Error occured while authenticating with Vault. Update "
                            "parameters from Settings > General > Secrets Manager."
                        ),
                        details=traceback.format_exc(),
                    )
                except (hvac.exceptions.InvalidPath, CouldNotResolve):
                    logger.error(
                        (
                            "Error occured while authenticating with Vault. Make sure that "
                            "the secret path exists or prefix is correct from "
                            "Settings > General > Secrets Manager."
                        ),
                        details=traceback.format_exc(),
                    )
                except SecretsManagerDisabledException:
                    logger.error(
                        (
                            "Error occured while authenticating with Vault. Secrets "
                            "manager is disabled. Enable from Settings > General > Secrets Manager."
                        ),
                        details=traceback.format_exc(),
                    )
                except Exception as ex:
                    logger.error(
                        (
                            f"Error occured while resolving secret from Vault. {repr(ex)}"
                        ),
                        details=traceback.format_exc(),
                    )
        else:
            raise NotImplementedError("Unsupported provider.")
        return path


connector = DBConnector()
logger = Logger()


def init_manager() -> SecretManager:
    """Initialize and return the secrets manager.

    Returns:
        SecretManager: Initialized secrets manager.
    """
    try:
        settings = SecretManager.load_settings()
        return SecretManager().initialize(
            settings.secretsManagerSettings, proxy=get_proxy_params(settings)
        )
    except requests.exceptions.ProxyError:
        logger.error("Could not initialize the secrets manager. Proxy error occurred.")
    except (ValueError, TypeError):
        return None


def resolve_secret(val: str) -> str:
    """Resolve a string that might possibly be a secret path.

    Args:
        val (str): String to resolve.

    Returns:
        str: Resolved string.
    """
    if not isinstance(val, str):
        return val
    if val.startswith(SECRET_PREFIX):
        try:
            val = val[len(SECRET_PREFIX) :]  # noqa
            manager = init_manager()
            return manager.resolve(val)
        except AttributeError:
            manager = init_manager()
            return manager.resolve(val) if manager else val
    if val.startswith(PLAINTEXT_PREFIX):
        return val[len(PLAINTEXT_PREFIX) :]  # noqa
    return val


class SecretDict(dict):
    """Dict that automatically resolves secrets."""

    def _resolve_and_return(self, val):
        """Resolve the given value if secret.

        Args:
            val (Any): Value to be resolved.

        Returns:
            Any: Resolved value.
        """
        if isinstance(val, dict):
            return SecretDict(val)
        return resolve_secret(val)

    def __getitem__(self, k):
        """Get item from dict and resolve if it's a secret."""
        return self._resolve_and_return(super(SecretDict, self).__getitem__(k))

    def get(self, k, *args, **kwargs):
        """Get item from dict and resolve if it's a secret."""
        return self._resolve_and_return(super(SecretDict, self).get(k, *args, **kwargs))
