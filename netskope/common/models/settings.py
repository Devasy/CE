"""Provides settings related models."""

from typing import Dict, List, Optional, Literal, Union
from datetime import datetime, timedelta, UTC
from requests import Response
from functools import wraps
from pydantic import (
    field_validator,
    StringConstraints,
    BaseModel,
    Field,
    AnyHttpUrl,
    model_validator,
)
from enum import Enum
import requests
import traceback
import json
import hvac
from jsonschema import validate, ValidationError

from ..models import LogType
from ..utils.db_connector import DBConnector, Collections

from .other import PollIntervalUnit
from ..api import __version__
from typing_extensions import Annotated

connector = DBConnector()

SECRET_PREFIX = "secret:"
PLAINTEXT_PREFIX = "plain:"


def handle_vault_exceptions(fn):
    """Handle exceptions."""

    @wraps(fn)
    def decorated(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except ValueError:
            raise
        except requests.exceptions.ConnectionError:
            raise ValueError(
                "Timeout error occurred while validating credentials. Check Vault URL."
            )
        except hvac.exceptions.Forbidden:
            raise ValueError("Invalid credentials provided. Forbidden error occurred.")
        except hvac.exceptions.InvalidRequest:
            raise ValueError(
                "Invalid credentials provided. Invalid request error occurred."
            )
        except hvac.exceptions.InvalidPath:
            raise ValueError("Invalid path provided.")
        except Exception:
            from netskope.common.utils.logger import Logger

            logger = Logger()
            logger.error(
                "Error occurred while validating vault params.",
                details=traceback.format_exc(),
            )
            raise ValueError("Error occurred while validating params. Check logs.")

    return decorated


class ProxySchemes(str, Enum):
    """Enumeration for supported proxy schemes."""

    HTTP = "http"
    HTTPS = "https"


class ProxyIn(BaseModel):
    """The incoming proxy model."""

    scheme: ProxySchemes = Field(...)
    server: str = Field(...)
    username: str = Field(...)
    password: str = Field(...)


class ProxyOut(BaseModel):
    """The outgoing proxy model."""

    scheme: ProxySchemes
    server: str
    username: str


class Ssosaml(BaseModel):
    """The incoming SSOSAML model."""

    idpEntityId: Union[
        Annotated[str, StringConstraints(strip_whitespace=True)], None
    ] = Field(None)
    idpSsoUrl: Union[Annotated[str, StringConstraints(strip_whitespace=True)], None] = (
        Field(None)
    )
    idpSloUrl: Union[Annotated[str, StringConstraints(strip_whitespace=True)], None] = (
        Field(None)
    )
    idpX509Cert: Union[str, None] = Field(None)
    spEntityId: Union[
        Annotated[str, StringConstraints(strip_whitespace=True)], None
    ] = Field(None)
    spAcsUrl: Union[Annotated[str, StringConstraints(strip_whitespace=True)], None] = (
        Field(None)
    )
    spSlsUrl: Union[Annotated[str, StringConstraints(strip_whitespace=True)], None] = (
        Field(None)
    )


class ScoreMappings(BaseModel):
    """Score mappings for CRE."""

    uptoCritical: int = Field(250)
    uptoHigh: int = Field(500)
    uptoMedium: int = Field(750)


class CREWeekDay(int, Enum):
    """CRE week day model."""

    MONDAY = 0
    TUESDAY = 1
    WEDNESDAY = 2
    THURSDAY = 3
    FRIDAY = 4
    SATURDAY = 5
    SUNDAY = 6


class CRESettings(BaseModel):
    """CRE settings model."""

    logsCleanup: int = Field(7)
    updateTaskLockedAt: Union[datetime, None] = Field(None)
    normalizedScoreMappings: ScoreMappings = Field(ScoreMappings())
    generateAlerts: bool = Field(True)
    maxDataPointsInDays: int = Field(30, gt=0, lt=366)
    normalizedScoreHistory: List[Dict] = Field([])
    flapSuppression: int = Field(5, gt=0, lt=1441)
    syncInterval: int = Field(12)
    syncIntervalUnit: PollIntervalUnit = Field(PollIntervalUnit.HOURS)
    startTime: datetime = Field(
        (datetime.now(UTC) + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    )
    endTime: datetime = Field(
        (datetime.now(UTC) + timedelta(days=1)).replace(
            hour=23, minute=59, second=59, microsecond=0
        )
    )
    maintenanceDays: list[CREWeekDay] = Field(
        [
            CREWeekDay.MONDAY.value,
            CREWeekDay.TUESDAY.value,
            CREWeekDay.WEDNESDAY.value,
            CREWeekDay.THURSDAY.value,
            CREWeekDay.FRIDAY.value,
            CREWeekDay.SATURDAY.value,
            CREWeekDay.SUNDAY.value,
        ]
    )

    @field_validator("maintenanceDays")
    @classmethod
    def _validate_maintenance_days(cls, v):
        if not v:
            raise ValueError("At least one maintenance day must be selected.")
        return v

    purgeRecords: bool = Field(False)
    purgeDays: int = Field(7)


class CLSRetryStrategy(str, Enum):
    """Retry strategy enum."""

    INFINITE = "infinite"
    LIMITED = "limited"


class CLSSettings(BaseModel):
    """CLS settings model."""

    retryStrategy: CLSRetryStrategy = Field(CLSRetryStrategy.LIMITED)
    utf8Encoding: bool = Field(False)


class CTECriterias(str, Enum):
    """Enumeration for supported proxy schemes."""

    FIRST_SEEN = "firstSeen"
    LAST_SEEN = "lastSeen"
    HIGHEST_SEVERITY = "highestSeverity"


class CTESettings(BaseModel):
    """CTE settings model."""

    criteria: CTECriterias = Field(CTECriterias.LAST_SEEN)
    iocRetraction: bool = Field(False)
    iocRetractionInterval: int = Field(1)
    deleteInactiveIndicators: bool = Field(False)


class EDMSettings(BaseModel):
    """EDM settings model."""

    edmFilesCleanup: int = Field(
        1, description="Number of days to keep the EDM hash files", ge=1, le=365
    )


class CFCSettings(BaseModel):
    """CFC settings model."""

    cfcImageMetadataCleanup: int = Field(
        7, description="Number of days to retain CFC image metadata", ge=1, le=365
    )


class HashicorpAuthMethod(str, Enum):
    """Hashicorp supported auth methods."""

    USERNAME_PASSWORD = "username/password"
    TOKEN = "token"
    APPROLE = "approle"


class SecretsManagerHashicorpParams(BaseModel):
    """Hashicorp related params."""

    provider: Literal["hashicorp"] = "hashicorp"
    clusterURL: str = Field(...)

    @field_validator("clusterURL", mode="before")
    def validate_cluster_url(cls, v):
        """Validate plugin repository url."""
        try:
            _ = AnyHttpUrl(v)
        except Exception:
            raise ValueError("Error: invalid or missing URL scheme")
        return v

    namespace: str = Field("")

    authMethod: HashicorpAuthMethod = Field(...)
    token: str = Field("")

    @field_validator("token")
    @classmethod
    def _validate_token(cls, val, values, **kwargs):
        """Validate token."""
        values = values.data
        if "clusterURL" not in values:
            raise ValueError("clusterURL is required.")
        if "authMethod" not in values:
            raise ValueError("authMethod is required.")
        if values["authMethod"] == HashicorpAuthMethod.TOKEN and not val:
            raise ValueError("token is required.")
        elif values["authMethod"] != HashicorpAuthMethod.TOKEN:
            return ""

        @handle_vault_exceptions
        def validate():
            from ..utils.proxy import get_proxy_params

            proxy = get_proxy_params(
                SettingsDB(**connector.collection(Collections.SETTINGS).find_one({}))
            )
            client = hvac.Client(
                url=values["clusterURL"], namespace=values["namespace"], proxies=proxy
            )
            client.token = val
            dummy_token = client.read("kv/data/dummy")
            if not client.is_authenticated() or isinstance(dummy_token, Response):
                raise ValueError("Incorrect token/namespace/vault url provided.")
            return val

        return validate()

    path: Union[Annotated[str, Field(validate_default=True)], None] = Field(None)

    @field_validator("path")
    @classmethod
    def _validate_path(cls, val, values, **kwargs):
        """Validate path/mount point."""
        values = values.data
        if (
            values.get("authMethod")
            in [HashicorpAuthMethod.USERNAME_PASSWORD, HashicorpAuthMethod.APPROLE]
            and not val
        ):
            raise ValueError("path is required.")
        return val

    username: Annotated[str, Field(validate_default=True)] = Field("")

    @field_validator("username")
    @classmethod
    def _validate_username(cls, val, values, **kwargs):
        """Validate username."""
        values = values.data
        if "authMethod" not in values:
            raise ValueError("authMethod is required.")
        if values["authMethod"] == HashicorpAuthMethod.USERNAME_PASSWORD and not val:
            raise ValueError("username is required.")
        elif values["authMethod"] != HashicorpAuthMethod.USERNAME_PASSWORD:
            return ""
        return val

    password: str = Field("")

    @field_validator("password")
    @classmethod
    def _validate_password(cls, val, values, **kwargs):
        """Validate username."""
        values = values.data
        if "clusterURL" not in values:
            raise ValueError("clusterURL is required.")
        if "authMethod" not in values:
            raise ValueError("authMethod is required.")
        if values["authMethod"] == HashicorpAuthMethod.USERNAME_PASSWORD and not val:
            raise ValueError("password is required.")
        elif values["authMethod"] != HashicorpAuthMethod.USERNAME_PASSWORD:
            return ""

        @handle_vault_exceptions
        def validate():
            from ..utils.proxy import get_proxy_params

            proxy = get_proxy_params(
                SettingsDB(**connector.collection(Collections.SETTINGS).find_one({}))
            )
            client = hvac.Client(
                url=values["clusterURL"], namespace=values["namespace"], proxies=proxy
            )
            client.token = None
            client.auth.userpass.login(
                username=values["username"], password=val, mount_point=values["path"]
            )
            if not client.is_authenticated():
                raise ValueError("Incorrect username/password provided.")
            return val

        return validate()

    roleId: str = Field("")

    def _validate_role_id(cls, val, values, **kwargs):
        """Validate username."""
        if "authMethod" not in values:
            raise ValueError("authMethod is required.")
        if values["authMethod"] == HashicorpAuthMethod.APPROLE and not val:
            raise ValueError("roleId is required.")
        elif values["authMethod"] != HashicorpAuthMethod.APPROLE:
            return ""
        return val

    secretId: Annotated[str, Field(validate_default=True)] = Field("")

    @field_validator("secretId")
    @classmethod
    def _validate_secret_id(cls, val, values, **kwargs):
        """Validate role."""
        values = values.data
        if "clusterURL" not in values:
            raise ValueError("clusterURL is required.")
        if "authMethod" not in values:
            raise ValueError("authMethod is required.")
        if values["authMethod"] == HashicorpAuthMethod.APPROLE and not val:
            raise ValueError("secretId is required.")
        elif values["authMethod"] != HashicorpAuthMethod.APPROLE:
            return ""

        @handle_vault_exceptions
        def validate():
            from ..utils.proxy import get_proxy_params

            proxy = get_proxy_params(
                SettingsDB(**connector.collection(Collections.SETTINGS).find_one({}))
            )
            client = hvac.Client(
                url=values["clusterURL"], namespace=values["namespace"], proxies=proxy
            )
            client.token = None

            client.auth.approle.login(
                role_id=values["roleId"], secret_id=val, mount_point=values["path"]
            )
            if not client.is_authenticated():
                raise ValueError("Could not authenticate using the role.")
            return val

        return validate()

    @model_validator(mode="before")
    def clear_fields(cls, values):
        """Clear unused fields."""
        auth_method = values.get("authMethod")
        if auth_method is None:
            return values
        if auth_method == HashicorpAuthMethod.TOKEN:
            values["username"] = ""
            values["password"] = ""
            values["roleId"] = ""
            values["secretId"] = ""
        elif auth_method == HashicorpAuthMethod.APPROLE:
            values["username"] = ""
            values["password"] = ""
            values["token"] = ""
        elif auth_method == HashicorpAuthMethod.USERNAME_PASSWORD:
            values["roleId"] = ""
            values["secretId"] = ""
            values["token"] = ""
        return values


class SecretsManagerHashicorpParamsDB(BaseModel):
    """Hashicorp related params."""

    provider: Literal["hashicorp"] = "hashicorp"
    clusterURL: str = Field(...)

    @field_validator("clusterURL")
    def validate_cluster_url(cls, v):
        """Validate plugin repository url."""
        try:
            _ = AnyHttpUrl(v)
        except Exception:
            raise ValueError("Error: invalid or missing URL scheme")
        return v

    namespace: str = Field("")
    authMethod: HashicorpAuthMethod = Field(...)
    token: Union[str, None] = Field(None)
    path: Union[str, None] = Field(None)
    username: Union[str, None] = Field(None)
    password: Union[str, None] = Field(None)
    roleId: Union[str, None] = Field(None)
    secretId: Union[str, None] = Field(None)


class SecretsManagerHashicorpParamsOut(BaseModel):
    """Hashicorp related params."""

    provider: Literal["hashicorp"] = "hashicorp"
    clusterURL: str = Field(...)

    @field_validator("clusterURL")
    def validate_cluster_url(cls, v):
        """Validate plugin repository url."""
        try:
            _ = AnyHttpUrl(v)
        except Exception:
            raise ValueError("Error: invalid or missing URL scheme")
        return v

    namespace: str = Field("")
    authMethod: HashicorpAuthMethod = Field(...)
    path: Union[str, None] = Field(None)
    username: Union[str, None] = Field(None)
    roleId: Union[str, None] = Field(None)


def handle_azure_exceptions(fn):
    """Handle exceptions for Azure Key Vault operations."""
    # Lazy import Azure exceptions to handle case when SDK is not installed
    try:
        from azure.core.exceptions import (
            ClientAuthenticationError,
            HttpResponseError,
            ServiceRequestError,
        )
        AZURE_EXCEPTIONS_AVAILABLE = True
    except ImportError:
        AZURE_EXCEPTIONS_AVAILABLE = False
        ClientAuthenticationError = None
        HttpResponseError = None
        ServiceRequestError = None

    @wraps(fn)
    def decorated(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except ValueError:
            raise
        except requests.exceptions.ConnectionError:
            raise ValueError(
                "Connection error occurred while validating credentials. Check Vault URL and network connectivity."
            )
        except Exception as e:
            from netskope.common.utils.logger import Logger

            logger = Logger()
            error_msg = str(e)
            logger.error(
                "Error occurred while validating Azure Key Vault params.",
                details=traceback.format_exc(),
            )

            # Handle Azure SDK specific exceptions
            if AZURE_EXCEPTIONS_AVAILABLE:
                if ClientAuthenticationError and isinstance(e, ClientAuthenticationError):
                    raise ValueError(
                        "Azure authentication failed. Verify Tenant ID, Client ID, and credentials are correct."
                    )
                if HttpResponseError and isinstance(e, HttpResponseError):
                    if e.status_code == 401:
                        raise ValueError(
                            "Unauthorized. Check Client ID and Client Secret/Certificate."
                        )
                    elif e.status_code == 403:
                        raise ValueError(
                            "Access denied. Ensure the app has 'Secret List' and "
                            "'Secret Get' permissions in Key Vault access policy."
                        )
                    elif e.status_code == 404:
                        raise ValueError(
                            "Key Vault not found. Verify the Vault URL is correct."
                        )
                    raise ValueError(f"Azure Key Vault error ({e.status_code}): {e.message}")
                if ServiceRequestError and isinstance(e, ServiceRequestError):
                    raise ValueError(
                        "Failed to connect to Azure Key Vault. Check Vault URL and network connectivity."
                    )

            # Fallback for string-based error detection (in case exceptions aren't caught above)
            if "AADSTS" in error_msg:
                raise ValueError("Azure AD authentication failed. Check credentials.")
            raise ValueError("Error occurred while validating params. Check logs for details.")

    return decorated


class AzureAuthMethod(str, Enum):
    """Azure Key Vault supported auth methods."""

    CLIENT_SECRET = "client_secret"
    CERTIFICATE = "certificate"


class SecretsManagerAzureParams(BaseModel):
    """Azure Key Vault related params."""

    provider: Literal["azure"] = "azure"
    vaultUrl: str = Field(...)

    @field_validator("vaultUrl", mode="before")
    def validate_vault_url(cls, v):
        """Validate Azure Key Vault URL."""
        try:
            AnyHttpUrl(v)
        except Exception:
            raise ValueError("Vault URL must be a valid URL starting with http:// or https://")
        return v

    tenantId: str = Field(...)
    clientId: str = Field(...)
    authMethod: AzureAuthMethod = Field(...)

    # Client Secret auth fields
    clientSecret: Annotated[str, Field(validate_default=True)] = Field("")

    @field_validator("clientSecret")
    @classmethod
    def _validate_client_secret(cls, val, values, **kwargs):
        """Validate client secret and authenticate."""
        values = values.data
        if "vaultUrl" not in values:
            raise ValueError("Vault URL is required.")
        if "tenantId" not in values:
            raise ValueError("Tenant ID is required.")
        if "clientId" not in values:
            raise ValueError("Client ID is required.")
        if "authMethod" not in values:
            raise ValueError("Authentication Method is required.")
        if values.get("authMethod") == AzureAuthMethod.CLIENT_SECRET and not val:
            raise ValueError("Client Secret is required for Client Secret authentication.")
        elif values.get("authMethod") == AzureAuthMethod.CLIENT_SECRET:

            @handle_azure_exceptions
            def validate_auth():
                from ..utils.proxy import get_proxy_params
                from netskope.common.utils.secrets_manager import (
                    _build_azure_client_for_validation,
                )

                proxy = get_proxy_params(
                    SettingsDB(**connector.collection(Collections.SETTINGS).find_one({}))
                )
                _build_azure_client_for_validation(
                    vault_url=values["vaultUrl"],
                    tenant_id=values["tenantId"],
                    client_id=values["clientId"],
                    client_secret=val,
                    certificate=None,
                    certificate_password=None,
                    proxy=proxy,
                )
                return val

            return validate_auth()
        return ""

    # Certificate auth fields - certificatePassword must be defined BEFORE certificate
    # so it's available in values.data during certificate validation
    certificatePassword: Union[str, None] = Field(None)

    certificate: Annotated[str, Field(validate_default=True)] = Field("")

    @field_validator("certificate")
    @classmethod
    def _validate_certificate(cls, val, values, **kwargs):
        """Validate certificate and authenticate."""
        values = values.data
        if "vaultUrl" not in values:
            raise ValueError("Vault URL is required.")
        if "tenantId" not in values:
            raise ValueError("Tenant ID is required.")
        if "clientId" not in values:
            raise ValueError("Client ID is required.")
        if "authMethod" not in values:
            raise ValueError("Authentication Method is required.")
        if values.get("authMethod") == AzureAuthMethod.CERTIFICATE and not val:
            raise ValueError("Certificate is required for Certificate authentication.")
        elif values.get("authMethod") == AzureAuthMethod.CERTIFICATE:
            @handle_azure_exceptions
            def validate_auth():
                from ..utils.proxy import get_proxy_params
                from netskope.common.utils.secrets_manager import (
                    _build_azure_client_for_validation,
                )

                proxy = get_proxy_params(
                    SettingsDB(**connector.collection(Collections.SETTINGS).find_one({}))
                )
                _build_azure_client_for_validation(
                    vault_url=values["vaultUrl"],
                    tenant_id=values["tenantId"],
                    client_id=values["clientId"],
                    client_secret=None,
                    certificate=val,
                    certificate_password=values.get("certificatePassword"),
                    proxy=proxy,
                )
                return val

            return validate_auth()
        return ""

    @model_validator(mode="before")
    def clear_fields(cls, values):
        """Clear unused fields based on auth method."""
        auth_method = values.get("authMethod")
        if auth_method is None:
            return values
        if auth_method == AzureAuthMethod.CLIENT_SECRET:
            values["certificate"] = ""
            values["certificatePassword"] = None
        elif auth_method == AzureAuthMethod.CERTIFICATE:
            values["clientSecret"] = ""
        return values


class SecretsManagerAzureParamsDB(BaseModel):
    """Azure Key Vault params for DB storage."""

    provider: Literal["azure"] = "azure"
    vaultUrl: str = Field(...)

    @field_validator("vaultUrl")
    def validate_vault_url(cls, v):
        """Validate Azure Key Vault URL."""
        try:
            _ = AnyHttpUrl(v)
        except Exception:
            raise ValueError("Error: invalid or missing URL scheme for Vault URL")
        return v

    tenantId: str = Field(...)
    clientId: str = Field(...)
    authMethod: AzureAuthMethod = Field(...)
    clientSecret: Union[str, None] = Field(None)
    certificatePassword: Union[str, None] = Field(None)
    certificate: Union[str, None] = Field(None)


class SecretsManagerAzureParamsOut(BaseModel):
    """Azure Key Vault params for API output (hides secrets)."""

    provider: Literal["azure"] = "azure"
    vaultUrl: str = Field(...)

    @field_validator("vaultUrl")
    def validate_vault_url(cls, v):
        """Validate Azure Key Vault URL."""
        try:
            _ = AnyHttpUrl(v)
        except Exception:
            raise ValueError("Error: invalid or missing URL scheme for Vault URL")
        return v

    tenantId: str = Field(...)
    clientId: str = Field(...)
    authMethod: AzureAuthMethod = Field(...)
    # Note: clientSecret and certificate are intentionally excluded


def is_vault_used(value) -> bool:
    """Check if vault is used."""
    if isinstance(value, str) and value.startswith(SECRET_PREFIX):
        return True
    elif isinstance(value, dict):
        return any(is_vault_used(v) for v in value.values())
    else:
        return False


class SecretsManagerSettings(BaseModel):
    """Secrets manager related settings."""

    enabled: Annotated[bool, Field(validate_default=True)] = Field(False)

    @field_validator("enabled")
    @classmethod
    def validate_disabled(cls, val):
        """Validate disabled secret manager."""
        if val:
            return val
        connector = DBConnector()
        for repo in connector.collection(Collections.PLUGIN_REPOS).find({}):
            if is_vault_used(repo.get("password")):
                raise ValueError("Secrets manager can not be disabled while in use.")
        for tenant in connector.collection(Collections.NETSKOPE_TENANTS).find({}):
            tenant_parameters = tenant.get("parameters", {})
            if is_vault_used(tenant_parameters.get("v2token")) or is_vault_used(
                tenant_parameters.get("token")
            ):
                raise ValueError("Secrets manager can not be disabled while in use.")
        config_collections = (
            Collections.CONFIGURATIONS,
            Collections.CRE_CONFIGURATIONS,
            Collections.CREV2_CONFIGURATIONS,
            Collections.ITSM_CONFIGURATIONS,
            Collections.CLS_CONFIGURATIONS,
            Collections.GRC_CONFIGURATIONS,
            Collections.EDM_CONFIGURATIONS,
            Collections.CFC_CONFIGURATIONS,
        )
        for collection in config_collections:
            for config in connector.collection(collection).find({}):
                for _, value in config.get("parameters", {}).items():
                    if is_vault_used(value):
                        raise ValueError(
                            "Secrets manager can not be disabled while in use."
                        )
        return val

    params: Union[
        Annotated[SecretsManagerHashicorpParams, Field(validate_default=True)],
        Annotated[SecretsManagerAzureParams, Field(validate_default=True)],
        None,
    ] = Field(None, discriminator="provider")

    @field_validator("params")
    @classmethod
    def validate_params(cls, v, values, **kwargs):
        """Validate params."""
        values = values.data
        if values.get("enabled", False) and not v:
            raise ValueError("params can not be empty if secrets manager is enabled.")
        elif not values.get("enabled"):
            return None
        return v

    @model_validator(mode="before")
    def validate_provider_switch(cls, values):
        """Validate that provider cannot be switched while secrets are in use."""
        if not values.get("enabled"):
            return values
        new_provider = values.get("params", {}).get("provider") if values.get("params") else None
        if not new_provider:
            return values

        # Get current settings from DB
        connector = DBConnector()
        current_settings = connector.collection(Collections.SETTINGS).find_one({})
        if not current_settings:
            return values

        current_sm_settings = current_settings.get("secretsManagerSettings", {})
        if not current_sm_settings.get("enabled"):
            return values

        current_provider = current_sm_settings.get("params", {}).get("provider")
        if not current_provider or current_provider == new_provider:
            return values

        # Provider is being changed - check if secrets are in use
        def check_secrets_in_use():
            for repo in connector.collection(Collections.PLUGIN_REPOS).find({}):
                if is_vault_used(repo.get("password")):
                    return True
            for tenant in connector.collection(Collections.NETSKOPE_TENANTS).find({}):
                tenant_parameters = tenant.get("parameters", {})
                if is_vault_used(tenant_parameters.get("v2token")) or is_vault_used(
                    tenant_parameters.get("token")
                ):
                    return True
            config_collections = (
                Collections.CONFIGURATIONS,
                Collections.CRE_CONFIGURATIONS,
                Collections.CREV2_CONFIGURATIONS,
                Collections.ITSM_CONFIGURATIONS,
                Collections.CLS_CONFIGURATIONS,
                Collections.GRC_CONFIGURATIONS,
                Collections.EDM_CONFIGURATIONS,
                Collections.CFC_CONFIGURATIONS,
            )
            for collection in config_collections:
                for config in connector.collection(collection).find({}):
                    for _, value in config.get("parameters", {}).items():
                        if is_vault_used(value):
                            return True
            return False

        if check_secrets_in_use():
            raise ValueError(
                f"Cannot switch secrets manager provider from '{current_provider}' to '{new_provider}' "
                "while secrets are in use. Remove all secret references first."
            )

        return values


class SecretsManagerSettingsDB(BaseModel):
    """Secrets manager related settings."""

    enabled: bool = Field(False)
    params: Union[
        SecretsManagerHashicorpParamsDB, SecretsManagerAzureParamsDB, None
    ] = Field(None, discriminator="provider")


class SecretsManagerSettingsOut(BaseModel):
    """Secrets manager related settings."""

    enabled: bool = Field(False)
    params: Union[
        SecretsManagerHashicorpParamsOut, SecretsManagerAzureParamsOut, None
    ] = Field(None, discriminator="provider")


class PasswordPolicySettings(BaseModel):
    """Password policy settings model."""

    minLength: int = Field(8, ge=1, le=128)
    maxLength: int = Field(72, ge=8, le=128)
    requireUppercase: bool = Field(True)
    requireLowercase: bool = Field(True)
    requireDigits: bool = Field(True)
    requireSpecialChars: bool = Field(True)
    updated_at: Union[datetime, None] = Field(None)
    updated_by: Union[str, None] = Field(None)

    @field_validator("minLength", mode="before")
    @classmethod
    def validate_min_length(cls, v):
        """Validate minLength with specific error messages."""
        if v is None:
            return v
        if not isinstance(v, int):
            raise ValueError("Minimum length must be an integer")
        if v < 1:
            raise ValueError("Minimum length must be greater than or equal to 1.")
        if v > 128:
            raise ValueError("Minimum length must be less than or equal to 128.")
        return v

    @field_validator("maxLength", mode="before")
    @classmethod
    def validate_max_length(cls, v, info):
        """Validate maxLength with specific error messages and cross-field validation."""
        if v is None:
            return v
        if not isinstance(v, int):
            raise ValueError("Maximum length must be an integer")
        if v < 8:
            raise ValueError("Maximum length must be greater than or equal to 8.")
        if v > 128:
            raise ValueError("Maximum length must be less than or equal to 128.")

        # Cross-field validation with minLength
        if info.data and "minLength" in info.data:
            min_length = info.data["minLength"]
            if isinstance(min_length, int) and v < min_length:
                raise ValueError("Maximum length must be greater than or equal to minimum length.")

        return v


class CertExpiry(BaseModel):
    """Certificate expiry information."""

    ui: Union[datetime, None] = Field(None)
    mongodb_rabbitmq: Union[datetime, None] = Field(None)


class SettingsOut(BaseModel):
    """The outgoing settings model."""

    proxy: Union[ProxyOut, None] = Field(None)
    ssoEnable: bool = Field(False)
    forceAuth: bool = Field(False)
    ssosaml: Union[Ssosaml, None] = Field(None)
    logLevel: LogType = Field(...)
    logsCleanup: int = Field(2)
    dataBatchCleanup: int = Field(7)
    version: str = Field(__version__)
    enableUpdateChecking: bool = Field(False)
    platforms: Dict[str, bool] = Field(...)
    databaseVersion: str
    alertCleanup: int
    eventCleanup: int = Field(7)
    ticketsCleanup: int = Field(7)
    ticketsCleanupMongo: Union[str, None] = Field(None)
    ticketsCleanupQuery: Union[str, None] = Field(None)
    notificationsCleanup: int = Field(7)
    notificationsCleanupUnit: Union[PollIntervalUnit, None] = Field(None)
    tasksCleanup: int = Field(24)
    cre: CRESettings = Field(CRESettings())
    cls: CLSSettings = Field(CLSSettings())
    cte: CTESettings = Field(CTESettings())
    edm: EDMSettings = Field(EDMSettings())
    cfc: CFCSettings = Field(CFCSettings())
    itsm: Dict = Field({})
    disk_alarm: bool = Field(False)
    columns: dict = Field(...)
    sslValidation: bool = Field(True)
    emailAddress: Union[Optional[str], None] = Field(None)
    uid: Union[str, None] = Field(None)
    secretsManagerSettings: SecretsManagerSettingsOut = Field(
        SecretsManagerSettingsOut()
    )
    analyticsServerConnectivity: bool = Field(True)
    pluginsUpdatedAt: Union[datetime, None] = Field(None)
    tourCompleted: Dict[str, bool] = Field(None)
    passwordPolicy: PasswordPolicySettings = Field(PasswordPolicySettings())
    certExpiry: Union[CertExpiry, None] = Field(None)


class BeatStatusSettings(BaseModel):
    """Beat status settings."""

    node_name: str = Field("")
    last_update_time: datetime = Field(datetime.now(UTC) - timedelta(seconds=60))


class SettingsDB(SettingsOut):
    """The database settings model."""

    columns: dict = Field({})
    enableUpdateChecking: bool = Field(False)
    proxy: Union[ProxyIn, None] = Field(None)
    secretsManagerSettings: SecretsManagerSettingsDB = Field(SecretsManagerSettingsDB())
    beat_status: BeatStatusSettings = Field(BeatStatusSettings())
    analytics: Union[Dict, str, None] = Field(None)
    analyticsSharedAt: Union[datetime, None] = Field(None)
    shareAnalytics: Union[bool, None] = Field(None)
    analyticsSharedCount: Union[int, None] = Field(0)
    migrationHistory: List[Dict] = Field([])


class SettingsIn(BaseModel):
    """The incoming settings model."""

    proxy: Union[ProxyIn, None] = Field(None)
    ssoEnable: Union[bool, None] = Field(None)
    ssosaml: Union[Ssosaml, None] = Field(None)
    forceAuth: Union[bool, None] = Field(None)
    logLevel: Union[LogType, None] = Field(None)
    logsCleanup: Union[int, None] = Field(None, lt=366, gt=0)
    dataBatchCleanup: Union[int, None] = Field(None, lt=366, gt=0)
    password: Union[str, None] = Field(None)
    enableUpdateChecking: Union[bool, None] = Field(None)
    platforms: Union[Dict[str, bool], None] = Field(None)
    alertCleanup: Union[int, None] = Field(None, lt=366, gt=0)
    eventCleanup: Union[int, None] = Field(None, lt=366, gt=0)
    ticketsCleanup: Union[int, None] = Field(None, lt=366, gt=0)
    ticketsCleanupMongo: Dict = Field(None)
    ticketsCleanupQuery: str = Field(None)
    notificationsCleanup: Union[int, None] = Field(None)
    tasksCleanup: Union[int, None] = Field(None, lt=168, gt=0)
    cre: Union[CRESettings, None] = Field(None)
    cls: Union[CLSSettings, None] = Field(None)
    cte: Union[CTESettings, None] = Field(None)
    edm: Union[EDMSettings, None] = Field(None)
    cfc: Union[CFCSettings, None] = Field(None)
    itsm: Union[Dict, None] = Field({})
    disk_alarm: bool = Field(False)
    notificationsCleanupUnit: Union[PollIntervalUnit, None] = Field(None)
    emailAddress: Union[Optional[str], None] = Field(None, max_length=100)
    uid: Union[str, None] = Field(None)
    tourCompleted: Union[Dict[str, bool], None] = Field(None)
    secretsManagerSettings: Union[SecretsManagerSettings, None] = Field(None)
    passwordPolicy: Union[PasswordPolicySettings, Literal["reset"], None] = Field(None)

    @field_validator("ticketsCleanupMongo")
    @classmethod
    def validate_alert_cleanup_mongo(cls, v):
        """Validate alert cleanup query."""
        try:
            from netskope.integrations.itsm.utils import task_query_schema
            _, _, alert_query_schema = task_query_schema()
            validate(v, alert_query_schema)
        except ValidationError as ex:
            raise ValueError(f"Invalid query provided. {ex.message}.")
        except Exception:
            raise ValueError("Could not parse the query.")
        return json.dumps(v)

    columns: Union[dict, None] = Field(None)

    @field_validator("platforms")
    @classmethod
    def _validate_platforms(cls, v):
        """Validate platform dictionary."""
        if (
            "cte" not in v
            or "itsm" not in v
            or "cls" not in v
            or "cre" not in v
            or "edm" not in v
            or "cfc" not in v
        ):
            raise ValueError(
                "Platforms must contain cte, itsm, cls, cre, edm, and cfc keys."
            )
        return v

    sslValidation: Union[bool, None] = Field(None)


class AccountSettingsIn(BaseModel):
    """The incoming account settings model."""

    oldPassword: str = Field(...)
    newPassword: str = Field(...)
    emailAddress: Union[Optional[str], None] = Field(None, max_length=100)
