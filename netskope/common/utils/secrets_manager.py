"""Secrets manager related utils."""
import base64
import hashlib
import hvac
import json
import requests
import traceback
from datetime import datetime, timedelta, timezone
from typing import Optional, TYPE_CHECKING
from urllib.parse import unquote

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

if TYPE_CHECKING:
    from netskope.common.models.settings import SecretsManagerAwsParams

from azure.identity import ClientSecretCredential, CertificateCredential
from azure.keyvault.secrets import SecretClient
from azure.core.exceptions import (
    ClientAuthenticationError,
    ResourceNotFoundError,
    HttpResponseError,
)
from azure.core.pipeline.transport import RequestsTransport

from netskope.common.utils.db_connector import DBConnector, Collections
from netskope.common.utils.singleton import Singleton
from netskope.common.utils.logger import Logger
from netskope.common.utils.proxy import get_proxy_params
from netskope.common.models.settings import (
    SettingsDB,
    SecretsManagerSettings,
    SecretsManagerHashicorpParams,
    HashicorpAuthMethod,
    SecretsManagerAzureParams,
    AzureAuthMethod,
    AwsAuthMethod,
    SECRET_PREFIX,
    PLAINTEXT_PREFIX,
)

AWS_CREDENTIAL_EXPIRY_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
AWS_CREDENTIAL_REFRESH_MINUTES = 3
AWS_NO_CREDENTIALS_MSG = (
    "No AWS Credentials were found in the environment. "
    "Deploy Cloud Exchange in an AWS environment or use AWS IAM Roles Anywhere "
    "authentication method."
)


class SecretsManagerDisabledException(Exception):
    """Disabled secrets manager exception."""

    pass


class CouldNotResolve(Exception):
    """Could not resolve secrets manager exception."""

    pass


class AwsSecretsManagerClient:
    """AWS Secrets Manager client wrapper (mirrors plugin credential behavior)."""

    def __init__(self, params, proxy: dict):
        """Initialize AWS Secrets Manager client."""
        self.params = params
        self.proxy = proxy or {}
        self._credentials_cache = None  # Roles Anywhere only

    def _load_private_key(self, pem: str, passphrase: Optional[str]):
        data = pem.encode("utf-8")
        try:
            return serialization.load_pem_private_key(data, None)
        except TypeError:
            if not passphrase:
                raise ValueError("Private key is encrypted; provide a passphrase.")
            return serialization.load_pem_private_key(
                data, passphrase.encode("utf-8")
            )

    def _create_roles_anywhere_session(self) -> dict:
        """Mint temporary credentials via IAM Roles Anywhere CreateSession."""
        private_key = self._load_private_key(
            self.params.privateKey.strip(),
            self.params.passPhrase,
        )
        cert_pem = self.params.publicCertificate.strip()
        cert = x509.load_pem_x509_certificate(cert_pem.encode("utf-8"))
        amz_x509 = base64.b64encode(
            cert.public_bytes(encoding=serialization.Encoding.DER)
        ).decode("utf-8")

        region = self.params.region.strip()
        body_obj = {
            "durationSeconds": 3600,
            "profileArn": self.params.profileArn.strip(),
            "roleArn": self.params.roleArn.strip(),
            "trustAnchorArn": self.params.trustAnchorArn.strip(),
        }
        body = json.dumps(body_obj, separators=(",", ":"))

        service = "rolesanywhere"
        host = f"rolesanywhere.{region}.amazonaws.com"
        endpoint = f"https://{host}/sessions"
        content_type = "application/json"

        amz_date = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        date_stamp = amz_date[:8]

        canonical_headers = (
            f"content-type:{content_type}\n"
            f"host:{host}\n"
            f"x-amz-date:{amz_date}\n"
            f"x-amz-x509:{amz_x509}\n"
        )
        signed_headers = "content-type;host;x-amz-date;x-amz-x509"
        payload_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
        canonical_request = (
            "POST\n/sessions\n\n"
            f"{canonical_headers}\n{signed_headers}\n{payload_hash}"
        )

        algorithm = "AWS4-X509-RSA-SHA256"
        credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
        string_to_sign = (
            f"{algorithm}\n{amz_date}\n{credential_scope}\n"
            f"{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"
        )

        signature_hex = private_key.sign(
            string_to_sign.encode("utf-8"),
            padding=padding.PKCS1v15(),
            algorithm=hashes.SHA256(),
        ).hex()

        authorization = (
            f"{algorithm} Credential={cert.serial_number}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature_hex}"
        )

        session = requests.Session()
        if self.proxy and (self.proxy.get("http") or self.proxy.get("https")):
            session.proxies = {
                "http": self.proxy.get("http", ""),
                "https": self.proxy.get("https", ""),
            }

        response = session.post(
            endpoint,
            data=body,
            headers={
                "Content-Type": content_type,
                "X-Amz-Date": amz_date,
                "X-Amz-X509": amz_x509,
                "Authorization": authorization,
                "User-Agent": "netskope-ce-secrets-manager/1.0",
            },
            timeout=60,
        )

        if response.status_code not in (200, 201):
            detail = response.text
            try:
                detail = json.dumps(response.json(), indent=2)
            except Exception:
                pass
            raise ValueError(
                f"IAM Roles Anywhere CreateSession failed (HTTP {response.status_code}): "
                f"{detail}"
            )

        payload = response.json()
        creds = payload.get("credentialSet", [{}])[0].get("credentials")
        if not creds:
            raise ValueError(
                "IAM Roles Anywhere returned an unexpected response (no credentials)."
            )
        return creds

    def _get_aws_keys(self):
        """Return (access_key_id, secret_access_key, session_token) or (None, None, None)."""
        if self.params.authMethod == AwsAuthMethod.DEPLOYED_ON_AWS:
            # Plugin parity: deployed_on_aws leaves keys unset so boto3 uses its default chain.
            return None, None, None

        refresh_needed = True
        if self._credentials_cache:
            expiration = self._credentials_cache.get("expiration")
            if expiration:
                try:
                    exp_dt = datetime.strptime(expiration, AWS_CREDENTIAL_EXPIRY_FORMAT)
                    if exp_dt > datetime.utcnow() + timedelta(
                        minutes=AWS_CREDENTIAL_REFRESH_MINUTES
                    ):
                        refresh_needed = False
                except ValueError:
                    pass

        if refresh_needed:
            self._credentials_cache = self._create_roles_anywhere_session()

        creds = self._credentials_cache or {}
        ak = creds.get("accessKeyId")
        sk = creds.get("secretAccessKey")
        token = creds.get("sessionToken")
        if not all([ak, sk, token]):
            raise ValueError(
                "Unable to generate temporary credentials. Check IAM Roles Anywhere configuration."
            )
        return ak, sk, token

    def _boto3_client(self, service: str):
        ak, sk, token = self._get_aws_keys()
        return boto3.client(
            service,
            aws_access_key_id=ak,
            aws_secret_access_key=sk,
            aws_session_token=token,
            region_name=self.params.region.strip(),
            config=Config(proxies=self.proxy or {}),
        )

    @staticmethod
    def _secret_string_from_response(resp: dict) -> str:
        if resp.get("SecretString") is not None:
            return resp["SecretString"]
        binary = resp.get("SecretBinary")
        if binary is None:
            raise CouldNotResolve("Secret response missing SecretString and SecretBinary.")
        if isinstance(binary, str):
            return binary
        return binary.decode("utf-8", errors="replace")

    def validate_secrets_manager_access(self) -> None:
        """Verify credentials AND secretsmanager:GetSecretValue permission.

        STS GetCallerIdentity requires no IAM permissions, so it cannot confirm
        Secrets Manager access. Instead we probe with a non-existent secret:
        ResourceNotFoundException means the call was authorized (secret just absent);
        AccessDeniedException means the IAM role lacks GetSecretValue permission.
        """
        client = self._boto3_client("secretsmanager")
        try:
            client.get_secret_value(SecretId="netskope-ce-validation-probe")
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code == "ResourceNotFoundException":
                # Authorized — probe secret simply does not exist, which is expected.
                return
            raise

    @staticmethod
    def _extract_json_key(raw: str, secret_id: str, secret_key: str) -> str:
        """Return a single key from a JSON object secret string."""
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as err:
            raise CouldNotResolve(
                f"AWS secret '{secret_id}' is not valid JSON; "
                f"cannot extract key '{secret_key}'."
            ) from err
        if not isinstance(obj, dict):
            raise CouldNotResolve(
                f"AWS secret '{secret_id}' JSON root must be an object to extract "
                f"key '{secret_key}'."
            )
        if secret_key not in obj:
            raise CouldNotResolve(
                f"Key '{secret_key}' not found in AWS secret '{secret_id}'."
            )
        value = obj[secret_key]
        if value is None:
            raise CouldNotResolve(
                f"Key '{secret_key}' in AWS secret '{secret_id}' is null or missing."
            )
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return str(value)

    def get_secret_value(
        self, secret_id: str, secret_key: Optional[str] = None
    ) -> str:
        """Fetch secret by id; optionally extract a key from JSON SecretString."""
        client = self._boto3_client("secretsmanager")
        try:
            resp = client.get_secret_value(SecretId=secret_id)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "ClientError")
            msg = e.response.get("Error", {}).get("Message", str(e))
            if code in ("ResourceNotFoundException", "ResourceNotFound"):
                raise CouldNotResolve(f"AWS secret '{secret_id}' not found.") from e
            if code in (
                "AccessDeniedException",
                "UnauthorizedException",
                "UnrecognizedClientException",
            ):
                raise CouldNotResolve(
                    f"Access denied retrieving AWS secret '{secret_id}': {msg}"
                ) from e
            raise CouldNotResolve(f"AWS Secrets Manager error ({code}): {msg}") from e
        raw = self._secret_string_from_response(resp)
        if secret_key:
            return self._extract_json_key(raw, secret_id, secret_key)
        return raw


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
        elif settings.params.provider == "azure":
            self.client = self._build_azure_client(settings.params, proxy=proxy)
        elif settings.params.provider == "aws":
            self.client = self._build_aws_client(settings.params, proxy=proxy)
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

    def _build_azure_client(
        self, settings: SecretsManagerAzureParams, proxy: dict = {}
    ) -> SecretClient:
        """Build the Azure Key Vault client.

        Args:
            settings (SecretsManagerAzureParams): Parameters.
            proxy (dict, defaults to {}): Proxy to use. Format: {"http": "url", "https": "url"}

        Returns:
            SecretClient: Initialized Azure Key Vault client.
        """
        # Create transport with proxy support if proxy is configured
        transport = None
        if proxy and (proxy.get("http") or proxy.get("https")):
            session = requests.Session()
            session.proxies = {
                "http": proxy.get("http", ""),
                "https": proxy.get("https", ""),
            }
            transport = RequestsTransport(session=session)
            logger.debug(f"Azure client using proxy: {proxy.get('https') or proxy.get('http')}")

        credential = None
        if settings.authMethod == AzureAuthMethod.CLIENT_SECRET:
            credential = ClientSecretCredential(
                tenant_id=settings.tenantId,
                client_id=settings.clientId,
                client_secret=settings.clientSecret,
                transport=transport,
            )
        elif settings.authMethod == AzureAuthMethod.CERTIFICATE:
            # Certificate content is passed directly as PEM string
            credential = CertificateCredential(
                tenant_id=settings.tenantId,
                client_id=settings.clientId,
                certificate_data=settings.certificate.encode("utf-8"),
                password=settings.certificatePassword.encode("utf-8")
                if settings.certificatePassword
                else None,
                transport=transport,
            )
        else:
            raise NotImplementedError(f"Unsupported Azure auth method: {settings.authMethod}")

        client = SecretClient(vault_url=settings.vaultUrl, credential=credential, transport=transport)
        logger.debug(f"Azure Key Vault client initialized for {settings.vaultUrl}")
        return client

    def _build_aws_client(self, settings, proxy: dict = {}) -> AwsSecretsManagerClient:
        """Build the AWS Secrets Manager wrapper client."""
        return AwsSecretsManagerClient(settings, proxy=proxy)

    @staticmethod
    def _parse_aws_resolve_path(path: str) -> tuple:
        """Split resolve path into secret id and optional JSON key.

        Path format (without secret: prefix): ``tenantv2`` or ``tenantv2:apiKey``.
        Secret names cannot contain ``:``; ARNs can, so key extraction is only
        supported when using the secret name (not the full ARN).
        """
        decoded = unquote(path)
        if decoded.startswith("arn:") or ":" not in decoded:
            return decoded, None
        secret_id, secret_key = decoded.split(":", 1)
        secret_key = unquote(secret_key).strip() if secret_key else None
        return secret_id, secret_key or None

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
                    raise CouldNotResolve(
                        "Could not resolve HashiCorp secret. Make sure the secret path "
                        "exists or prefix is correct."
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
        elif self.settings.params.provider == "azure":
            try:
                # Azure format: secret:{secretName}
                # The path comes without the "secret:" prefix, so it's just {secretName}
                secret_name = unquote(path)
                secret = self.client.get_secret(secret_name)
                if secret is None or secret.value is None:
                    raise CouldNotResolve(f"Azure secret '{secret_name}' not found or empty.")
                return secret.value
            except ResourceNotFoundError:
                logger.error(
                    f"Azure Key Vault secret '{secret_name}' not found.",
                    details=traceback.format_exc(),
                )
                raise CouldNotResolve(f"Azure secret '{secret_name}' not found.")
            except ClientAuthenticationError:
                # Try to re-initialize and retry
                settings = SecretManager.load_settings()
                self.initialize(
                    settings.secretsManagerSettings, proxy=get_proxy_params(settings)
                )
                if not self.settings.enabled:
                    raise SecretsManagerDisabledException()
                try:
                    secret = self.client.get_secret(secret_name)
                    if secret is None or secret.value is None:
                        raise CouldNotResolve(f"Azure secret '{secret_name}' not found or empty.")
                    return secret.value
                except ClientAuthenticationError:
                    logger.error(
                        "Azure Key Vault authentication failed. Update credentials in "
                        "Settings > General > Secrets Manager.",
                        details=traceback.format_exc(),
                    )
                    raise CouldNotResolve("Azure authentication failed.")
                except ResourceNotFoundError:
                    logger.error(
                        f"Azure Key Vault secret '{secret_name}' not found.",
                        details=traceback.format_exc(),
                    )
                    raise CouldNotResolve(f"Azure secret '{secret_name}' not found.")
            except HttpResponseError as ex:
                logger.error(
                    f"Azure Key Vault error: {ex.message}",
                    details=traceback.format_exc(),
                )
                raise CouldNotResolve(f"Azure Key Vault error: {ex.message}")
            except Exception as ex:
                logger.error(
                    f"Error occurred while resolving Azure secret. {repr(ex)}",
                    details=traceback.format_exc(),
                )
                raise CouldNotResolve(f"Azure error: {repr(ex)}")
        elif self.settings.params.provider == "aws":
            try:
                secret_id, secret_key = self._parse_aws_resolve_path(path)
                return self.client.get_secret_value(secret_id, secret_key)
            except CouldNotResolve:
                raise
            except NoCredentialsError:
                logger.error(
                    AWS_NO_CREDENTIALS_MSG,
                    details=traceback.format_exc(),
                )
                raise CouldNotResolve(AWS_NO_CREDENTIALS_MSG)
            except Exception as ex:
                logger.error(
                    f"Error resolving AWS secret '{path}': {repr(ex)}",
                    details=traceback.format_exc(),
                )
                settings = SecretManager.load_settings()
                self.initialize(
                    settings.secretsManagerSettings, proxy=get_proxy_params(settings)
                )
                if not self.settings.enabled:
                    raise SecretsManagerDisabledException()
                try:
                    secret_id, secret_key = self._parse_aws_resolve_path(path)
                    return self.client.get_secret_value(secret_id, secret_key)
                except CouldNotResolve:
                    raise
                except NoCredentialsError:
                    logger.error(
                        "AWS authentication failed. Update credentials in "
                        "Settings > General > Secrets Manager.",
                        details=traceback.format_exc(),
                    )
                    raise CouldNotResolve("AWS authentication failed.")
                except Exception as retry_ex:
                    logger.error(
                        f"Error occurred while resolving AWS secret. {repr(retry_ex)}",
                        details=traceback.format_exc(),
                    )
                    raise CouldNotResolve(f"AWS error: {repr(retry_ex)}")
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
        except (CouldNotResolve, SecretsManagerDisabledException) as e:
            raise ValueError(str(e)) from e
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


def _build_azure_client_for_validation(
    vault_url: str,
    tenant_id: str,
    client_id: str,
    client_secret: Optional[str] = None,
    certificate: Optional[str] = None,
    certificate_password: Optional[str] = None,
    proxy: dict = {},
) -> SecretClient:
    """Build Azure Key Vault client for validation purposes.

    This function is called during settings validation to verify credentials.

    Args:
        vault_url: Azure Key Vault URL
        tenant_id: Azure AD Tenant ID
        client_id: App Registration Client ID
        client_secret: Client secret (for client_secret auth)
        certificate: PEM certificate content (for certificate auth)
        certificate_password: Certificate password (optional)
        proxy: Proxy settings. Format: {"http": "url", "https": "url"}

    Returns:
        SecretClient: Validated Azure Key Vault client

    Raises:
        ValueError: If authentication fails
    """
    # Create transport with proxy support if proxy is configured
    transport = None
    if proxy and (proxy.get("http") or proxy.get("https")):
        session = requests.Session()
        session.proxies = {
            "http": proxy.get("http", ""),
            "https": proxy.get("https", ""),
        }
        transport = RequestsTransport(session=session)
        logger.debug(f"Azure validation using proxy: {proxy.get('https') or proxy.get('http')}")

    credential = None
    try:
        if client_secret:
            credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
                transport=transport,
            )
        elif certificate:
            credential = CertificateCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                certificate_data=certificate.encode("utf-8"),
                password=certificate_password.encode("utf-8")
                if certificate_password
                else None,
                transport=transport,
            )
        else:
            raise ValueError("Either client_secret or certificate must be provided.")

        client = SecretClient(vault_url=vault_url, credential=credential, transport=transport)
        # Try to list secrets (limited to 1) to validate the connection
        # This will fail if credentials are invalid
        list(client.list_properties_of_secrets(max_page_size=1))
        logger.debug(f"Azure Key Vault validation successful for {vault_url}")
        return client
    except ClientAuthenticationError as ex:
        raise ValueError(f"Azure authentication failed: {str(ex)}")
    except HttpResponseError as ex:
        if ex.status_code == 403:
            raise ValueError(
                "Access denied. Ensure the app has 'Secret List' and 'Secret Get' "
                "permissions in the Key Vault access policy."
            )
        raise ValueError(f"Azure Key Vault error: {ex.message}")
    except Exception as ex:
        raise ValueError(f"Azure validation failed: {str(ex)}")


def _validate_aws_connection(params: "SecretsManagerAwsParams", proxy: dict = {}) -> None:
    """Validate AWS credentials and secretsmanager:GetSecretValue permission."""
    try:
        AwsSecretsManagerClient(params, proxy=proxy).validate_secrets_manager_access()
    except NoCredentialsError as exp:
        raise ValueError(AWS_NO_CREDENTIALS_MSG) from exp
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "ClientError")
        msg = e.response.get("Error", {}).get("Message", str(e))
        if code in ("AccessDeniedException", "UnauthorizedException"):
            raise ValueError(
                f"Credentials are valid but lack secretsmanager:GetSecretValue permission. "
                f"Attach the required policy to the IAM role. ({msg})"
            ) from e
        raise ValueError(f"AWS Secrets Manager validation failed ({code}): {msg}") from e
