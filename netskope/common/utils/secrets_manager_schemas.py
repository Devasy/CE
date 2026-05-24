"""Secrets Manager Provider Schemas for Dynamic UI Generation.

This module defines the configuration schemas for each secrets manager provider.
The UI renders forms dynamically based on these schemas, following the same
pattern as plugin configuration forms.

Schema Structure:
- Each provider has a unique ID and display name
- `fields` define the form fields for provider configuration
- `secret_path_schema` defines how secrets are referenced in plugin configs
"""

from typing import List, Dict, Any, Optional
from enum import Enum


class FieldType(str, Enum):
    """Supported field types for dynamic form rendering."""

    TEXT = "text"
    PASSWORD = "password"
    SELECT = "select"
    BOOLEAN = "boolean"
    NUMBER = "number"
    TEXTAREA = "textarea"


def create_field(
    key: str,
    label: str,
    field_type: FieldType,
    required: bool = True,
    placeholder: str = "",
    description: str = "",
    options: Optional[List[Dict[str, str]]] = None,
    depends_on: Optional[Dict[str, Any]] = None,
    validation: Optional[Dict[str, str]] = None,
    default: Any = None,
) -> Dict[str, Any]:
    """Create a field definition for dynamic form rendering.

    Args:
        key: Field identifier (used as form field name)
        label: Display label for the field
        field_type: Type of input field
        required: Whether the field is mandatory
        placeholder: Placeholder text
        description: Help text shown in tooltip
        options: For select fields, list of {value, label} options
        depends_on: Conditional display based on other field values
        validation: Validation rules (pattern, message)
        default: Default value

    Returns:
        Field definition dictionary
    """
    field = {
        "key": key,
        "label": label,
        "type": field_type.value,
        "required": required,
        "placeholder": placeholder,
        "description": description,
    }

    if options:
        field["options"] = options
    if depends_on:
        field["depends_on"] = depends_on
    if validation:
        field["validation"] = validation
    if default is not None:
        field["default"] = default

    return field


# =============================================================================
# HashiCorp Vault Provider Schema
# =============================================================================

HASHICORP_FIELDS = [
    create_field(
        key="clusterURL",
        label="Vault URL",
        field_type=FieldType.TEXT,
        required=True,
        placeholder="https://vault.example.com:8200",
        description="Enter your HashiCorp Vault server URL",
        validation={
            "pattern": "^https?://.*",
            "message": "Must be a valid URL starting with http:// or https://",
        },
    ),
    create_field(
        key="namespace",
        label="Namespace",
        field_type=FieldType.TEXT,
        required=False,
        placeholder="admin/my-namespace",
        description="Vault namespace (optional, for Vault Enterprise)",
    ),
    create_field(
        key="authMethod",
        label="Authentication Method",
        field_type=FieldType.SELECT,
        required=True,
        description="Select how to authenticate with HashiCorp Vault",
        options=[
            {"value": "token", "label": "Token"},
            {"value": "approle", "label": "AppRole"},
            {"value": "username/password", "label": "Username & Password"},
        ],
    ),
    # Token auth fields
    create_field(
        key="token",
        label="Token",
        field_type=FieldType.PASSWORD,
        required=True,
        placeholder="Token",
        description="Vault authentication token",
        depends_on={"authMethod": "token"},
    ),
    # AppRole auth fields
    create_field(
        key="path",
        label="Auth Path",
        field_type=FieldType.TEXT,
        required=True,
        placeholder="approle",
        description="Mount path for the authentication method",
        default="approle",
        depends_on={"authMethod": ["approle", "username/password"]},
    ),
    create_field(
        key="roleId",
        label="Role ID",
        field_type=FieldType.TEXT,
        required=True,
        placeholder="Role ID",
        description="AppRole Role ID",
        depends_on={"authMethod": "approle"},
    ),
    create_field(
        key="secretId",
        label="Secret ID",
        field_type=FieldType.PASSWORD,
        required=True,
        placeholder="Secret ID",
        description="AppRole Secret ID",
        depends_on={"authMethod": "approle"},
    ),
    # Username/Password auth fields
    create_field(
        key="username",
        label="Username",
        field_type=FieldType.TEXT,
        required=True,
        placeholder="Username",
        description="Username for authentication",
        depends_on={"authMethod": "username/password"},
    ),
    create_field(
        key="password",
        label="Password",
        field_type=FieldType.PASSWORD,
        required=True,
        placeholder="Password",
        description="Password for authentication",
        depends_on={"authMethod": "username/password"},
    ),
]

HASHICORP_SECRET_PATH_SCHEMA = {
    "fields": [
        {
            "key": "engine",
            "label": "Secret Engine",
            "type": "text",
            "required": True,
            "placeholder": "Secret Engine",
            "description": "Secret engine mount path (e.g., 'kv', 'secret')",
        },
        {
            "key": "path",
            "label": "Secret Path",
            "type": "text",
            "required": True,
            "placeholder": "Path to the Secret",
            "description": "Path to the secret within the engine",
        },
        {
            "key": "key",
            "label": "Secret Key",
            "type": "text",
            "required": True,
            "placeholder": "Secret Key",
            "description": "Key name within the secret",
        },
    ],
    "format": "secret:{engine}/data/{path}:{key}",
    "parse_regex": r"^secret:([^/]+)/data/([^:]+):(.+)$",
    "display_format": "{engine} / {path} : {key}",
}


AZURE_FIELDS = [
    create_field(
        key="vaultUrl",
        label="Vault URL",
        field_type=FieldType.TEXT,
        required=True,
        placeholder="https://myvault.vault.azure.net/",
        description="Your Azure Key Vault URL (e.g., https://myvault.vault.azure.net/)",
        validation={
            "pattern": "^https?://.*",
            "message": "Must be a valid URL starting with http:// or https://",
        },
    ),
    create_field(
        key="tenantId",
        label="Tenant ID",
        field_type=FieldType.TEXT,
        required=True,
        placeholder="Tenant ID",
        description="Your Azure AD Tenant ID (GUID format)",
    ),
    create_field(
        key="clientId",
        label="Client ID",
        field_type=FieldType.TEXT,
        required=True,
        placeholder="Client ID",
        description="App Registration Client ID (Application ID)",
    ),
    create_field(
        key="authMethod",
        label="Authentication Method",
        field_type=FieldType.SELECT,
        required=True,
        description="Select how to authenticate with Azure Key Vault",
        options=[
            {"value": "client_secret", "label": "Client Secret"},
            {"value": "certificate", "label": "Certificate"},
        ],
    ),
    # Client Secret auth fields
    create_field(
        key="clientSecret",
        label="Client Secret",
        field_type=FieldType.PASSWORD,
        required=True,
        placeholder="Client Secret",
        description="Service Principal client secret value",
        depends_on={"authMethod": "client_secret"},
    ),
    # Certificate auth fields
    create_field(
        key="certificate",
        label="Certificate (PEM)",
        field_type=FieldType.TEXTAREA,
        required=True,
        placeholder=(
            "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----\n"
            "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
        ),
        description="Paste the full PEM certificate content including the private key",
        depends_on={"authMethod": "certificate"},
    ),
    create_field(
        key="certificatePassword",
        label="Certificate Password",
        field_type=FieldType.PASSWORD,
        required=False,
        placeholder="Enter certificate password (if encrypted)",
        description="Password for encrypted certificate private key (optional)",
        depends_on={"authMethod": "certificate"},
    ),
]

AZURE_SECRET_PATH_SCHEMA = {
    "fields": [
        {
            "key": "secretName",
            "label": "Secret Name",
            "type": "text",
            "required": True,
            "placeholder": "Secret Name",
            "description": "Name of the secret in Azure Key Vault",
        },
    ],
    "format": "secret:{secretName}",
    "parse_regex": r"^secret:([^/]+)$",
    "display_format": "{secretName}",
}


SECRETS_MANAGER_PROVIDERS = {
    "hashicorp": {
        "id": "hashicorp",
        "name": "HashiCorp Vault",
        "description": "HashiCorp Vault enterprise secrets management",
        "fields": HASHICORP_FIELDS,
        "secret_path_schema": HASHICORP_SECRET_PATH_SCHEMA,
    },
    "azure": {
        "id": "azure",
        "name": "Azure Key Vault",
        "description": "Microsoft Azure Key Vault secrets management",
        "fields": AZURE_FIELDS,
        "secret_path_schema": AZURE_SECRET_PATH_SCHEMA,
    },
}


def get_provider_schema(provider_id: str) -> Optional[Dict[str, Any]]:
    """Get the schema for a specific provider.

    Args:
        provider_id: Provider identifier (e.g., 'hashicorp', 'azure')

    Returns:
        Provider schema dictionary or None if not found
    """
    return SECRETS_MANAGER_PROVIDERS.get(provider_id)


def get_all_providers() -> List[Dict[str, Any]]:
    """Get list of all available providers with their schemas.

    Returns:
        List of provider schema dictionaries
    """
    return list(SECRETS_MANAGER_PROVIDERS.values())


def get_secret_path_schema(provider_id: str) -> Optional[Dict[str, Any]]:
    """Get the secret path schema for a provider.

    Args:
        provider_id: Provider identifier

    Returns:
        Secret path schema dictionary or None
    """
    provider = SECRETS_MANAGER_PROVIDERS.get(provider_id)
    if provider:
        return provider.get("secret_path_schema")
    return None
