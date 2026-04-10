"""Tenant related stuff."""

import re
from datetime import datetime
from typing import Union, List, Optional

from pydantic import field_validator, StringConstraints, BaseModel, Field

from .other import PollIntervalUnit
from ..utils.db_connector import DBConnector, Collections
from typing_extensions import Annotated

connector = DBConnector()


class LockFields(BaseModel):
    """Locking related fields."""

    alerts: Union[datetime, None] = Field(None)
    events: Union[datetime, None] = Field(None)
    applications: Union[datetime, None] = Field(None)


class Checkpoint(BaseModel):
    """The outgoing checkpoint model for tenant."""

    alert: Union[datetime, None] = Field(None)
    event: Union[datetime, None] = Field(None)


"""
    Sample structure of Netskope Provider
    "parameters": {
        tenantName: str
        token: Optional[str] = None
        v2token: Union[str, None] = Field(None)
    }

    "storage": {
        lockedAt: LockFields = Field(LockFields())
        first_event_pull: dict = Field(
            {
                "first_infrastructure_pull": True,
                "first_page_pull": True,
                "first_network_pull": True,
                "first_audit_pull": True,
                "first_application_pull": True,
                "first_incident_pull": True,
            }
        )
        disabled_event_pull: dict = Field(
            {
                "disabled_infrastructure_pull": None,
                "disabled_page_pull": None,
                "disabled_network_pull": None,
                "disabled_audit_pull": None,
                "disabled_application_pull": None,
                "disabled_incident_pull": None,
            }
        )
        is_checkpoint_used_application: bool = Field(False)
        is_checkpoint_used_audit: bool = Field(False)
        is_checkpoint_used_infrastructure: bool = Field(False)
        is_checkpoint_used_network: bool = Field(False)
        is_checkpoint_used_page: bool = Field(False)
        is_checkpoint_used_alert: bool = Field(False)
        is_checkpoint_used_incident: bool = Field(False)
        first_alert_pull: dict = Field(
            {
                "first_policy_pull": True,
                "first_Compromised Credential_pull": True,
                "first_Legal Hold_pull": True,
                "first_malsite_pull": True,
                "first_Malware_pull": True,
                "first_DLP_pull": True,
                "first_Security Assessment_pull": True,
                "first_watchlist_pull": True,
                "first_quarantine_pull": True,
                "first_Remediation_pull": True,
                "first_uba_pull": True,
                "first_ctep_pull": True,
            }
        )
        disabled_alert_pull: dict = Field(
            {
                "disabled_policy_pull": None,
                "disabled_Compromised Credential_pull": None,
                "disabled_Legal Hold_pull": None,
                "disabled_malsite_pull": None,
                "disabled_Malware_pull": None,
                "disabled_DLP_pull": None,
                "disabled_Security Assessment_pull": None,
                "disabled_watchlist_pull": None,
                "disabled_quarantine_pull": None,
                "disabled_Remediation_pull": None,
                "disabled_uba_pull": None,
                "disabled_ctep_pull": None,
            }
        )
        forbidden_endpoints: dict = Field({})
        is_v2_token_expired: bool = Field(False)
    }
"""


def validate_tenant_parameters(cls, value, values, **kwargs):
    """Validate tenant parameters."""
    if not isinstance(value, dict):
        raise ValueError("Parameters should be a dictionary.")
    if "tenantName" not in value:
        raise ValueError("Tenant URL is required.")
    value["tenantName"] = value["tenantName"].strip().strip("/").strip()
    if value.get("token", ""):
        value["token"] = value["token"].strip()
    if not value.get("v2token", ""):
        raise ValueError("V2 token is required.")
    value["v2token"] = value["v2token"].strip()
    return value


def validate_tenant_update_parameters(cls, value, values, **kwargs):
    """Validate tenant update parameters."""
    if not value:
        return None
    if not isinstance(value, dict):
        raise ValueError("Parameters should be a dictionary.")
    values = values.data
    if "tenantName" in value:
        value["tenantName"] = value["tenantName"].strip().strip("/").strip()
    return value


class TenantDB(BaseModel):
    """Netskope tenant model."""

    name: str
    plugin: str = Field(...)
    parameters: dict = Field(dict())
    storage: dict = Field(dict())
    pollInterval: int = Field(30)
    pollIntervalUnit: PollIntervalUnit = Field(PollIntervalUnit.SECONDS)
    checkpoint: dict = Field(dict())
    lockedAt: dict = Field(dict())


class TenantIn(BaseModel):
    """Generic tenant model."""

    name: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)
    ]
    plugin: str = Field(...)

    @field_validator("name")
    @classmethod
    def _validate_unique_name(cls, v):
        """Make sure the name is unique."""
        if not re.match(r"^[a-zA-Z0-9]([a-zA-Z0-9 _\-]*[a-zA-Z0-9])*$", v):
            raise ValueError(
                (
                    "Name should start and end with an alpha-numeric character "
                    "and can include alpha-numeric characters, dashes, underscores and spaces."
                )
            )
        if (
            connector.collection(Collections.NETSKOPE_TENANTS).find_one({"name": v})
            is not None
        ):
            raise ValueError(f"Tenant with name {v} already exists.")
        return v

    parameters: dict = Field(dict())
    _validate_parameters = field_validator("parameters")(validate_tenant_parameters)
    pollInterval: int = Field(30)
    pollIntervalUnit: PollIntervalUnit = Field(PollIntervalUnit.SECONDS)


class TenantOldIn(BaseModel):
    """Netskope tenant model before 5.1.0."""

    name: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)
    ]

    @field_validator("name")
    @classmethod
    def _validate_unique_name(cls, v):
        """Make sure the name is unique."""
        if not re.match(r"^[a-zA-Z0-9]([a-zA-Z0-9 _\-]*[a-zA-Z0-9])*$", v):
            raise ValueError(
                (
                    "Name should start and end with an alpha-numeric character "
                    "and can include alpha-numeric characters, dashes, underscores and spaces."
                )
            )
        if (
            connector.collection(Collections.NETSKOPE_TENANTS).find_one({"name": v})
            is not None
        ):
            raise ValueError(f"Tenant with name {v} already exists.")
        return v

    tenantName: str

    @field_validator("tenantName")
    @classmethod
    def _validate_tenant_name(cls, v: str):
        return v.strip()

    alert_types: List[str] = []
    v2token: Annotated[str, StringConstraints(strip_whitespace=True)]
    token: Optional[Annotated[str, StringConstraints(strip_whitespace=True)]] = None
    pollInterval: int = Field(30)
    pollIntervalUnit: PollIntervalUnit = Field(PollIntervalUnit.SECONDS)
    initialRange: int = Field(7)
    first_alert_pull: dict = Field({})
    disabled_alert_pull: dict = Field({})
    first_event_pull: dict = Field({})
    disabled_event_pull: dict = Field({})
    forbidden_endpoints: dict = Field({})


class TenantUpdate(BaseModel):
    """Generic tenant update model."""

    name: str

    @field_validator("name")
    @classmethod
    def _validate_name_exists(cls, v):
        """Make sure the name exists."""
        if (
            connector.collection(Collections.NETSKOPE_TENANTS).find_one({"name": v})
            is None
        ):
            raise ValueError(f"Tenant with name {v} does not exist.")
        return v

    parameters: dict
    _validate_parameters = field_validator("parameters")(validate_tenant_update_parameters)
    pollInterval: Union[int, None] = Field(None)
    pollIntervalUnit: Union[PollIntervalUnit, None] = Field(None)


class TenantOldUpdate(BaseModel):
    """Netskope tenant update model before 5.1.0."""

    name: str

    @field_validator("name")
    @classmethod
    def _validate_name_exists(cls, v):
        """Make sure the name exists."""
        if (
            connector.collection(Collections.NETSKOPE_TENANTS).find_one({"name": v})
            is None
        ):
            raise ValueError(f"Tenant with name {v} does not exist.")
        return v

    tenantName: str = Field(...)

    @field_validator("tenantName")
    @classmethod
    def _validate_tenant_name(cls, v: str):
        if v:
            return v.strip()

    alert_types: List[str] = []
    v2token: Union[
        Annotated[
            str, Field(validate_default=True), StringConstraints(strip_whitespace=True)
        ],
        None,
    ] = Field(None)
    token: Optional[Annotated[str, StringConstraints(strip_whitespace=True)]] = None

    pollInterval: Union[int, None] = Field(None)
    pollIntervalUnit: Union[PollIntervalUnit, None] = Field(None)


class TenantOut(BaseModel):
    """Generic tenant model."""

    name: str
    plugin: str = Field(...)
    parameters: dict = Field(dict())
    pollInterval: Union[int, None] = Field(None)
    pollIntervalUnit: Union[PollIntervalUnit, None] = Field(None)
    checkpoint: dict = Field(dict())

    @field_validator("parameters")
    @classmethod
    def _remove_sensitive_parameters(cls, val, values, **kwargs):
        """Remove password fields."""
        # Imported here to avoid circular imports
        from netskope.common.utils.plugin_helper import PluginHelper

        plugin_helper = PluginHelper()

        values = values.data
        plugin_path = values["plugin"]
        PluginClass = plugin_helper.find_by_id(plugin_path)
        if PluginClass is None:
            print(f"Could not find the provider plugin with id='{plugin_path}'.")
            raise ValueError(
                f"Could not find the provider plugin with id='{plugin_path}'."
            )

        metadata = PluginClass.metadata
        if metadata and metadata.get("configuration"):
            for field in metadata["configuration"]:
                if field["type"] == "password":
                    val.pop(field["key"], None)
        return val
