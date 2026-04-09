"""Provides NCE upload related models."""
from typing import Optional

from pydantic import (
    BaseModel,
    Field,
    FieldValidationInfo,
    StringConstraints,
    field_validator,
)
from typing_extensions import Annotated

from netskope.common.utils import Collections, DBConnector, Logger, PluginHelper

from .plugin import ConfigurationDB

connector = DBConnector()
helper = PluginHelper()
logger = Logger()


class NCEUpload(BaseModel):
    """Netskope CE plugin upload model."""

    destination: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(
        ..., description="Destination NCE Configuration name."
    )
    ce_identifier: Optional[str] = Field(
        None, description="Unique ID as a source identifier."
    )
    file_name: str = Field(
        description="File name to be used for analytics and dashboards."
    )
    edm_hashes_cfg: str = Field(description="EDM metadate file name.")

    @field_validator("destination")
    @classmethod
    def _validate_destination(cls, val: str):
        destination_config = connector.collection(
            Collections.EDM_CONFIGURATIONS
        ).find_one({"name": val})
        if destination_config is None:
            raise ValueError(
                f"Destination configuration with name '{val}' "
                "does not exist on provided Netskope CE machine."
            )
        destination_config = ConfigurationDB(**destination_config)
        if not destination_config.active:
            raise ValueError(
                f"Destination configuration with name '{val}'"
                " is disabled on provided Netskope CE machine."
            )
        PluginClass = helper.find_by_id(destination_config.plugin)
        if not PluginClass:
            raise ValueError(
                f"Configuration: {val} is not a valid plugin on provided Netskope CE machine."
            )
        metadata = PluginClass.metadata
        logger.debug(f"config plugin {destination_config.plugin}")
        if (
            not metadata.get("netskope")
            or not PluginHelper.check_plugin_name_with_regex(
                "netskope_edm_forwarder_receiver",
                destination_config.plugin
            )
        ):
            raise ValueError(
                f"Configuration: {val} is not of type Netskope EDM Forwarder/Receiver Plugin,"
                " on provided Netskope CE machine."
            )
        if not destination_config.pluginType == "receiver":
            raise ValueError(
                f"Configuration: '{val}', is not a Netskope CE receiver type plugin on provided Netskope CE machine."
            )
        return val

    @field_validator("ce_identifier")
    @classmethod
    def _validate_ce_identifier(cls, val: str, info: FieldValidationInfo):
        not_allowed_identifier = ["edm_hash_folder", "edm_hash_available"]
        if val and info.data.get("destination"):
            config_name = info.data["destination"]
            destination_config = connector.collection(
                Collections.EDM_CONFIGURATIONS
            ).find_one({"name": config_name})
            if (
                destination_config
                and val in not_allowed_identifier
                and val not in (destination_config.get("storage", {}) or {})
            ):
                raise ValueError("Invalid request")
        return val
