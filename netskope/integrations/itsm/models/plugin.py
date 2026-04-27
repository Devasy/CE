"""Plugin related models."""

from pydantic import StringConstraints, BaseModel, Field
from typing_extensions import Annotated


class PluginOut(BaseModel):
    """Outgoing plugin model."""

    name: str = Field(..., description="Name of the plugin.")
    id: str = Field(..., description="Unique ID of the plugin.")
    version: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(..., description="Version of the plugin.")
    description: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(
        ..., description="Plugin description."
    )
    configuration: list = Field(
        ..., description="Plugin configuration fields."
    )
    receivingSupported: bool = Field(...)
    sharingSupported: bool = Field(...)
    icon: Annotated[str, StringConstraints(strip_whitespace=True)] = Field(..., description="Plugin icon image.")
