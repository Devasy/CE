"""Provides plugin implementation related classes."""

from typing import List, Optional
from pydantic import BaseModel

from netskope.common.utils import PluginBase as CommonPluginBase


class ValidationResult(BaseModel):
    """Validation result model (returned by Plugin.validate method)."""

    message: str
    success: bool = False


class PushResult(BaseModel):
    """Push result model (returned by Plugin.push method)."""

    message: str
    success: bool = False
    failed_data: Optional[List] = []


class PluginBase(CommonPluginBase):
    """CLS plugin base class."""

    integration = "cls"

    def __init__(
        self,
        name,
        configuration,
        storage,
        last_run_at,
        logger,
        use_proxy=True,
        ssl_validation=True,
        source=None,
        mappings=None,
    ):
        """Initialize."""
        super().__init__(
            name,
            configuration,
            storage,
            last_run_at,
            logger,
            use_proxy=use_proxy,
            ssl_validation=ssl_validation,
        )
        if self.configuration:
            transform_data = self._configuration.get("transformData")
            if isinstance(transform_data, str) and self.metadata and not self.metadata.get("format_options", None):
                self._configuration["transformData"] = transform_data == "cef"

        self._source = source
        self._mappings = mappings

    def pull(self, cursor=None, start_time=None, end_time=None) -> List:
        """Pull indicators from Netskope.

        Raises:
            NotImplementedError: If the method is not implemented.

        Args:
            cursor : Used for Syslog for CE. Defaults to None.
            start_time : Used for Syslog for CE. Defaults to None.
            end_time : Used for Syslog for CE. Defaults to None.

        Returns:
            List: List of raw_data to be ingested on the target platform.
        """
        pass

    def push(self, transformed_data, data_type, subtype) -> PushResult:
        """Push the transformed_data to the 3rd party platform.

        Args:
            transformed_data (list): The transformed data to be ingested.
            data_type (str): The type of data to be ingested (alert/event)
            subtype (str): The subtype of data to be ingested (DLP, anomaly etc. in case of alerts)

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            PushResult: Result indicating ingesting outcome and message
        """
        raise NotImplementedError()

    def transform(self, raw_data, data_type, subtype) -> List:
        """Transform the raw netskope JSON data into target platform supported data formats.

        Args:
            raw_data (list): The raw data to be tranformed.
            data_type (str): The type of data to be ingested (alert/event)
            subtype (str): The subtype of data to be ingested (DLP, anomaly etc. in case of alerts)

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            List: list of transformed data.
        """
        raise NotImplementedError()

    def validate(self, configuration: dict, value: None) -> ValidationResult:
        """Validate the configuration parameters dict and mapping file.

        Args:
            configuration (dict): Dictionary containing all the requested parameters.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            ValidationResult: Result indicating validation outcome and message.
        """
        raise NotImplementedError()

    def extract_and_store_fields(
        self, data: list[dict], data_type: str, subtype: str
    ) -> None:
        """Extract and store fields from data."""
        raise NotImplementedError()

    @staticmethod
    def chunk_size() -> int:
        """Define the chunk size of data to be pushed in one go. Each plugin must implement this method."""
        return 10000

    def get_subtypes(self, data_type):
        """Extract the data types (types of alerts and events) from plugin specific mapping file.

        :param data_type: The type of data for which subtypes are to be fetched
        """
        pass
        # raise NotImplementedError()

    @property
    def mappings(self) -> dict:
        """Get the mapping for CLS.

        Returns:
            dict: mappping json string.
        """
        return self._mappings

    @property
    def source(self) -> str:
        """Get source for the current configuration.

        Returns:
            str: Source.
        """
        return self._source

    def validate_mappings(self) -> ValidationResult:
        """Validate the configured mappings.

        Returns:
            ValidationResult: Result indicating the Mapping validation and output.
        """
        pass
