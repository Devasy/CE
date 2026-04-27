"""All the integrations."""


def trim_space_parameters_fields(configuration_parameters):
    """Trim empty spaces for parameters fields."""
    if isinstance(configuration_parameters, dict):
        for key, value in configuration_parameters.items():
            if isinstance(value, str):
                configuration_parameters[key] = value.strip()
            if isinstance(value, dict):
                trim_space_parameters_fields(value)
