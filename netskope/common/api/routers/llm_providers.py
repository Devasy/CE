"""LLM provider configuration related endpoints."""

from datetime import datetime, timezone
from typing import List, Optional
import traceback

from fastapi import APIRouter, HTTPException, Path, Security

from netskope.common.celery.attention_scan import (
    purge_attention_findings,
    register_attention_scan_schedule,
    unregister_attention_scan_schedule,
)
from netskope.common.utils.attention_data import llm_provider_configured
from netskope.integrations import trim_space_parameters_fields

from ...models import (
    LLMProviderDB,
    LLMProviderIn,
    LLMProviderOut,
    LLMProviderUpdate,
    LLMProviderValidateIn,
    User,
)
from ...utils import (
    Collections,
    DBConnector,
    Logger,
    PluginHelper,
    SecretDict,
    get_dynamic_fields_from_plugin,
)
from .auth import get_current_user

router = APIRouter()
connector = DBConnector()
logger = Logger()
plugin_helper = PluginHelper()


def _get_plugin_class(plugin_id: str):
    plugin_class = plugin_helper.find_by_id(plugin_id)
    if plugin_class is None:
        raise HTTPException(
            400, f"Could not find LLM provider plugin with id='{plugin_id}'."
        )
    if plugin_helper.find_integration_by_id(plugin_id) != "llm_provider":
        raise HTTPException(400, f"Plugin '{plugin_id}' is not an LLM provider plugin.")
    return plugin_class


def _validate_provider(plugin, parameters: dict):
    """Run the plugin's validate() and raise a 400 on failure.

    The ONE definition create/update/validate share, so all three reject bad credentials
    identically (same error strings). Returns the ValidationResult on success (its ``.message`` is
    surfaced by the validate route).
    """
    try:
        result = plugin.validate(SecretDict(parameters))
    except ValueError:
        logger.error(
            "Error occurred while validating plugin.",
            details=traceback.format_exc(),
        )
        raise HTTPException(
            400, "Error occurred while validating plugin. Check logs for more details."
        )
    if result.success is False:
        raise HTTPException(400, result.message)
    return result


def _other_active_config(exclude_name: Optional[str] = None) -> Optional[dict]:
    """Find ANY other active LLM provider config, if one exists.

    Only a SINGLE LLM provider may be enabled at a time (across all plugins), so this
    check is GLOBAL — not scoped to one plugin. The runtime already assumes one active
    provider (the gateway/analyze/attention all resolve it via ``find_one({"active": True})``),
    so allowing two would be an unenforced ambiguity; this makes the constraint explicit.
    ``exclude_name`` skips the config currently being updated.
    """
    query: dict = {"active": True}
    if exclude_name is not None:
        query["name"] = {"$ne": exclude_name}
    return connector.collection(Collections.LLM_PROVIDER_CONFIGURATIONS).find_one(query)


@router.get("/llm/providers/plugins", tags=["LLM Provider"])
async def list_llm_provider_plugins(
    user: User = Security(get_current_user, scopes=[])
) -> List[dict]:
    """List available LLM provider plugins."""
    out = []
    for plugin_class in plugin_helper.plugins.get("llm_provider", []):
        metadata = plugin_class.metadata
        out.append(
            {
                "name": metadata["name"],
                "id": plugin_class.__module__,
                "version": metadata["version"],
                "configuration": metadata["configuration"],
                "description": metadata["description"],
                "icon": metadata["icon"],
                "repo": metadata.get("repo_name"),
            }
        )
    return sorted(out, key=lambda item: item.get("name", "").lower())


@router.get("/llm/providers", tags=["LLM Provider"])
async def list_llm_provider_configurations(
    user: User = Security(get_current_user, scopes=[])
) -> List[LLMProviderOut]:
    """List configured LLM Provider."""
    out = []
    for provider in connector.collection(Collections.LLM_PROVIDER_CONFIGURATIONS).find(
        {}
    ):
        out.append(LLMProviderOut(**provider))
    return out


@router.post("/llm/providers", tags=["LLM Provider"])
async def create_llm_provider_configuration(
    payload: LLMProviderIn,
    user: User = Security(get_current_user, scopes=["ai_write"]),
) -> LLMProviderOut:
    """Create a new LLM provider configuration."""
    existing = connector.collection(Collections.LLM_PROVIDER_CONFIGURATIONS).find_one(
        {"name": payload.name}
    )
    if existing is not None:
        raise HTTPException(
            400,
            f"LLM provider configuration with name '{payload.name}' already exists.",
        )

    if payload.active:
        other_active = _other_active_config()
        if other_active is not None:
            raise HTTPException(
                400,
                f"Another LLM provider is already enabled ('{other_active['name']}'). "
                f"Only one provider can be enabled at a time — disable it first, or create this one "
                f"disabled and enable it later.",
            )

    plugin_class = _get_plugin_class(payload.plugin)
    trim_space_parameters_fields(payload.parameters)
    plugin = plugin_class(
        payload.name, payload.parameters, {}, None, logger,
        ssl_validation=payload.sslValidation,
    )
    _validate_provider(plugin, payload.parameters)

    doc = LLMProviderDB(
        name=payload.name,
        plugin=payload.plugin,
        parameters=payload.parameters,
        storage=plugin.storage if plugin.storage is not None else {},
        active=payload.active,
        sslValidation=payload.sslValidation,
        updatedAt=datetime.now(timezone.utc),
    )
    connector.collection(Collections.LLM_PROVIDER_CONFIGURATIONS).insert_one(
        doc.model_dump()
    )
    # Ensure the attention-scan schedule is registered (a no-op if it already is — see
    # register_attention_scan_schedule's own existence check). Called on every create, not
    # just the first, so the schedule self-heals if it's ever missing while providers exist.
    register_attention_scan_schedule()
    logger.debug(f"LLM provider configuration '{payload.name}' has been created.")
    return LLMProviderOut(**doc.model_dump())


@router.patch("/llm/providers/{name}", tags=["LLM Provider"])
async def update_llm_provider_configuration(
    payload: LLMProviderUpdate,
    name: str = Path(...),
    user: User = Security(get_current_user, scopes=["ai_write"]),
) -> LLMProviderOut:
    """Update an existing LLM provider configuration."""
    existing = connector.collection(Collections.LLM_PROVIDER_CONFIGURATIONS).find_one(
        {"name": name}
    )
    if existing is None:
        raise HTTPException(
            404, f"LLM provider configuration with name '{name}' does not exist."
        )

    plugin_id = payload.plugin if payload.plugin is not None else existing["plugin"]

    if payload.active is True and not existing.get("active"):
        other_active = _other_active_config(exclude_name=name)
        if other_active is not None:
            raise HTTPException(
                400,
                f"Another LLM provider is already enabled ('{other_active['name']}'). "
                f"Only one provider can be enabled at a time — disable it first.",
            )

    plugin_class = _get_plugin_class(plugin_id)
    parameters = {
        **existing.get("parameters", {}),
        **(payload.parameters or {}),
    }
    trim_space_parameters_fields(parameters)
    ssl_validation = (
        existing.get("sslValidation", True)
        if payload.sslValidation is None
        else payload.sslValidation
    )
    plugin = plugin_class(
        name, parameters, existing.get("storage", {}), None, logger,
        ssl_validation=ssl_validation,
    )
    active = existing.get("active", True) if payload.active is None else payload.active
    if active:
        _validate_provider(plugin, parameters)

    updated = LLMProviderDB(
        name=name,
        plugin=plugin_id,
        parameters=parameters,
        storage=plugin.storage,
        active=active,
        sslValidation=ssl_validation,
        updatedAt=datetime.now(timezone.utc),
    )
    connector.collection(Collections.LLM_PROVIDER_CONFIGURATIONS).update_one(
        {"name": name}, {"$set": updated.model_dump()}
    )
    logger.debug(f"LLM provider configuration '{name}' has been updated.")
    return LLMProviderOut(**updated.model_dump())


@router.delete("/llm/providers/{name}", tags=["LLM Provider"])
async def delete_llm_provider_configuration(
    name: str = Path(...),
    user: User = Security(get_current_user, scopes=["ai_write"]),
):
    """Delete an LLM provider configuration."""
    existing = connector.collection(Collections.LLM_PROVIDER_CONFIGURATIONS).find_one(
        {"name": name}
    )
    if existing is None:
        raise HTTPException(
            404, f"LLM provider configuration with name '{name}' does not exist."
        )
    plugin_class = _get_plugin_class(existing["plugin"])
    plugin = plugin_class(
        name, existing.get("parameters", {}), existing.get("storage", {}), None, logger,
        ssl_validation=existing.get("sslValidation", True),
    )
    plugin.cleanup(existing.get("parameters", {}))
    connector.collection(Collections.LLM_PROVIDER_CONFIGURATIONS).delete_one(
        {"name": name}
    )
    if not llm_provider_configured():
        # That was the last configured provider — remove the schedule entirely so celery beat
        # stops firing a periodic task with nothing left to scan. Re-created on the next create.
        # Shared existence gate (llm_invoke) — same check the scan + feed use, so they can't drift.
        unregister_attention_scan_schedule()
        # Purge existing findings too: unconsumable without a provider, and this guarantees a
        # clean slate (no stale pre-removal findings) when a provider is re-added.
        purge_attention_findings()
    logger.debug(f"LLM provider configuration '{name}' has been deleted.")
    return {"success": True}


@router.post("/llm/providers/validate", tags=["LLM Provider"])
async def validate_llm_provider_configuration(
    payload: LLMProviderValidateIn,
    user: User = Security(get_current_user, scopes=["ai_write"]),
):
    """Validate credentials for an LLM provider plugin without persisting any configuration.

    Instantiates the plugin with the supplied parameters, calls plugin.validate(),
    and returns success/failure.  Useful for a "test credentials" step before save.
    """
    plugin_class = _get_plugin_class(payload.plugin)
    trim_space_parameters_fields(payload.parameters)
    plugin = plugin_class(
        "llm_provider_validation", payload.parameters, {}, None, logger,
        ssl_validation=payload.sslValidation,
    )
    validation_result = _validate_provider(plugin, payload.parameters)
    return {"success": True, "message": validation_result.message}


@router.post(
    "/llm/providers/get_dynamic_fields/{plugin_id}",
    tags=["LLM Provider"],
    description="Get dynamic config fields from an LLM provider plugin based on other fields.",
)
async def get_llm_provider_dynamic_fields(
    plugin_id: str,
    config_details: dict,
    user: User = Security(get_current_user, scopes=["ai_write"]),
):
    """Return model-dependent dynamic fields for an LLM provider plugin.

    Mirrors the per-integration dynamic-fields routes (CRE/CTO/CLS/...) but is
    scoped to ``ai_write`` so LLM provider admins can use it. Delegates to the
    plugin's get_dynamic_fields(), which reads the selected model from the
    supplied parameters (e.g. effort levels depend on the chosen model).
    """
    return get_dynamic_fields_from_plugin(plugin_id, config_details)
