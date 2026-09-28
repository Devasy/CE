"""CRE Auto-Mapper endpoint — AI-assisted plugin-to-entity field mapping.

The user selects the target platform (CE) entity themselves in the UI and then
triggers the auto-mapper, so the AI only has to map fields — it does NOT choose
the entity. A single LLM call maps every plugin field of the requested entity to
a field on the user-selected platform entity, proposing new platform fields where
no suitable existing field exists.

No record data leaves the deployment: the model is told each field's label, type
and flags, never the values stored in it. The label therefore carries the whole
semantic burden on the platform side, which is why the system prompt spends as
much of itself on how to read a label as on the mapping rules themselves.
"""

import json
import traceback
from typing import Optional

from fastapi import APIRouter, HTTPException, Security

from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User, AIFeature
from netskope.common.utils import (
    Collections,
    DBConnector,
    PluginHelper,
    PrefixedLogger,
    invoke_with_tracking,
    is_platform_enabled,
    plugin_id_to_provider,
    resolve_active_llm_plugin,
)
from netskope.common.utils.llm_provider_plugin_base import LLMProviderError

from ..models import (
    AutoMapperRequest,
    AutoMapperResponse,
    Entity,
    FieldMappingResult,
    FieldMappingResponse,
    NewFieldSpec,
)

router = APIRouter()
connector = DBConnector()
logger = PrefixedLogger("CRE-AUTO-MAPPER")
plugin_helper = PluginHelper()

# Field types the auto-mapper supports. Only these basic, directly-storable types
# are sent to the model and considered for mapping — complex platform types
# (calculated, reference, value/range maps) and specialised string types
# (email, ipv4, ipv6) are excluded by design, both to avoid feeding the model
# unmappable fields and because the UI only creates/maps these basic types.
# Extend this set to support additional types in future.
_MAPPABLE_FIELD_TYPES = frozenset(
    {"string", "number", "list", "boolean", "datetime"}
)

# ---------------------------------------------------------------------------
# Field mapping prompt (the user has already chosen the target entity)
# ---------------------------------------------------------------------------

FIELD_MAPPING_SYSTEM_PROMPT = """You are a field-mapping engine for Netskope Cloud Exchange.
You map the fields of a plugin entity onto the fields of a platform entity the user has already
selected.

Producing the mapping list is your ONLY task and your ONLY output: exactly one entry for every
plugin field you are given, each pointing either at an existing platform field or at a new platform
field you propose. Never return an empty list, never omit a plugin field, and never reply with an
explanation instead of the mappings. Every plugin field can always be mapped — to an existing field
or to a new one — so there is no situation in which the list is empty.

There is NO plugin field you may leave out, whatever the platform entity looks like. In particular:
no existing field has a compatible type; the only similarly-named platform field holds a different
type; the label you would naturally give a new field is already taken by an existing field. None of
these is a reason to omit an entry — each one simply means "create a new field" (STEP 4), giving the
new field a label that is not already in use. Omitting a field is always the wrong answer; the
reviewer would silently lose that data with nothing on screen to tell them.

<platform_field_model>
This is what a platform entity field is, and what you are creating when you propose a new one:

  label             Human-readable text shown in the UI, e.g. "Device Serial Number".
  name              Machine name, derived mechanically from the label: lowercased, with spaces and
                    dots replaced by underscores ("Device Serial Number" -> "device_serial_number").
  type              One of: string | number | list | boolean | datetime.
  unique            true  -> the field is an IDENTITY / MERGE KEY. Records written by DIFFERENT
                             plugin configurations that carry the same value in this field are
                             merged into a single platform record.
                    false -> a plain attribute. Its value is overwritten or appended; it never
                             causes records to merge.
  coalesceStrategy  Append/overwrite behaviour of a non-unique field. Not yours to choose.

Each EXISTING platform field is given to you as
  {"label", "type", "unique", "is_mapped"}
  is_mapped      true  -> another plugin configuration already writes to this field.
                 false -> the field exists but nothing writes to it yet, so it is free to adopt.

"name" is deliberately NOT sent to you, and you never write one: it carries no information the
label does not already carry, being the same text after a mechanical transformation. Refer to every
platform field by its LABEL, exactly as spelled in the input — the system turns labels back into
names for you.
</platform_field_model>

<how_to_read_a_platform_field>
A platform field's LABEL is your only evidence of what it holds.
Use it — but only in its proper place, which is LAST.

Order of reasoning, never varied:
  1. type      — a hard gate (STEP 1). Settles nothing about meaning.
  2. unique    — a hard gate (STEP 3b). Settles nothing about meaning.
  3. label     — decides meaning, among whatever survived 1 and 2.

Each stage can only ever REJECT; none of them can carry a pair on its own.
  - A perfect label match NEVER rescues a type or unique mismatch.
  - Passing the type and unique gates NEVER makes a pair a match. Being type-compatible, unique and
    is_mapped=false makes a field AVAILABLE, not RELEVANT. You still have to ask what it holds.

Judging meaning from a label means asking: would these two fields hold the SAME real-world value?
  - Different wording, same thing -> MATCH. Plugin "Device Name" and platform "Host Name" both hold
    the machine's network name; map them (once the gates pass).
  - Same wording, different thing -> NO MATCH. Judge the value, not the string.
  - GENERIC labels — "ID", "Name", "Key", "Value", "Type", "Identifier", "Reference" — name no
    particular real-world value, so they match NOTHING specific. Never map onto one on the strength
    of its flags. A plugin "Hostname" must NOT be mapped onto a platform field labelled "ID" merely
    because "ID" is unique and unmapped and the types agree; a hostname is not "an ID". Create a new
    field instead.
</how_to_read_a_platform_field>

<decision_procedure>
Apply these steps to EVERY plugin field, in this order.

STEP 1 — Type gate (hard, no exceptions).
  A pair may be mapped only when the plugin field's type and the platform field's type are
  IDENTICAL: string->string, number->number, list->list, boolean->boolean, datetime->datetime.
  Read the plugin field's real shape from its declared type and its description; read the platform
  field's from its declared type.
  CANDIDATES = the existing platform fields that pass this gate. If there are none, go to STEP 4.
  A type mismatch REJECTS THAT CANDIDATE, never the plugin field: it means "do not reuse this one",
  not "skip this plugin field". A plugin field of type string named exactly like a platform field of
  type number still gets its own entry — a NEW string field, labelled so it does not clash with the
  number field (see STEP 4, LABEL MUST BE FREE).

STEP 2 — Classify the plugin field from its description and required flag.
  MERGE KEY          Its description declares, in words, EITHER of these:
                       (a) MERGEABLE — the value can be used to merge, correlate, deduplicate or
                           match records with other plugins/products; or
                       (b) UNIQUE — the value is unique, is a unique identifier, or uniquely
                           identifies the person, device or object the record is about, WITHOUT
                           tying that uniqueness to the plugin's own product or console.
                     ONLY the description establishes either one. A value that merely LOOKS
                     cross-vendor or one-per-record (an email address, a hostname, a serial number)
                     is NOT a merge key unless the description says (a) or (b); and neither the field
                     name nor the required flag can say it for the description.
                     REQUIRED-BUT-MERGEABLE IS THE EXCEPTION THAT MATTERS: when a field is
                     required=true AND its description says it is mergeable or unique, it is a MERGE
                     KEY, not a PLUGIN IDENTIFIER. The description decides; the required flag does
                     not. Such a field takes the ordinary MERGE KEY route in STEP 3 — it MAY and
                     SHOULD be mapped onto a suitable existing unique field. required=true is never
                     a reason to skip STEP 3 and create a new field instead, and it never adds a
                     vendor prefix. Being mergeable is exactly what makes sharing that field correct.
  PLUGIN IDENTIFIER  required=true, and the value identifies the record inside the plugin's own
                     product, with no description wording making it mergeable. Such a value is sent
                     BACK to the plugin whenever that record is updated, so it must stay under this
                     one plugin's control: it must NEVER be unique and must never land in a field
                     another plugin also writes to. If a value produced by a different plugin ever
                     reached this field, the plugin would be handed an identifier it does not
                     recognise and the update would fail.
                     THE WORD "UNIQUE" DOES NOT PROMOTE IT. A description like "unique identifier of
                     the device in the vendor console" states uniqueness INSIDE that one product,
                     which is what a plugin identifier is; criterion (b) above is about a value that
                     is unique to the person/device itself, not to the vendor's own record of it. So
                     a field described as uniquely identifying the record in the plugin's product,
                     console, tenant or API is still a PLUGIN IDENTIFIER and still must not be
                     unique.
  ATTRIBUTE          Everything else — score, confidence, severity, risk, status, name, region,
                     action, version, timestamp, count, ...

STEP 3 — Reuse an existing field, but only when one truly fits.
  Narrow the CANDIDATES in this order — flags first, meaning last:
    a) FLAG GATE. Keep only candidates whose "unique" suits the STEP 2 class:
         MERGE KEY          -> only candidates with unique=true (this applies to a required
                               mergeable field exactly as it does to an optional one)
         PLUGIN IDENTIFIER  -> only candidates with unique=false AND is_mapped=false
         ATTRIBUTE          -> only candidates with unique=false
    b) MEANING GATE. Of those, keep only the ones whose LABEL says they hold the same real-world
       value the plugin field's description says it contains — applying
       <how_to_read_a_platform_field>. Surviving (a) is not evidence for (b); a field that is
       available is not thereby relevant, and a generic label ("ID", "Name", "Key") survives (b)
       for nothing.
  If any candidate survives BOTH, map to it (existing=true), preferring is_mapped=false over
  is_mapped=true: a field nobody writes to yet is meant to be adopted, and creating a near-duplicate
  beside it (a second "Serial Number" when an unused one already exists) is wrong.
  A PLUGIN IDENTIFIER rarely has a surviving candidate and normally falls through to STEP 4; reuse
  one only when a free, not-yet-written field unmistakably holds this same plugin's own identifier.
  If none survives, go to STEP 4.

STEP 4 — Create a new platform field (existing=false). ALWAYS possible, so this is where every
  plugin field that STEP 3 could not place ends up. Never omit the field instead.
  Its type is the plugin field's exact type. Set new_field_unique=true ONLY for a MERGE KEY. A
  PLUGIN IDENTIFIER and every ATTRIBUTE get new_field_unique=false. required=true on its own NEVER
  makes a field unique: a mandatory internal ID and a mandatory score are both non-unique, and only
  a description that says the value can be merged across plugins, or that the value is unique to the
  person/device itself, justifies a unique field.

  LABEL THE NEW FIELD (do this first, then apply LABEL MUST BE FREE below):
  - PLUGIN IDENTIFIER -> prefix the label with the plugin's vendor name, taken from "Plugin" in the
    user message and shortened to the vendor itself ("Netskope Risk Exchange" -> "Netskope",
    "Microsoft Intune" -> "Microsoft Intune"). A plugin field "Device ID" from the Netskope plugin
    becomes "Netskope Device ID". WHY: several products expose an identically-named identifier —
    Netskope and Microsoft Intune both have a "Device ID" — and an unprefixed "Device ID" is a field
    the OTHER plugin would later be mapped onto too, feeding this plugin identifiers from a foreign
    product. The prefix keeps each product's own identifier in its own field.
  - MERGE KEY and ATTRIBUTE -> do NOT prefix. Name the field for the value it holds ("Email",
    "Risk Score"). A merge key must stay vendor-neutral, since its whole purpose is for several
    plugins to write to it.

  LABEL MUST BE FREE:
  A new field's machine name is derived from its label, so a label that repeats the label of a field
  ALREADY on the platform entity cannot be created. That includes a field of a DIFFERENT type — the
  very field STEP 1 rejected. So once you have a label, check it against every existing platform
  label in the input. If it is taken, you MUST still return the entry, and you MUST make the label
  distinct by qualifying it:
    - MERGE KEY -> add a word that names the value more precisely and keep it VENDOR-NEUTRAL
      ("Device Hostname" beside an existing "Hostname"). Never the vendor name: several plugins are
      meant to write to a merge key, which a vendor-named field would prevent.
    - ATTRIBUTE -> prefer a word that names the value more precisely ("Device Hostname" beside an
      existing "Hostname", "Mac Address List" beside an existing "Mac Address"). Where the value is
      really this one product's own measure of something — a score, a rating, a status — the vendor
      name is the clearest qualifier and is allowed HERE ONLY, to break the collision
      ("Netskope Risk Score" beside an existing "Risk Score"). An attribute is not a merge key, so
      naming it after the product costs nothing.
    - PLUGIN IDENTIFIER -> the vendor prefix normally makes it distinct already; if even that is
      taken, add a further qualifying word ("Netskope Asset Device ID").
  Pick a qualifier a reviewer would recognise as describing the value. Never reuse the taken label,
  never point the plugin field at the same-labelled field of the wrong type, and never drop the field
  because its natural label is in use.
</decision_procedure>

<hard_rules>
1. Output exactly ONE entry for EVERY plugin field listed — no omissions, no empty list. No plugin
   field may be skipped for ANY reason: not a type that matches nothing, not a natural label that is
   already taken, not a field you judge unmappable. Each of those is a STEP 4 new field.
2. Never map across differing types, however similar the names are. A same-named platform field of a
   different type means "create a new field under a different label", never "omit this field".
3. A platform field MUST NOT be used as a destination more than once across the whole response.
4. Never map an ATTRIBUTE to a platform field with unique=true, and never set new_field_unique=true
   for an ATTRIBUTE.
5. Never map a MERGE KEY to a platform field with unique=false — create a new unique field instead.
   A field is a MERGE KEY when its description says the value is mergeable across products OR that
   the value is unique to the person/device it describes; nothing else makes one.
6. Never treat a PLUGIN IDENTIFIER as unique: never map it to a platform field with unique=true, and
   never set new_field_unique=true for it. A MERGE KEY is the only class that may be unique.
7. For a score, confidence, severity, risk or similar metric, always create a new field
   (existing=false, new_field_unique=false); never reuse an existing one.
8. Never map onto a platform field whose label is generic ("ID", "Name", "Key", "Value", "Type",
   "Identifier", "Reference"). Its flags cannot make it a match — create a new field.
9. Every new field created for a PLUGIN IDENTIFIER carries the vendor prefix. A MERGE KEY NEVER
   does. An ATTRIBUTE does not either, with one exception: when its natural label is already taken
   and the value is that product's own measure (a score, rating, status), the vendor name may be
   used as the qualifier that makes the label distinct.
10. required=true never blocks reuse. A required field whose description says it is mergeable is a
    MERGE KEY and must be mapped onto a suitable existing unique field when one exists — do not
    create a new field for it, and do not prefix it.
11. A new field's label MUST NOT repeat the label of any field already on the platform entity, even
    one of a different type — add a prefix or suffix that keeps it distinct.
</hard_rules>

<examples>
Plugin "Mac Addresses" (list; "List of MAC addresses of the device") against platform "MAC Address"
(string): the types differ, so the near-identical name counts for nothing — create a NEW list field,
destination "MAC Addresses", new_field_unique=false.

Plugin "Email" (string, required; "User's email address, can be used to correlate users across
products") against platform "User Email" (string, unique=true): required AND mergeable, so it is a
MERGE KEY, and the label says that field holds email addresses — existing=true, destination
"User Email". Do NOT create a new "<Vendor> Email" for it: required only prefixes and blocks reuse
when the description is silent about merging, and here it is not.

The same plugin "Email" when the platform has no unique string field labelled for email addresses:
create a NEW string field, destination "Email", with new_field_unique=true.

Plugin "Device Name" (string; "Name of the device on the network") against platform "Host Name"
(string, unique=false): the labels are worded differently but name the same real-world value, and
both gates pass — existing=true, destination "Host Name". Different wording is not a mismatch.

Plugin "Hostname" (string; "Hostname of the device, can be used to merge this device with records
from other plugins") against a platform entity whose only unique string field is labelled "ID"
(is_mapped=false): a MERGE KEY, and "ID" passes both hard gates — yet "ID" names no particular
value, so it does not hold a hostname. Do NOT map onto it just because it is unique and free.
Create a NEW string field, destination "Hostname", with new_field_unique=true.

Plugin "Device ID" (string, required; "Identifier of the device in the vendor console", nothing
about merging) from the plugin "Netskope Risk Exchange", against platform "Vendor Device Id"
(string, unique=true, is_mapped=false): a PLUGIN IDENTIFIER, so that unique field is off limits
however well it matches. Create a NEW string field, destination "Netskope Device ID",
new_field_unique=false — prefixed, because Microsoft Intune also has a "Device ID" and an
unprefixed field would end up collecting both products' identifiers.

Plugin "Risk Score" (number, required): an ATTRIBUTE metric — create a NEW number field with
new_field_unique=false, even when a "Risk Score" field already exists on the platform. Because that
label is taken, the new one must be distinct: destination and new_field_label "Netskope Risk Score"
(the vendor name is the right qualifier for a score, which is that product's own rating). When no
"Risk Score" exists yet, the plain "Risk Score" is correct.

Plugin "Hostname" (string; "Hostname of the device") against a platform entity whose only "Hostname"
field is a NUMBER: the type gate rejects that field, so there is nothing to reuse — but the entry is
still REQUIRED. It is an ATTRIBUTE, so create a NEW string field under a label that is not already
taken and is not vendor-prefixed: destination and new_field_label "Device Hostname", new_field_type
"string", new_field_unique=false. Never answer with the bare "Hostname" (its name would collide with
the number field), never map onto the number field, and never leave the field out of the list.

Plugin "Serial Number" (string; "Unique serial number of the device", nothing about merging) against
a platform entity with no unique string field labelled for serial numbers: the description calls the
value unique and the uniqueness belongs to the device rather than to the vendor's console, so it is a
MERGE KEY — create a NEW string field, destination "Serial Number", new_field_unique=true.

Plugin "Asset ID" (string, required; "Unique identifier of the asset in the vendor console"): the
description says "unique", but the uniqueness is inside the vendor's own product, so this is a PLUGIN
IDENTIFIER, NOT a merge key — create a NEW string field, destination "<Vendor> Asset ID", with
new_field_unique=false.
</examples>

<reason_style>
"reason" is read by the person reviewing the suggestion, who is often NOT technical. Write one or
two short, plain sentences addressed directly to them: a recommendation about what will happen and
why, never narration of your own reasoning and never a restatement of the input.

  - Use everyday language. Do not use the vocabulary of these instructions — no "type", "type-
    compatible", "unique=true", "is_mapped", "merge key", "label", "STEP 3", "candidate", "schema",
    "gate". Describe the substance instead: "both hold a list of MAC addresses".
  - Name a platform field by its label, exactly as given — that is what the reviewer sees on screen.
  - Say what the data will do: which field it will go into, or that a new field is being created
    because the entity has nothing suitable to hold it yet.
  - When you propose a unique field, or map to one, explain in plain words that the value identifies
    the same person or device in other products, so information about it can be brought together
    into one record.

  Prefer: "Both fields hold a list of MAC addresses, so this data will go into the existing
          MAC Addresses field."
  Avoid:  "Field types match (list) and both labels refer to MAC addresses."
  Prefer: "This email address identifies the same user in other products, so creating it as a unique
          field lets information about that user be combined into one record."
  Avoid:  "The description says this field can be used to merge records with other plugins."
  Prefer: "Nothing on this entity currently stores a risk score, so a new field is created for it."
  Avoid:  "No type-compatible candidate remained after STEP 3, so a new field is created per rule 6."
  Prefer: "The new field is named after Netskope so this device identifier stays separate from the
          one another product uses."
  Avoid:  "Prefixed with the vendor name per the PLUGIN IDENTIFIER labelling rule."
</reason_style>

<output_contract>
- source: the plugin field NAME exactly as provided. Plugin fields are identified by name, platform
  fields by label — do not swap the two.
- EXISTING platform field: existing=true, destination=<that field's label, copied exactly from the
  input>, new_field_label="" (empty string), new_field_type="" (empty string),
  new_field_unique=false.
- NEW platform field: existing=false, destination=<the label you propose for the new field>,
  new_field_label=<the same label>, new_field_type=<the plugin field's exact type: string, number,
  list, boolean or datetime>, new_field_unique=<per STEP 4>. The proposed label MUST NOT be the label
  of any existing platform field listed in the input, of any type — qualify it (see STEP 4, LABEL
  MUST BE FREE) rather than reusing it.
- Never invent or emit a machine name (lowercase_with_underscores). Labels only, on both sides of
  every destination.
</output_contract>
"""

FIELD_MAPPING_HUMAN_TEMPLATE = """Map the following plugin entity fields to the selected platform entity's fields.

Plugin: {plugin_name}
Plugin Entity: {plugin_entity_name}
Plugin Fields:
<plugin_fields>
{plugin_fields_json}
</plugin_fields>

Selected Platform Entity: {platform_entity_name}
Existing Platform Entity Fields:
<platform_entity>
{platform_entity_json}
</platform_entity>

Now output the mappings. Every plugin field listed above MUST appear exactly once in your
response — map it to an existing platform field where one is compatible, otherwise create a
new field. Do not reply with an explanation; reply only with the field mappings.
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_mapped_field_names(
    entity_name: str, exclude_configuration: Optional[str]
) -> set:
    """Names of ``entity_name`` fields already mapped by a plugin configuration.

    A platform field counts as mapped when some plugin configuration maps this
    entity (``mappedEntities.destination == entity_name``) and lists the field as
    a destination in that entity's ``fields``. The configuration currently being
    edited (``exclude_configuration``) is skipped so its own saved mappings do not
    make the auto-mapper treat the fields it already maps as in use by another
    plugin — otherwise re-mapping an existing configuration would needlessly
    create duplicate fields.
    """
    query = {"mappedEntities.destination": entity_name}
    if exclude_configuration:
        query["name"] = {"$ne": exclude_configuration}
    mapped_names = set()
    for config in connector.collection(Collections.CREV2_CONFIGURATIONS).find(
        query, {"mappedEntities": 1}
    ):
        for mapped_entity in config.get("mappedEntities", []):
            # A single config can map several entities; only count the fields
            # under the entity we are mapping to (dot-notation queries match
            # across array elements, so re-check the destination here).
            if mapped_entity.get("destination") != entity_name:
                continue
            for mapped_field in mapped_entity.get("fields", []):
                destination = mapped_field.get("destination")
                if destination:
                    mapped_names.add(destination)
    return mapped_names


def _get_platform_entity_fields(
    entity_name: str, exclude_configuration: Optional[str] = None
) -> Optional[dict]:
    """Return one CE entity's mappable fields, as the auto-mapper describes them.

    No record values are read. Sample values were considered as extra evidence of
    what a field really holds, but sending real customer data (which is routinely
    PII: emails, hostnames, user names) to a third-party LLM is not something this
    feature should do, so the model judges meaning from the field label instead —
    see the ``<how_to_read_a_platform_field>`` section of the system prompt.
    """
    entity_doc = connector.collection(Collections.CREV2_ENTITIES).find_one(
        {"name": entity_name}
    )
    if entity_doc is None:
        return None
    entity = Entity(**entity_doc)
    mapped_field_names = _get_mapped_field_names(entity.name, exclude_configuration)
    fields = []
    for field in entity.fields:
        # Only surface basic, mappable field types.
        if field.type not in _MAPPABLE_FIELD_TYPES:
            continue
        fields.append(
            {
                # "name" is kept for internal use (type lookups, label -> name
                # resolution) but is projected out before the payload reaches the
                # model — see _platform_fields_for_prompt. The model works in
                # labels only, since a field's name is just its label lowercased
                # with spaces/dots turned into underscores.
                "name": field.name,
                "label": field.label,
                "type": field.type,
                "unique": field.unique,
                # True when an existing plugin configuration already maps to this
                # field. Lets the model reuse an existing-but-unused field instead
                # of creating a near-duplicate (see FIELD_MAPPING_SYSTEM_PROMPT).
                "is_mapped": field.name in mapped_field_names,
            }
        )
    return {"name": entity.name, "fields": fields}


def _plugin_display_name(plugin_class, plugin_id: str) -> str:
    """Vendor-facing name of a CRE plugin, for prefixing new field labels.

    Two plugins routinely expose an identically-named identifier (both Netskope
    and Microsoft Intune have a "Device ID"), so a new field created for one must
    not be a name the other would map onto as well. The manifest name is the
    source of truth; it falls back to the plugin's module segment
    ("netskope_ztre" -> "Netskope Ztre") when no manifest is loaded.
    """
    metadata = getattr(plugin_class, "metadata", None)
    name = metadata.get("name") if isinstance(metadata, dict) else None
    if isinstance(name, str) and name.strip():
        return name.strip()
    segment = plugin_id.split(".")[-2] if "." in plugin_id else plugin_id
    return segment.replace("_", " ").title()


def _platform_fields_for_prompt(entity_data: dict) -> dict:
    """Project the internal platform-entity dict down to what the model sees.

    The model works purely in labels — a field's machine name is derived from its
    label, so sending both would duplicate every field name for no added meaning.
    ``name`` is dropped here and re-attached deterministically by
    ``_resolve_destination`` when the model's answer comes back.
    """
    return {
        "name": entity_data["name"],
        "fields": [
            {k: v for k, v in field.items() if k != "name"}
            for field in entity_data["fields"]
        ],
    }


def _derive_field_name(label: str) -> str:
    """Machine name for a field label, mirroring ``EntityFieldIn.validate_name``.

    Kept in lockstep with that validator: the platform derives a new field's name
    from its label on create, so the destination proposed here must match what the
    field will actually be called once the user creates it.
    """
    return ("_".join((label or "").strip().lower().split(" "))).replace(".", "_")


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.post(
    "/auto-mapper",
    response_model=AutoMapperResponse,
    tags=["CREv2 Auto-Mapper"],
    description="AI-assisted mapping of a plugin entity's fields to a user-selected platform entity.",
)
async def auto_map_entity(
    payload: AutoMapperRequest,
    user: User = Security(get_current_user, scopes=["cre_write"]),
) -> AutoMapperResponse:
    """Map the plugin entity's fields to the user-selected platform entity via one LLM call."""
    # skip LLM call if module is disabled
    if not is_platform_enabled("cre"):
        logger.warn(
            f"Rejected Auto-Mapper request from '{user.username}': "
            "the CRE module is disabled."
        )
        raise HTTPException(
            400,
            "Integration cre is disabled. Enable the Cloud Risk Exchange module "
            "before using the AI Auto-Mapper.",
        )

    logger.debug(
        f"Request from '{user.username}': plugin '{payload.plugin}', "
        f"plugin_entity '{payload.entity}', platform_entity '{payload.destination}'."
    )

    provider_doc, llm_plugin = resolve_active_llm_plugin(logger)

    cre_plugin_class = plugin_helper.find_by_id(payload.plugin)
    if not cre_plugin_class:
        raise HTTPException(404, f"Plugin '{payload.plugin}' not found.")

    try:
        cre_plugin = cre_plugin_class(None, payload.parameters, {}, None, logger)
        plugin_entities = cre_plugin.get_entities()
    except Exception:
        logger.error(
            f"Failed to retrieve entities from plugin '{payload.plugin}'.",
            details=traceback.format_exc(),
        )
        raise HTTPException(
            422, f"Failed to retrieve entities from plugin '{payload.plugin}'."
        )

    plugin_entity = next(
        (e for e in plugin_entities if e.name == payload.entity), None
    )
    if plugin_entity is None:
        raise HTTPException(
            404,
            f"Entity '{payload.entity}' not found in plugin '{payload.plugin}'.",
        )

    # Incremental re-mapping: drop fields the user has already committed to
    # (locked rows in the UI) so only the remaining fields are re-suggested.
    locked_sources = set(payload.locked_sources or [])
    plugin_fields_data = [
        {
            # Unlike a platform field, a plugin field's name is NOT derived from
            # its label — the plugin author sets them independently, and the name
            # is the key every mapping's "source" must match. So the name is what
            # travels, and the label (which plugins almost always leave unset, or
            # set equal to the name) is dropped.
            "name": f.name,
            "type": f.type,
            "required": f.required,
            **({"description": f.description} if f.description else {}),
        }
        for f in plugin_entity.fields
        if f.type in _MAPPABLE_FIELD_TYPES and f.name not in locked_sources
    ]
    logger.debug(
        f"Plugin entity '{payload.entity}' has {len(plugin_fields_data)} "
        f"field(s) to suggest ({len(locked_sources)} locked)."
    )

    # Nothing left to suggest — every mappable field is locked. Return an empty
    # result WITHOUT calling the LLM; the UI turns this into an info toast.
    if not plugin_fields_data:
        logger.debug(
            f"No fields to suggest for entity '{payload.entity}'; "
            "all mappable fields are locked. Skipping LLM call."
        )
        return AutoMapperResponse(
            destination=payload.destination,
            reason=(
                "All fields are already mapped or reviewed. "
                "Clear a mapping to get new suggestions."
            ),
            fields=[],
        )

    selected_entity_data = _get_platform_entity_fields(
        payload.destination, exclude_configuration=payload.configuration_name
    )
    if selected_entity_data is None:
        raise HTTPException(
            404,
            f"Platform entity '{payload.destination}' not found. "
            "Create or select an existing entity before using Auto-Mapper.",
        )
    logger.debug(
        f"Platform entity '{payload.destination}' has "
        f"{len(selected_entity_data['fields'])} fields."
    )

    tracking_kwargs = dict(
        feature=AIFeature.CRE_AUTO_MAPPER,
        username=user.username,
        provider_config=provider_doc["name"],
        provider=plugin_id_to_provider(provider_doc["plugin"]),
        model=provider_doc.get("parameters", {}).get("model"),
        feature_metadata={
            "plugin": payload.plugin,
            "entity": payload.entity,
            "destination": payload.destination,
        },
    )

    human_prompt = FIELD_MAPPING_HUMAN_TEMPLATE.format(
        plugin_name=_plugin_display_name(cre_plugin_class, payload.plugin),
        plugin_entity_name=plugin_entity.name,
        plugin_fields_json=json.dumps(plugin_fields_data, indent=2),
        platform_entity_name=payload.destination,
        platform_entity_json=json.dumps(
            _platform_fields_for_prompt(selected_entity_data), indent=2
        ),
    )

    # Bind the schema as a FORCED tool and invoke RAW (no response_model). This mirrors
    # the working curl exactly and, crucially, lets us inspect what the model actually
    # returns: with response_model set, invoke_with_tracking parses inside itself and a
    # validation error (empty tool args) triggers 4 retries before we ever see the body.
    #
    # A mapping entry is ~80 output tokens; an entity with many fields easily exceeds the
    # provider's default max_tokens. When the cap is hit the forced tool call is truncated
    # mid-JSON and arrives as empty args (stop_reason=max_tokens), so give it ample room.
    _MAPPER_MAX_TOKENS = 16384
    try:
        base_runnable = llm_plugin.get_runnable({"max_tokens": _MAPPER_MAX_TOKENS})
        # Belt-and-suspenders: also set it directly in case get_runnable did not honor the
        # runtime_config override (the deployed provider truncated at ~1000 tokens).
        base_runnable.max_tokens = _MAPPER_MAX_TOKENS
        runnable = base_runnable.bind_tools(
            [FieldMappingResponse],
            tool_choice="FieldMappingResponse",
        )
    except Exception:
        logger.error(
            "Failed to build the LLM runnable.",
            details=traceback.format_exc(),
        )
        raise HTTPException(400, "Error initializing LLM provider plugin.")

    try:
        raw = await invoke_with_tracking(
            llm_plugin,
            runnable,
            [
                ("system", FIELD_MAPPING_SYSTEM_PROMPT),
                ("human", human_prompt),
            ],
            **tracking_kwargs,
        )
    except LLMProviderError:
        # Classified provider/model failure (rate limit, overload, auth, ...) —
        # let it bubble to the global LLMProviderError handler, which maps it to
        # the right status and message. Don't mask it as a generic 502.
        logger.error(
            "LLM provider returned a classified error.",
            details=traceback.format_exc(),
        )
        raise
    except Exception:
        logger.error(
            "Unexpected error invoking the LLM provider.",
            details=traceback.format_exc(),
        )
        raise HTTPException(
            502,
            "Error contacting the AI provider. Try again or map fields manually.",
        )

    tool_calls = getattr(raw, "tool_calls", None) or []
    if len(tool_calls) > 1:
        logger.warn(
            f"LLM returned {len(tool_calls)} tool calls; "
            "only the first is processed."
        )

    stop_reason = (getattr(raw, "response_metadata", {}) or {}).get("stop_reason")
    tool_args = tool_calls[0].get("args", {}) if tool_calls else {}
    if not tool_args.get("fields"):
        logger.error(
            "LLM returned no field mappings.",
            details=(
                f"stop_reason={stop_reason} "
                f"tool_calls={json.dumps(tool_calls, default=str)[:2000]}"
            ),
        )
        if stop_reason == "max_tokens":
            raise HTTPException(
                422,
                "The AI response was truncated before the mapping was complete "
                "(model token limit reached). Map this entity manually, or retry "
                "with an entity that has fewer fields.",
            )
        raise HTTPException(
            422,
            "The LLM returned no field mappings. Try again or map fields manually.",
        )

    try:
        mapping = FieldMappingResponse(**tool_args)
    except Exception as exc:
        logger.error(
            "Could not parse field mappings from tool call.",
            details=f"{exc} | args={json.dumps(tool_args, default=str)[:2000]}",
        )
        raise HTTPException(
            422,
            "The LLM returned malformed field mappings. Try again or map fields manually.",
        )

    logger.debug(
        f"LLM returned {len(mapping.fields)} field mapping(s) "
        f"for {len(plugin_fields_data)} plugin field(s)."
    )

    # Deterministic guards over the model output. The prompt forbids all of
    # these, but an LLM cannot be trusted to always comply — enforce them here
    # so an invalid suggestion can never reach the UI:
    #   - source must be a known plugin field
    #   - the label the model answered with is turned back into a machine name
    #     (existing field -> its stored name; new field -> derived from the label)
    #   - a "new" field whose proposed name already exists is reclassified as an
    #     existing-field mapping, so it gets the same existence/type validation
    #   - existing=true destinations must exist on the platform entity and
    #     have the exact same type as the plugin field
    #   - new fields are forced to the plugin field's type
    #   - a destination may be used only once (keep the first occurrence),
    #     including destinations already occupied by the user's locked rows
    plugin_type_by_name = {f["name"]: f["type"] for f in plugin_fields_data}
    plugin_required_by_name = {
        f["name"]: bool(f.get("required")) for f in plugin_fields_data
    }
    platform_type_by_name = {
        f["name"]: f["type"] for f in selected_entity_data["fields"]
    }
    # Built from the stored fields rather than by re-deriving each label, so a
    # legacy field whose name does not match its label still resolves correctly.
    platform_name_by_label = {
        f["label"]: f["name"] for f in selected_entity_data["fields"]
    }
    validated_fields = []
    # Seed with the locked destinations so a fresh suggestion can never reuse a
    # platform field the user is already keeping on a committed row.
    used_destinations = set(payload.locked_destinations or [])
    for f in mapping.fields:
        plugin_type = plugin_type_by_name.get(f.source)
        if plugin_type is None:
            logger.warn(
                f"Dropped mapping for unknown plugin field "
                f"'{f.source}'."
            )
            continue
        # The model answers in labels; everything below this point works in
        # machine names. An existing field resolves through the entity's stored
        # label -> name map; a new field's name is derived from its label exactly
        # as the platform will derive it when the user creates the field. A model
        # that answered with the machine name anyway still resolves, since that
        # name is already a key of platform_type_by_name.
        if f.existing and f.destination not in platform_type_by_name:
            resolved = platform_name_by_label.get(f.destination)
            if resolved is None:
                logger.warn(
                    f"Dropped mapping '{f.source}' -> '{f.destination}': "
                    f"destination marked existing but no field with that label "
                    f"exists on entity '{payload.destination}'."
                )
                continue
            f.destination = resolved
        elif not f.existing:
            derived = _derive_field_name(f.new_field_label or f.destination)
            if not derived:
                logger.warn(
                    f"Dropped new-field mapping for '{f.source}': no usable "
                    "label to derive a field name from."
                )
                continue
            f.destination = derived

        # The model can propose a NEW field under a name that already exists on
        # the entity (e.g. a new "identifier" for a number field when a string
        # "identifier" is already there). Creating it would fail with "Field name
        # is not unique", and leaving it as a new field skips the type check
        # entirely — silently mapping a number onto an existing string field.
        # Reclassify it so it runs the existence/type validation below, which
        # keeps it when the types agree and drops it when they do not.
        if not f.existing and f.destination in platform_type_by_name:
            logger.warn(
                f"Reclassified proposed new field '{f.destination}' "
                f"(source '{f.source}') as an existing field: a field with that "
                f"name already exists on entity '{payload.destination}'."
            )
            f.existing = True
        if f.existing:
            # Resolution above guarantees the destination is a real field name:
            # it either already was one, came out of the label -> name map, or
            # was reclassified precisely because it collided with one.
            platform_type = platform_type_by_name[f.destination]
            if platform_type != plugin_type:
                logger.warn(
                    f"Dropped type-incompatible mapping "
                    f"'{f.source}' ({plugin_type}) -> '{f.destination}' "
                    f"({platform_type})."
                )
                continue
        elif f.new_field_type != plugin_type:
            logger.warn(
                f"Corrected new field '{f.destination}' type "
                f"'{f.new_field_type}' to plugin field type '{plugin_type}'."
            )
            f.new_field_type = plugin_type
        if f.destination in used_destinations:
            logger.warn(
                f"Dropped duplicate destination "
                f"'{f.destination}' (source '{f.source}')."
            )
            continue
        used_destinations.add(f.destination)
        validated_fields.append(f)

    # Surface the plugin's required fields first in the UI — they are the ones the
    # user must get right before the mapping can be saved. The sort is stable, so
    # within each group the model's own ordering is preserved.
    validated_fields.sort(
        key=lambda f: not plugin_required_by_name.get(f.source, False)
    )

    converted_fields = [
        FieldMappingResult(
            source=f.source,
            destination=f.destination,
            existing=f.existing,
            new_field=(
                NewFieldSpec(
                    label=f.new_field_label,
                    type=f.new_field_type,
                    unique=f.new_field_unique,
                )
                if (not f.existing and f.new_field_label and f.new_field_type)
                else None
            ),
            reason=f.reason,
        )
        for f in validated_fields
    ]
    new_field_count = sum(1 for f in converted_fields if f.new_field is not None)
    # The one operation-level line: an AI call was made on the user's behalf and
    # this is what it produced. Everything finer-grained is a debug log.
    logger.info(
        f"Mapped plugin entity '{payload.entity}' to platform entity "
        f"'{payload.destination}' for user '{user.username}': "
        f"{len(converted_fields)} mapping(s), "
        f"{new_field_count} new platform field(s) suggested."
    )

    # The LLM no longer returns a top-level summary (removing it stopped the model
    # from explaining instead of mapping); synthesise one for the API response.
    summary = f"Mapped {len(converted_fields)} plugin field(s) to '{payload.destination}'."
    if new_field_count:
        summary += f" Suggested {new_field_count} new platform field(s)."

    return AutoMapperResponse(
        destination=payload.destination,
        reason=summary,
        fields=converted_fields,
    )
