"""Business rule related endpoints."""

import traceback
from datetime import datetime, UTC
from typing import List

from fastapi import APIRouter, HTTPException, Security
from fastapi.responses import JSONResponse

from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User
from netskope.common.utils import Collections, DBConnector, Logger
from netskope.integrations.cfc.models.business_rule import (
    BusinessRuleDB,
    BusinessRuleIn,
    BusinessRuleOut,
    BusinessRulesDelete,
    BusinessRuleTestOut,
    BusinessRuleUpdate,
    BusinessRuleUsedIn,
)
from netskope.integrations.cfc.utils.query import build_mongo_query

router = APIRouter()
logger = Logger()
db_connector = DBConnector()


@router.get(
    "/business_rules",
    tags=["CFC Business Rule"],
    description="Get list of business rules for CFC.",
)
async def get_business_rules(
    _: User = Security(get_current_user, scopes=["cfc_read"])
) -> List[BusinessRuleOut]:
    """Get list of business rules.

    Args:
        _ (User): The authenticated user.

    Raises:
        HTTPException: In case of any errors raises HTTPException error.

    Returns:
        List[BusinessRuleOut]: List of business rules.
    """
    try:
        rules = []
        for rule in db_connector.collection(Collections.CFC_BUSINESS_RULES).find({}):
            rule_obj = BusinessRuleOut(**rule)
            if db_connector.collection(Collections.CFC_SHARING).find_one(
                {"mappings.businessRule": rule_obj.name}
            ):
                rule_obj.mapped = True
            rules.append(rule_obj)

        return list(reversed(rules))
    except Exception as error:
        logger.error(
            "Error occurred while fetching business rules.",
            details=traceback.format_exc(),
        )
        raise HTTPException(
            500, "Error occurred while fetching business rules."
        ) from error


@router.post(
    "/business_rule",
    tags=["CFC Business Rule"],
    description="Create a business rule for CFC."
)
async def create_business_rule(
    rule: BusinessRuleIn,
    user: User = Security(get_current_user, scopes=["cfc_write"]),
) -> BusinessRuleOut:
    """Create a business rule.

    Args:
        rule (BusinessRuleIn): Configuration for business rule to create.
        user (User): The authenticated user.

    Raises:
        HTTPException: In case of any errors raises HTTPException error.

    Returns:
        BusinessRuleOut: Business rule configuration.
    """
    try:
        db_connector.collection(Collections.CFC_BUSINESS_RULES).insert_one(
            {
                **rule.model_dump(),
                **{
                    "updatedBy": user.username,
                    "updatedAt": datetime.now(UTC),
                },
            }
        )

        result: BusinessRuleOut = db_connector.collection(
            Collections.CFC_BUSINESS_RULES
        ).find_one({"name": rule.name})
        logger.debug(f"CFC business rule '{rule.name}' successfully created.")
        return result
    except Exception as error:
        logger.error(
            "Error occurred while creating new business rule.",
            details=traceback.format_exc(),
        )
        raise HTTPException(
            500, "Error occurred while creating new business rule."
        ) from error


@router.patch(
    "/business_rule",
    tags=["CFC Business Rule"],
    description="Update a business rule for CFC."
)
async def update_business_rule(
    rule: BusinessRuleUpdate,
    user: User = Security(get_current_user, scopes=["cfc_write"]),
) -> BusinessRuleOut:
    """Update an existing business rule.

    Args:
        rule (BusinessRuleUpdate): Configuration for business rule to update.
        user (User): The authenticated user.

    Raises:
        HTTPException: In case of any errors raises HTTPException error.

    Returns:
        BusinessRuleOut: Updated business rule configuration.
    """
    try:
        updated_rule = rule.model_dump(exclude_none=True)

        if rule.muted is False:  # set unmutedAt to None if muted is false.
            updated_rule["unmuteAt"] = None
        db_connector.collection(Collections.CFC_BUSINESS_RULES).update_one(
            {"name": rule.name},
            {
                "$set": {
                    **updated_rule,
                    **{
                        "updatedBy": user.username,
                        "updatedAt": datetime.now(UTC),
                    },
                }
            },
        )
        result = db_connector.collection(Collections.CFC_BUSINESS_RULES).find_one(
            {"name": rule.name}
        )
        logger.debug(f"CFC business rule '{rule.name}' updated.")
        return result
    except Exception as error:
        logger.error(
            "Error occurred while updating business rule.",
            details=traceback.format_exc(),
        )
        raise HTTPException(
            500, "Error occurred while updating business rule."
        ) from error


@router.delete(
    "/business_rules",
    tags=["CFC Business Rule"],
    description="Delete business rules for CFC."
)
async def delete_business_rules(
    rules: BusinessRulesDelete,
    _: User = Security(get_current_user, scopes=["cfc_write"]),
):
    """Delete business rules.

    Args:
        rules (BusinessRulesDelete): list of business rules to delete.
        _ (User): The authenticated user.

    Raises:
        HTTPException: In case of any errors raises HTTPException error.

    Returns:
        dict: contains stats of success or failure for each business rules to delete.
    """
    try:
        deleted_rules = []
        delete_failed_rules = []
        for rule in rules.names:
            try:
                if (
                    db_connector.collection(Collections.CFC_BUSINESS_RULES).find_one(
                        {"name": rule}
                    )
                    is None
                ):
                    logger.error(f"No business rule with name '{rule}' exists.")
                    data = {
                        "name": rule,
                        "reason": f"No business rule with name '{rule}' exists.",
                    }
                    delete_failed_rules.append(data)
                else:
                    # Removing Business rule from the mappings
                    db_connector.collection(Collections.CFC_SHARING).update_many(
                        {},
                        {"$pull": {
                            "mappings": {"businessRule": rule}
                        }}
                    )
                    # Removing sharing configuration with empty mappings
                    db_connector.collection(Collections.CFC_SHARING).delete_many(
                        {"mappings": {"$size": 0}}
                    )
                    # Delete business rule
                    db_connector.collection(Collections.CFC_BUSINESS_RULES).delete_one(
                        {"name": rule}
                    )
                    deleted_rules.append(rule)

            except Exception:
                logger.error(
                    f"Error occurred while deleting business rule '{rule}'.",
                    details=traceback.format_exc(),
                )
                data = {
                    "name": rule,
                    "reason": f"Error occurred while deleting business rule '{rule}'.",
                }
                delete_failed_rules.append(data)

        results_str = ", ".join([f"'{result}'" for result in deleted_rules])
        logger.debug(f"Business rules {results_str} has been successfully deleted.")

        response_body = {"success": deleted_rules, "failure": delete_failed_rules}
        return JSONResponse(content=response_body, status_code=207)

    except Exception as error:
        logger.error(
            "Error occurred while deleting business rules.",
            details=traceback.format_exc(),
        )
        raise HTTPException(
            500, "Error occurred while deleting business rules."
        ) from error


@router.get(
    "/business_rule/test",
    tags=["CFC Business Rule"],
    description="Test business rule for CFC."
)
async def test_business_rules(
    rule: str,
    _: User = Security(get_current_user, scopes=["cfc_read"]),
) -> BusinessRuleTestOut:
    """Test business rule.

    Args:
        rule (str): Name of business rule to test.
        _ (User): The authenticated user.

    Raises:
        HTTPException: In case of any errors raises HTTPException error.

    Returns:
        BusinessRuleTestOut: Result size and count of images on which business rule is applied on.
    """
    try:
        rule: BusinessRuleDB = db_connector.collection(
            Collections.CFC_BUSINESS_RULES
        ).find_one({"name": rule})
        if rule is None:
            raise HTTPException(400, f"No business rule with name '{rule}' exists.")
        rule = BusinessRuleDB(**rule)
        query = build_mongo_query(rule=rule)
        # get number of images and size of images on which business rule is applied
        result = db_connector.collection(Collections.CFC_IMAGES_METADATA).aggregate([
            {
                "$match": query
            },
            {
                "$group": {
                    "_id": None,
                    "count": {"$sum": 1},
                    "size": {"$sum": "$fileSize"}
                }
            }
        ], allowDiskUse=True)
        result_list = list(result)
        if not result_list:
            return BusinessRuleTestOut(images_count=0, images_size=0)
        else:
            aggregation_result = result_list[0]
            return BusinessRuleTestOut(
                images_count=aggregation_result.get("count", 0),
                images_size=aggregation_result.get("size", 0)
            )
    except HTTPException as error:
        logger.error(
            f"Error occurred while testing business rule: '{str(error)}'.",
            details=traceback.format_exc(),
        )
        raise error
    except Exception as error:
        logger.error(
            f"Error occurred while testing business rule: '{str(error)}'",
            details=traceback.format_exc(),
        )
        raise HTTPException(
            500, "Error occurred while testing business rule."
        ) from error


@router.get(
    "/business_rule/used_in",
    tags=["CFC Business Rule"],
    description="Get list of sharing business rule is used in for CFC."
)
async def get_business_rule_usage(
    rule: str, _: User = Security(get_current_user, scopes=["cfc_read"])
) -> List[BusinessRuleUsedIn]:
    """Get list of sharing business rule is used in.

    Args:
        rule (str): name of business rule to check mappings for.
        _ (User, optional): The authenticated user.

    Raises:
        HTTPException: In case of any errors raises HTTPException error.

    Returns:
        List[BusinessRuleUsedIn]: List of sharing in which business is used.
    """
    try:
        result = []
        if not db_connector.collection(Collections.CFC_BUSINESS_RULES).find_one(
            {"name": rule},
        ):
            return "Business rule with that name does not exists"
        mappings: list = db_connector.collection(
            Collections.CFC_SHARING
        ).find({"mappings.businessRule": rule})
        for mapping in mappings:
            result.append(
                BusinessRuleUsedIn(
                    **{
                        "sourceConfiguration": mapping["sourceConfiguration"],
                        "destinationConfiguration": mapping["destinationConfiguration"],
                    }
                )
            )

        return result
    except HTTPException as error:
        logger.error(
            f"Error occurred while getting business rule mappings: '{str(error)}'.",
            details=traceback.format_exc(),
        )
        raise error
    except Exception as error:
        logger.error(
            f"Error occurred while getting business rule mappings: '{str(error)}'.",
            details=traceback.format_exc(),
        )
        raise HTTPException(
            500, "Error occurred while getting business rule mappings."
        ) from error
