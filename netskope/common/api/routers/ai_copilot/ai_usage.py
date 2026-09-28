"""AI usage telemetry endpoints — token metrics dashboard and audit trail."""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Security

from netskope.common.api.routers.auth import get_current_user
from netskope.common.models import User
from netskope.common.models.ai_copilot.ai_usage import AIFeature, AIProvider
from netskope.common.utils import Collections, DBConnector, PrefixedLogger

router = APIRouter()
connector = DBConnector()
logger = PrefixedLogger("[AI-COPILOT]")

_FEATURE_LABELS: dict[str, str] = {
    AIFeature.POSTURE_ASSESSMENT: "Posture Assessment",
    AIFeature.COPILOT: "AI Copilot Bot",
    AIFeature.CONFIGURATION_COPILOT: "Configuration Copilot",
    AIFeature.CRE_AUTO_MAPPER: "CRE Automapper",
}


def _feature_label(key: str) -> str:
    return _FEATURE_LABELS.get(key, key.replace("_", " ").title())


# Anthropic list prices ($ per 1M tokens), matched by model-id substring. Used to turn token counts
# into a $-spend estimate in the dashboard — computed server-side (no cost is stored on the record),
# so historical rows get costed too. Source: Anthropic model overview (list as of 2026-09-07):
# Opus 4.8/5 $5/$25 · Sonnet 5 $2/$10 · Haiku 4.5 $1/$5. Sonnet's $2/$10 was originally an
# introductory rate through 2026-08-31; Anthropic made it the standard price and cancelled the
# scheduled increase back to $3/$15, so the sonnet tier is priced at $2/$10 from here on.
_MODEL_PRICING = {
    "opus": (5.0, 25.0),
    "sonnet": (2.0, 10.0),
    "haiku": (1.0, 5.0),
}
_DEFAULT_PRICE = (2.0, 10.0)  # unknown/custom model → sonnet-tier estimate


def _price_for(model: Optional[str], provider: Optional[str] = None) -> tuple:
    """Return (input, output) $/1M-token prices for a model, gated by provider.

    The _MODEL_PRICING substring table (and its sonnet-tier _DEFAULT_PRICE fallback)
    is Anthropic-only pricing. Applying it regardless of provider would mis-price any
    non-Anthropic model whose id happens to contain "opus"/"sonnet"/"haiku" as a
    substring, and would silently apply Anthropic rates to EVERY OpenAI/Gemini model
    (we have no price table for them). A false Anthropic price is worse than 0, so any
    KNOWN non-Anthropic provider returns (0.0, 0.0) — cost is simply not estimated for it.

    A MISSING/None provider is treated as Anthropic: before multi-provider support Anthropic
    was the only provider, so historical usage rows (written without a `provider` field) are
    Anthropic and must keep their cost estimate rather than silently dropping to 0.
    """
    # Normalise before comparing: a record may store the provider as the AIProvider enum OR a raw
    # string, and historical rows written before plugin_id_to_provider() may have mixed casing
    # ("Anthropic"). Compare case-insensitively so an Anthropic row is never mis-priced to 0.0.
    if provider is not None:
        provider_norm = getattr(provider, "value", provider)
        if str(provider_norm).lower() != AIProvider.ANTHROPIC.value:
            return (0.0, 0.0)
    m = (model or "").lower()
    for key, price in _MODEL_PRICING.items():
        if key in m:
            return price
    return _DEFAULT_PRICE


def _estimate_cost(
    model: Optional[str], input_tokens: int, output_tokens: int, provider: Optional[str] = None
) -> float:
    in_price, out_price = _price_for(model, provider)
    return round((input_tokens / 1e6) * in_price + (output_tokens / 1e6) * out_price, 4)


# Excludes feedback-only stub docs (created by POST /copilot/config/feedback when a turn's
# real usage record was never persisted). They carry no tokens/status and must not skew token
# rollups or appear as audit turns — but they MUST still count in the feedback facet: keeping
# a rating whose turn errored is the stub's whole purpose.
_EXCLUDE_STUBS = {"$match": {"feedbackStub": {"$ne": True}}}


def _build_match_filter(
    feature: Optional[str],
    username: Optional[str],
    start: Optional[datetime],
    end: Optional[datetime],
) -> dict:
    match: dict = {}
    if feature:
        match["feature"] = feature
    if username:
        match["username"] = username
    if start or end:
        ts_filter: dict = {}
        if start:
            ts_filter["$gte"] = start
        if end:
            ts_filter["$lte"] = end
        match["timestamp"] = ts_filter
    return match


@router.get(
    "/ai/usage/metrics",
    tags=["AI Usage"],
    description="Aggregated token usage grouped by feature and by user-feature.",
)
async def get_ai_usage_metrics(
    feature: Optional[str] = Query(None, description="Filter by AI feature key"),
    username: Optional[str] = Query(None, description="Filter by username"),
    start: Optional[datetime] = Query(None, description="Start of time range (ISO 8601)"),
    end: Optional[datetime] = Query(None, description="End of time range (ISO 8601)"),
    user: User = Security(get_current_user, scopes=["ai_read"]),
) -> dict:
    """Return token usage aggregated by feature and by top 100 users (per feature)."""
    match_filter = _build_match_filter(feature, username, start, end)
    pipeline = [
        *([{"$match": match_filter}] if match_filter else []),
        {
            "$facet": {
                "byFeature": [
                    _EXCLUDE_STUBS,
                    {
                        "$group": {
                            "_id": "$feature",
                            "inputTokens": {"$sum": "$inputTokens"},
                            "outputTokens": {"$sum": "$outputTokens"},
                            "totalTokens": {"$sum": "$totalTokens"},
                            "requests": {"$sum": 1},
                            "avgRoundTripMs": {"$avg": "$roundTripMs"},
                        }
                    }
                ],
                # Daily token/turn/cost trend per feature (usage-over-time area chart). Grouped by
                # day+feature+model+provider so per-day $ cost can be estimated server-side (cost
                # depends on the model AND provider — pricing is provider-gated, see _price_for);
                # the assembly re-aggregates to day+feature.
                "byDay": [
                    _EXCLUDE_STUBS,
                    {
                        "$group": {
                            "_id": {
                                "day": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}},
                                "feature": "$feature",
                                "model": {"$ifNull": ["$model", "unknown"]},
                                "provider": "$provider",
                            },
                            "totalTokens": {"$sum": "$totalTokens"},
                            "inputTokens": {"$sum": "$inputTokens"},
                            "outputTokens": {"$sum": "$outputTokens"},
                            "turns": {"$sum": 1},
                        }
                    },
                    {"$sort": {"_id.day": 1}},
                ],
                # Per-model token split (model donut + $ estimate). Grouped by model+provider so
                # pricing is never applied across a provider boundary (two providers could collide
                # on the same model-id string), then COLLAPSED back to one row per model in Python
                # (see the by_model loop) so the response shape the UI consumes is unchanged.
                "byModel": [
                    _EXCLUDE_STUBS,
                    {
                        "$group": {
                            "_id": {
                                "model": {"$ifNull": ["$model", "unknown"]},
                                "provider": "$provider",
                            },
                            "inputTokens": {"$sum": "$inputTokens"},
                            "outputTokens": {"$sum": "$outputTokens"},
                            "totalTokens": {"$sum": "$totalTokens"},
                            "turns": {"$sum": 1},
                        }
                    },
                    # Stable order (a $group returns arbitrary order) so the donut's slices/legend
                    # are deterministic across refreshes — the UI also sorts, this keeps the two aligned.
                    {"$sort": {"totalTokens": -1}},
                ],
                # Top error types (reliability panel).
                "byErrorType": [
                    {"$match": {"errorType": {"$nin": [None, ""]}}},
                    {"$group": {"_id": "$errorType", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}},
                    {"$limit": 8},
                ],
                # Thumbs feedback counts (👍 rate KPI).
                # Deliberately NOT stub-filtered: stub docs exist to keep these ratings.
                "feedback": [
                    {"$match": {"feedback.rating": {"$in": ["up", "down"]}}},
                    {"$group": {"_id": "$feedback.rating", "count": {"$sum": 1}}},
                ],
                # One-pass totals for the KPI strip (turns, tokens, success, avg latency).
                "totals": [
                    _EXCLUDE_STUBS,
                    {
                        "$group": {
                            "_id": None,
                            "turns": {"$sum": 1},
                            "inputTokens": {"$sum": "$inputTokens"},
                            "outputTokens": {"$sum": "$outputTokens"},
                            "totalTokens": {"$sum": "$totalTokens"},
                            "success": {"$sum": {"$cond": [{"$eq": ["$status", "success"]}, 1, 0]}},
                            "avgRoundTripMs": {"$avg": "$roundTripMs"},
                        }
                    }
                ],
                # feature → user → model token hierarchy (the sunburst). Capped so a huge tenant
                # can't blow up the payload; the tail is small tokens anyway.
                "hierarchy": [
                    {
                        "$group": {
                            "_id": {
                                "feature": "$feature",
                                "username": "$username",
                                "model": {"$ifNull": ["$model", "unknown"]},
                            },
                            "totalTokens": {"$sum": "$totalTokens"},
                        }
                    },
                    {"$sort": {"totalTokens": -1}},
                    {"$limit": 400},
                ],
                "byUserFeature": [
                    {
                        "$group": {
                            "_id": {"username": "$username", "feature": "$feature"},
                            "totalTokens": {"$sum": "$totalTokens"},
                            "inputTokens": {"$sum": "$inputTokens"},
                            "outputTokens": {"$sum": "$outputTokens"},
                            "requests": {"$sum": 1},
                        }
                    },
                    # Re-group by user to rank by per-user total, then unwind back
                    # so we get top-100 users (not top-100 rows) with all their
                    # feature breakdowns intact.
                    {
                        "$group": {
                            "_id": "$_id.username",
                            "userTotal": {"$sum": "$totalTokens"},
                            "rows": {"$push": "$$ROOT"},
                        }
                    },
                    {"$sort": {"userTotal": -1}},
                    {"$limit": 100},
                    {"$unwind": "$rows"},
                    {"$replaceRoot": {"newRoot": "$rows"}},
                ],
            }
        },
    ]
    try:
        result = list(connector.collection(Collections.AI_USAGE_METRICS).aggregate(pipeline))
    except Exception as exc:
        logger.error("Error fetching AI usage metrics.", details=str(exc), error_code="CE_1311")
        raise HTTPException(503, "Error fetching AI usage metrics. Check logs.")

    raw = result[0] if result else {}

    by_feature = [
        {
            "feature": r["_id"],
            "featureLabel": _feature_label(r["_id"]),
            "inputTokens": r["inputTokens"],
            "outputTokens": r["outputTokens"],
            "totalTokens": r["totalTokens"],
            "requests": r["requests"],
            "avgRoundTripMs": round(r["avgRoundTripMs"]) if r.get("avgRoundTripMs") else None,
        }
        for r in raw.get("byFeature", [])
    ]

    by_user_feature = [
        {
            "username": r["_id"]["username"],
            "feature": r["_id"]["feature"],
            "featureLabel": _feature_label(r["_id"]["feature"]),
            "totalTokens": r["totalTokens"],
            "inputTokens": r["inputTokens"],
            "outputTokens": r["outputTokens"],
            "requests": r["requests"],
        }
        for r in raw.get("byUserFeature", [])
    ]

    # Re-aggregate the day×feature×model rows to day×feature, costing each model split so the
    # usage-over-time chart can trend $ spend (not just tokens) — the "cost burnout" view.
    _by_day_acc: dict = {}
    for r in raw.get("byDay", []):
        key = (r["_id"]["day"], r["_id"]["feature"])
        acc = _by_day_acc.setdefault(key, {"totalTokens": 0, "turns": 0, "costUsd": 0.0})
        acc["totalTokens"] += r.get("totalTokens", 0)
        acc["turns"] += r.get("turns", 0)
        acc["costUsd"] += _estimate_cost(
            r["_id"].get("model"), r.get("inputTokens", 0), r.get("outputTokens", 0), r["_id"].get("provider")
        )
    by_day = [
        {
            "day": day,
            "feature": feature,
            "featureLabel": _feature_label(feature),
            "totalTokens": v["totalTokens"],
            "turns": v["turns"],
            "costUsd": round(v["costUsd"], 4),
        }
        for (day, feature), v in sorted(_by_day_acc.items())
    ]

    # Pricing groups by (model, provider) internally so a model id is never mis-priced across a
    # provider boundary, but the RESPONSE stays one row PER MODEL (no `provider` field) — the UI
    # (ModelDonutChart/TokenSankeyChart) keys byModel on the model name and must not see split rows
    # or a new field. In real data a model id belongs to one provider, so this merge is usually a
    # no-op; it's belt-and-suspenders against a duplicate model label.
    _by_model_acc: dict = {}
    total_cost = 0.0
    for r in raw.get("byModel", []):
        # Split cost into input vs output so the UI can show the OUTPUT-token spend (output is
        # ~5x pricier than input, so the split is the more useful number than the total alone).
        in_price, out_price = _price_for(r["_id"].get("model"), r["_id"].get("provider"))
        input_cost = round((r["inputTokens"] / 1e6) * in_price, 4)
        output_cost = round((r["outputTokens"] / 1e6) * out_price, 4)
        model = r["_id"].get("model")
        acc = _by_model_acc.setdefault(model, {
            "model": model, "inputTokens": 0, "outputTokens": 0, "totalTokens": 0,
            "turns": 0, "costUsd": 0.0, "inputCostUsd": 0.0, "outputCostUsd": 0.0,
        })
        acc["inputTokens"] += r["inputTokens"]
        acc["outputTokens"] += r["outputTokens"]
        acc["totalTokens"] += r["totalTokens"]
        acc["turns"] += r["turns"]
        acc["inputCostUsd"] = round(acc["inputCostUsd"] + input_cost, 4)
        acc["outputCostUsd"] = round(acc["outputCostUsd"] + output_cost, 4)
        acc["costUsd"] = round(acc["inputCostUsd"] + acc["outputCostUsd"], 4)
        total_cost += round(input_cost + output_cost, 4)
    by_model = sorted(_by_model_acc.values(), key=lambda x: x["totalTokens"], reverse=True)

    by_error_type = [{"errorType": r["_id"], "count": r["count"]} for r in raw.get("byErrorType", [])]

    fb = {r["_id"]: r["count"] for r in raw.get("feedback", [])}
    feedback = {"up": fb.get("up", 0), "down": fb.get("down", 0)}

    t = (raw.get("totals") or [{}])[0]
    turns = t.get("turns", 0)
    totals = {
        "turns": turns,
        "inputTokens": t.get("inputTokens", 0),
        "outputTokens": t.get("outputTokens", 0),
        "totalTokens": t.get("totalTokens", 0),
        "success": t.get("success", 0),
        "avgRoundTripMs": round(t["avgRoundTripMs"]) if t.get("avgRoundTripMs") else None,
        "costUsd": round(total_cost, 2),
    }

    hierarchy = [
        {
            "feature": r["_id"]["feature"],
            "featureLabel": _feature_label(r["_id"]["feature"]),
            "username": r["_id"]["username"],
            "model": r["_id"]["model"],
            "totalTokens": r["totalTokens"],
        }
        for r in raw.get("hierarchy", [])
    ]

    return {
        "byFeature": by_feature,
        "byUserFeature": by_user_feature,
        "byDay": by_day,
        "byModel": by_model,
        "byErrorType": by_error_type,
        "feedback": feedback,
        "totals": totals,
        "hierarchy": hierarchy,
    }
