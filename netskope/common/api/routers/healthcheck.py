"""Guvicorn healthcheck endpoints."""
from fastapi import APIRouter
router = APIRouter()


@router.get(
    "/healthcheck",
    tags=["FastAPI Status"],
    description="Read FastAPI status.",
    status_code=200,
)
def check_guvicorn_health():
    """Check Guvicorn health."""
    return {"sucess": True}
