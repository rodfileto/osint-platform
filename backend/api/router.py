"""Central API router composition for the FastAPI migration."""

from api.routers import auth, cnpj, finep, inovalink, inpi, search
from core.auth import require_authenticated_user
from core.config import get_settings


def get_api_router():
    try:
        from fastapi import APIRouter, Depends
    except ImportError as exc:
        raise RuntimeError(
            "FastAPI is not installed yet. Complete migration step 2 before running the app."
        ) from exc

    settings = get_settings()
    router = APIRouter()
    protected_router = APIRouter(
        dependencies=[Depends(require_authenticated_user)] if settings.keycloak_auth_enabled else []
    )

    router.include_router(auth.get_router(), prefix="/auth", tags=["auth"])
    protected_router.include_router(search.get_router(), prefix="/search", tags=["search"])
    protected_router.include_router(cnpj.get_router(), prefix="/cnpj", tags=["cnpj"])
    protected_router.include_router(finep.get_router(), prefix="/finep", tags=["finep"])
    protected_router.include_router(inovalink.get_router(), prefix="/inovalink", tags=["inovalink"])
    protected_router.include_router(inpi.get_router(), prefix="/inpi", tags=["inpi"])
    router.include_router(protected_router)
    return router
