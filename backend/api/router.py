"""Central API router composition for the FastAPI migration."""

from api.routers import cnpj, finep, inpi, search


def get_api_router():
    try:
        from fastapi import APIRouter
    except ImportError as exc:
        raise RuntimeError(
            "FastAPI is not installed yet. Complete migration step 2 before running the app."
        ) from exc

    router = APIRouter()
    router.include_router(search.get_router(), prefix="/search", tags=["search"])
    router.include_router(cnpj.get_router(), prefix="/cnpj", tags=["cnpj"])
    router.include_router(finep.get_router(), prefix="/finep", tags=["finep"])
    router.include_router(inpi.get_router(), prefix="/inpi", tags=["inpi"])
    return router
