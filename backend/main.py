"""FastAPI application entrypoint for the backend migration."""

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.router import get_api_router
from core.config import get_settings
from core.database import DatabaseManager


def create_app() -> FastAPI:
    settings = get_settings()
    database_manager = DatabaseManager(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.settings = settings
        app.state.database_manager = database_manager
        await database_manager.connect()
        try:
            yield
        finally:
            await database_manager.disconnect()

    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        debug=settings.debug,
        lifespan=lifespan,
    )

    if settings.cors_allowed_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_allowed_origins,
            allow_credentials=settings.cors_allow_credentials,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    @app.get("/health", tags=["system"])
    async def healthcheck(request: Request):
        report = await request.app.state.database_manager.healthcheck()
        is_healthy = all(service["ok"] for service in report.values())
        return JSONResponse(
            status_code=200 if is_healthy else 503,
            content={
                "status": "ok" if is_healthy else "degraded",
                "services": report,
            },
        )

    app.include_router(get_api_router(), prefix=settings.api_prefix)
    return app


app = create_app()