"""Authentication and identity endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from core.auth import AuthenticatedUser, require_authenticated_user
from core.config import get_settings


def get_router() -> APIRouter:
    router = APIRouter()

    @router.get("/config", summary="Authentication runtime configuration")
    async def get_auth_config() -> dict[str, object]:
        settings = get_settings()
        return {
            "enabled": settings.keycloak_auth_enabled,
            "issuer": settings.keycloak_realm_url,
            "client_id": settings.keycloak_client_id,
            "realm": settings.keycloak_realm,
        }

    @router.get("/me", summary="Current authenticated user")
    async def get_current_user(
        current_user: AuthenticatedUser = Depends(require_authenticated_user),
    ) -> dict[str, object]:
        return {
            "authenticated": current_user.is_authenticated,
            "user": current_user.as_dict(),
        }

    return router