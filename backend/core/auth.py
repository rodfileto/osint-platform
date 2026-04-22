"""Keycloak-backed JWT authentication helpers for FastAPI."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

import jwt
import requests
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from core.config import Settings, get_settings

_bearer_scheme = HTTPBearer(auto_error=False)
_jwks_cache: dict[str, tuple[float, dict[str, Any]]] = {}


@dataclass(slots=True)
class AuthenticatedUser:
    sub: str
    username: str
    display_name: str
    email: str | None
    roles: list[str]
    claims: dict[str, Any]

    @property
    def is_authenticated(self) -> bool:
        return True

    def as_dict(self) -> dict[str, Any]:
        return {
            "sub": self.sub,
            "username": self.username,
            "display_name": self.display_name,
            "email": self.email,
            "roles": self.roles,
            "claims": self.claims,
        }


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


def _local_development_user() -> AuthenticatedUser:
    claims = {
        "sub": "local-development",
        "preferred_username": "local-dev",
        "name": "Local Development",
        "realm_access": {"roles": ["developer"]},
    }
    return AuthenticatedUser(
        sub="local-development",
        username="local-dev",
        display_name="Local Development",
        email=None,
        roles=["developer"],
        claims=claims,
    )


def _load_jwks(settings: Settings) -> dict[str, Any]:
    cache_key = settings.keycloak_jwks_url
    cached = _jwks_cache.get(cache_key)
    now = time.time()
    if cached and cached[0] > now:
        return cached[1]

    response = requests.get(settings.keycloak_jwks_url, timeout=settings.keycloak_http_timeout_seconds)
    response.raise_for_status()
    payload = response.json()
    _jwks_cache[cache_key] = (now + settings.keycloak_jwks_cache_ttl_seconds, payload)
    return payload


def _get_signing_key(token: str, settings: Settings):
    try:
        header = jwt.get_unverified_header(token)
    except jwt.PyJWTError as exc:
        raise _unauthorized("Malformed bearer token.") from exc

    kid = header.get("kid")
    if not kid:
        raise _unauthorized("Token header is missing a signing key id.")

    try:
        jwks = _load_jwks(settings)
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication provider is currently unavailable.",
        ) from exc

    for candidate in jwks.get("keys", []):
        if candidate.get("kid") == kid:
            return jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(candidate))

    _jwks_cache.pop(settings.keycloak_jwks_url, None)
    raise _unauthorized("Token signing key was not recognized.")


def _extract_roles(claims: dict[str, Any], settings: Settings) -> list[str]:
    roles: list[str] = []

    realm_access = claims.get("realm_access")
    if isinstance(realm_access, dict):
        realm_roles = realm_access.get("roles")
        if isinstance(realm_roles, list):
            roles.extend(str(role) for role in realm_roles)

    resource_access = claims.get("resource_access")
    if isinstance(resource_access, dict):
        client_access = resource_access.get(settings.keycloak_client_id)
        if isinstance(client_access, dict):
            client_roles = client_access.get("roles")
            if isinstance(client_roles, list):
                roles.extend(str(role) for role in client_roles)

    return sorted(set(role for role in roles if role))


def _validate_client_access(claims: dict[str, Any], settings: Settings) -> None:
    expected_client_id = settings.keycloak_client_id.strip()
    if not expected_client_id:
        return

    audiences = claims.get("aud")
    if isinstance(audiences, str):
        audience_values = [audiences]
    elif isinstance(audiences, list):
        audience_values = [str(value) for value in audiences]
    else:
        audience_values = []

    if expected_client_id in audience_values:
        return

    authorized_party = claims.get("azp")
    if authorized_party == expected_client_id:
        return

    raise _unauthorized("Bearer token is not intended for this application.")


def _decode_token(token: str, settings: Settings) -> dict[str, Any]:
    signing_key = _get_signing_key(token, settings)

    try:
        claims = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            issuer=settings.keycloak_realm_url,
            options={"verify_aud": False},
        )
    except jwt.ExpiredSignatureError as exc:
        raise _unauthorized("Bearer token has expired.") from exc
    except jwt.InvalidIssuerError as exc:
        raise _unauthorized("Bearer token was issued by an unexpected provider.") from exc
    except jwt.PyJWTError as exc:
        raise _unauthorized("Bearer token validation failed.") from exc

    _validate_client_access(claims, settings)
    return claims


def _build_user_from_claims(claims: dict[str, Any], settings: Settings) -> AuthenticatedUser:
    username = (
        claims.get("preferred_username")
        or claims.get("email")
        or claims.get("sub")
        or "unknown"
    )
    display_name = claims.get("name") or claims.get("given_name") or username
    roles = _extract_roles(claims, settings)
    return AuthenticatedUser(
        sub=str(claims.get("sub") or username),
        username=str(username),
        display_name=str(display_name),
        email=str(claims["email"]) if claims.get("email") else None,
        roles=roles,
        claims=claims,
    )


def require_authenticated_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
) -> AuthenticatedUser:
    settings = get_settings()
    if not settings.keycloak_auth_enabled:
        user = _local_development_user()
        request.state.current_user = user
        return user

    if credentials is None or credentials.scheme.lower() != "bearer":
        raise _unauthorized("Authentication is required.")

    claims = _decode_token(credentials.credentials, settings)
    user = _build_user_from_claims(claims, settings)
    request.state.current_user = user
    return user
