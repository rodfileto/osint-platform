import Keycloak, { type KeycloakTokenParsed } from "keycloak-js";
import { runtimeEnv } from "@/lib/env";

export type AuthUser = {
  sub: string;
  username: string;
  displayName: string;
  email: string | null;
  roles: string[];
};

type KeycloakTokenWithRoles = KeycloakTokenParsed & {
  realm_access?: { roles?: string[] };
  resource_access?: Record<string, { roles?: string[] }>;
  preferred_username?: string;
  email?: string;
  name?: string;
};

const keycloak = runtimeEnv.keycloakEnabled
  ? new Keycloak({
      url: runtimeEnv.keycloakUrl,
      realm: runtimeEnv.keycloakRealm,
      clientId: runtimeEnv.keycloakClientId,
    })
  : null;

let initPromise: Promise<boolean> | null = null;

function normalizeRoles(tokenParsed: KeycloakTokenWithRoles | undefined): string[] {
  if (!tokenParsed) {
    return [];
  }

  const realmRoles = tokenParsed.realm_access?.roles ?? [];
  const clientRoles = tokenParsed.resource_access?.[runtimeEnv.keycloakClientId]?.roles ?? [];
  return Array.from(new Set([...realmRoles, ...clientRoles].filter(Boolean))).sort();
}

export function getKeycloakInstance() {
  return keycloak;
}

export function buildUserFromToken(tokenParsed: KeycloakTokenParsed | undefined): AuthUser | null {
  if (!tokenParsed) {
    return null;
  }

  const parsed = tokenParsed as KeycloakTokenWithRoles;
  const username = parsed.preferred_username || parsed.email || parsed.sub || "unknown";
  return {
    sub: parsed.sub || username,
    username,
    displayName: parsed.name || username,
    email: parsed.email || null,
    roles: normalizeRoles(parsed),
  };
}

export async function initializeKeycloak(): Promise<boolean> {
  if (!keycloak) {
    return false;
  }

  if (!initPromise) {
    initPromise = keycloak.init({
      onLoad: "login-required",
      pkceMethod: "S256",
      checkLoginIframe: false,
    });
  }

  try {
    return await initPromise;
  } catch (error) {
    initPromise = null;
    throw error;
  }
}

export async function ensureFreshAccessToken(minValidity = 60): Promise<string | null> {
  if (!keycloak) {
    return null;
  }

  await initializeKeycloak();
  if (!keycloak.authenticated) {
    await keycloak.login();
    return null;
  }

  try {
    await keycloak.updateToken(minValidity);
  } catch {
    await keycloak.login();
    return null;
  }

  return keycloak.token ?? null;
}