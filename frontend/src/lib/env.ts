function readString(value: string | undefined, fallback: string): string {
  const trimmed = value?.trim();
  return trimmed ? trimmed : fallback;
}

function readBoolean(value: string | undefined, fallback: boolean): boolean {
  if (value === undefined || value.trim() === "") {
    return fallback;
  }
  return ["true", "1", "yes", "on"].includes(value.trim().toLowerCase());
}

function stripTrailingSlash(url: string): string {
  return url.replace(/\/+$/, "");
}

export const runtimeEnv = {
  apiUrl: stripTrailingSlash(readString(import.meta.env.VITE_API_URL, "http://localhost:8000")),
  keycloakEnabled: readBoolean(import.meta.env.VITE_KEYCLOAK_ENABLED, false),
  keycloakUrl: stripTrailingSlash(readString(import.meta.env.VITE_KEYCLOAK_URL, "http://localhost:8081")),
  keycloakRealm: readString(import.meta.env.VITE_KEYCLOAK_REALM, "osint-platform"),
  keycloakClientId: readString(import.meta.env.VITE_KEYCLOAK_CLIENT_ID, "osint-frontend"),
} as const;
