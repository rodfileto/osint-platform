import axios from "axios";
import { ensureFreshAccessToken } from "@/auth/keycloak";
import { runtimeEnv } from "@/lib/env";

const apiClient = axios.create({
  baseURL: runtimeEnv.apiUrl,
  headers: { Accept: "application/json" },
});

apiClient.interceptors.request.use(async (config) => {
  if (runtimeEnv.keycloakEnabled) {
    const token = await ensureFreshAccessToken();
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
  }
  return config;
});

export default apiClient;
