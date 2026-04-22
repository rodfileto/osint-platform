import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { runtimeEnv } from "@/lib/env";
import {
  buildUserFromToken,
  getKeycloakInstance,
  initializeKeycloak,
  type AuthUser,
} from "@/auth/keycloak";

type AuthContextValue = {
  ready: boolean;
  enabled: boolean;
  isAuthenticated: boolean;
  user: AuthUser | null;
  error: string | null;
  login: () => Promise<void>;
  logout: () => Promise<void>;
};

const AuthContext = createContext<AuthContextValue | null>(null);

function AuthScreen({ title, description, action }: { title: string; description: string; action?: ReactNode }) {
  return (
    <div className="flex min-h-screen items-center justify-center bg-gray-50 px-6 dark:bg-gray-950">
      <div className="w-full max-w-lg rounded-3xl border border-gray-200 bg-white p-8 shadow-xl dark:border-gray-800 dark:bg-gray-900">
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-brand-500">OSINT Platform</p>
        <h1 className="mt-4 text-3xl font-semibold text-gray-900 dark:text-white">{title}</h1>
        <p className="mt-3 text-sm leading-6 text-gray-600 dark:text-gray-300">{description}</p>
        {action ? <div className="mt-6">{action}</div> : null}
      </div>
    </div>
  );
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [ready, setReady] = useState(!runtimeEnv.keycloakEnabled);
  const [isAuthenticated, setIsAuthenticated] = useState(!runtimeEnv.keycloakEnabled);
  const [user, setUser] = useState<AuthUser | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const keycloak = getKeycloakInstance();
    let isMounted = true;

    const syncState = () => {
      if (!keycloak || !isMounted) {
        return;
      }

      setIsAuthenticated(Boolean(keycloak.authenticated));
      setUser(buildUserFromToken(keycloak.tokenParsed));
      setReady(true);
      setError(null);
    };

    if (!runtimeEnv.keycloakEnabled || !keycloak) {
      setReady(true);
      setIsAuthenticated(true);
      setUser(null);
      return () => {
        isMounted = false;
      };
    }

    keycloak.onAuthSuccess = syncState;
    keycloak.onAuthRefreshSuccess = syncState;
    keycloak.onAuthLogout = syncState;
    keycloak.onTokenExpired = () => {
      void keycloak.updateToken(60).catch(() => keycloak.login());
    };

    void initializeKeycloak()
      .then(() => {
        syncState();
      })
      .catch((initializationError: unknown) => {
        if (!isMounted) {
          return;
        }

        setReady(true);
        setIsAuthenticated(false);
        setUser(null);
        setError(
          initializationError instanceof Error
            ? initializationError.message
            : "Nao foi possivel inicializar o login via Keycloak.",
        );
      });

    return () => {
      isMounted = false;
      keycloak.onAuthSuccess = undefined;
      keycloak.onAuthRefreshSuccess = undefined;
      keycloak.onAuthLogout = undefined;
      keycloak.onTokenExpired = undefined;
    };
  }, []);

  const value = useMemo<AuthContextValue>(() => {
    const keycloak = getKeycloakInstance();
    return {
      ready,
      enabled: runtimeEnv.keycloakEnabled,
      isAuthenticated,
      user,
      error,
      login: async () => {
        if (!keycloak) {
          return;
        }
        await keycloak.login();
      },
      logout: async () => {
        if (!keycloak) {
          return;
        }
        await keycloak.logout({ redirectUri: window.location.origin });
      },
    };
  }, [error, isAuthenticated, ready, user]);

  if (!ready) {
    return (
      <AuthScreen
        title="Inicializando sessao"
        description="Conectando ao provedor Keycloak e validando o token da sessao atual."
      />
    );
  }

  if (error) {
    return (
      <AuthScreen
        title="Falha no login"
        description={error}
        action={
          <button
            type="button"
            onClick={() => window.location.reload()}
            className="rounded-xl bg-brand-500 px-4 py-2 text-sm font-medium text-white transition hover:bg-brand-600"
          >
            Tentar novamente
          </button>
        }
      />
    );
  }

  if (runtimeEnv.keycloakEnabled && !isAuthenticated) {
    return (
      <AuthScreen
        title="Autenticacao necessaria"
        description="A sessao nao esta autenticada. Entre novamente para acessar a plataforma."
        action={
          <button
            type="button"
            onClick={() => {
              void value.login();
            }}
            className="rounded-xl bg-brand-500 px-4 py-2 text-sm font-medium text-white transition hover:bg-brand-600"
          >
            Entrar com Keycloak
          </button>
        }
      />
    );
  }

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within an AuthProvider.");
  }
  return context;
}