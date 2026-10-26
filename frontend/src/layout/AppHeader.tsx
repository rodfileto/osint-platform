import { Link } from "react-router-dom";
import { useAuth } from "@/auth/AuthProvider";
import { useSidebar } from "@/contexts/SidebarContext";
import { useTheme } from "@/contexts/ThemeContext";

function ThemeToggleButton() {
  const { theme, toggleTheme } = useTheme();

  return (
    <button
      type="button"
      onClick={toggleTheme}
      className="flex h-11 w-11 items-center justify-center rounded-full border border-gray-200 bg-white text-gray-500 transition-colors hover:bg-gray-100 hover:text-gray-700 dark:border-gray-800 dark:bg-gray-900 dark:text-gray-400 dark:hover:bg-gray-800 dark:hover:text-white"
      aria-label="Toggle theme"
    >
      {theme === "dark" ? "☀" : "☾"}
    </button>
  );
}

export default function AppHeader() {
  const { isMobileOpen, toggleSidebar, toggleMobileSidebar } = useSidebar();
  const { enabled, logout, user } = useAuth();

  const handleSidebarToggle = () => {
    if (window.innerWidth >= 1024) {
      toggleSidebar();
      return;
    }

    toggleMobileSidebar();
  };

  const userLabel = user?.displayName || "Sessao ativa";
  const userSecondaryLabel = user?.email || user?.username || (enabled ? "Keycloak" : "Modo local");
  const userInitials = userLabel
    .split(/\s+/)
    .slice(0, 2)
    .map((part) => part[0]?.toUpperCase() ?? "")
    .join("") || "OS";

  return (
    <header className="sticky top-0 z-99999 flex w-full border-b border-gray-200 bg-white dark:border-gray-800 dark:bg-gray-900">
      <div className="flex w-full flex-col items-center justify-between gap-3 px-3 py-3 lg:flex-row lg:px-6 lg:py-4">
        <div className="flex w-full items-center gap-3 lg:max-w-2xl">
          <button
            type="button"
            className="flex h-11 w-11 items-center justify-center rounded-lg border border-gray-200 text-gray-500 dark:border-gray-800 dark:text-gray-400"
            onClick={handleSidebarToggle}
            aria-label="Toggle sidebar"
          >
            {isMobileOpen ? "✕" : "☰"}
          </button>

          <Link to="/" className="flex items-center gap-3 lg:hidden">
            <img className="h-8 w-8" src="/images/logo/logo-icon.svg" alt="" />
            <span className="text-lg font-semibold text-gray-900 dark:text-white">OSINT Platform</span>
          </Link>
        </div>

        <div className="flex w-full items-center justify-between gap-3 lg:w-auto lg:justify-end">
          <ThemeToggleButton />
          {enabled ? (
            <button
              type="button"
              onClick={() => {
                void logout();
              }}
              className="rounded-full border border-gray-200 bg-white px-4 py-2 text-sm font-medium text-gray-700 transition hover:bg-gray-50 dark:border-gray-800 dark:bg-gray-900 dark:text-gray-300 dark:hover:bg-gray-800"
            >
              Sair
            </button>
          ) : null}
          <div className="flex items-center gap-3 rounded-full border border-gray-200 bg-white px-3 py-2 dark:border-gray-800 dark:bg-gray-900">
            <div className="flex h-9 w-9 items-center justify-center rounded-full bg-brand-500 text-sm font-semibold text-white">
              {userInitials}
            </div>
            <div className="hidden text-right sm:block">
              <p className="text-sm font-medium text-gray-800 dark:text-white/90">{userLabel}</p>
              <p className="text-theme-xs text-gray-500 dark:text-gray-400">{userSecondaryLabel}</p>
            </div>
          </div>
        </div>
      </div>
    </header>
  );
}