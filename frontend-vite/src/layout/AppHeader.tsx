import { useEffect, useRef } from "react";
import { Link } from "react-router-dom";
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

type AppHeaderProps = {
  apiBaseUrl: string;
};

export default function AppHeader({ apiBaseUrl }: AppHeaderProps) {
  const inputRef = useRef<HTMLInputElement>(null);
  const { isMobileOpen, toggleSidebar, toggleMobileSidebar } = useSidebar();

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        inputRef.current?.focus();
      }
    };

    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, []);

  const handleSidebarToggle = () => {
    if (window.innerWidth >= 1024) {
      toggleSidebar();
      return;
    }

    toggleMobileSidebar();
  };

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
            <img className="h-8 dark:hidden" src="/images/logo/logo.svg" alt="OSINT Platform" />
            <img className="hidden h-8 dark:block" src="/images/logo/logo-dark.svg" alt="OSINT Platform" />
          </Link>

          <div className="hidden w-full lg:block">
            <label className="relative block">
              <span className="pointer-events-none absolute left-4 top-1/2 -translate-y-1/2 text-gray-400">
                ⌕
              </span>
              <input
                ref={inputRef}
                type="text"
                placeholder="Search or type command..."
                className="h-11 w-full rounded-lg border border-gray-200 bg-transparent py-2.5 pl-12 pr-20 text-sm text-gray-800 shadow-theme-xs placeholder:text-gray-400 focus:border-brand-300 focus:outline-hidden focus:ring-3 focus:ring-brand-500/10 dark:border-gray-800 dark:bg-white/[0.03] dark:text-white/90 dark:placeholder:text-white/30"
              />
              <span className="absolute right-2.5 top-1/2 inline-flex -translate-y-1/2 items-center gap-1 rounded-lg border border-gray-200 bg-gray-50 px-2 py-1 text-xs text-gray-500 dark:border-gray-800 dark:bg-white/[0.03] dark:text-gray-400">
                Ctrl K
              </span>
            </label>
          </div>
        </div>

        <div className="flex w-full items-center justify-between gap-3 lg:w-auto lg:justify-end">
          <div className="hidden rounded-full border border-brand-200 bg-brand-50 px-3 py-2 text-xs font-medium text-brand-700 dark:border-brand-500/20 dark:bg-brand-500/10 dark:text-brand-300 sm:block">
            API {apiBaseUrl}
          </div>
          <ThemeToggleButton />
          <div className="flex items-center gap-3 rounded-full border border-gray-200 bg-white px-3 py-2 dark:border-gray-800 dark:bg-gray-900">
            <img className="h-9 w-9 rounded-full object-cover" src="/images/user/owner.jpg" alt="User" />
            <div className="hidden text-right sm:block">
              <p className="text-sm font-medium text-gray-800 dark:text-white/90">OSINT Operator</p>
              <p className="text-theme-xs text-gray-500 dark:text-gray-400">Shared shell only</p>
            </div>
          </div>
        </div>
      </div>
    </header>
  );
}