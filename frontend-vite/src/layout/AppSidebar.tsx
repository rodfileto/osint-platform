import { useMemo, useState } from "react";
import { NavLink, useLocation } from "react-router-dom";
import { useSidebar } from "@/contexts/SidebarContext";

type NavItem = {
  name: string;
  path?: string;
  subItems?: { name: string; path: string; badge?: string }[];
};

const navigation: NavItem[] = [
  {
    name: "Investigacao",
    subItems: [
      { name: "Pesquisa CNPJ", path: "/" },
      { name: "Pesquisa Pessoas", path: "/pessoa" },
      { name: "Painel FINEP", path: "/finep" },
      { name: "Painel INPI", path: "/inpi", badge: "new" },
    ],
  },
  {
    name: "Detalhes",
    subItems: [
      { name: "Empresa", path: "/cnpj/00000000" },
      { name: "Pessoa", path: "/pessoa/00000000000" },
      { name: "Pessoa detalhe", path: "/pessoa/detalhe" },
    ],
  },
];

function isGroupActive(currentPath: string, item: NavItem) {
  return item.subItems?.some((subItem) => currentPath === subItem.path) ?? false;
}

export default function AppSidebar() {
  const location = useLocation();
  const { isExpanded, isHovered, isMobileOpen, setIsHovered } = useSidebar();
  const [openGroup, setOpenGroup] = useState<string | null>(() => navigation[0]?.name ?? null);

  const widthClass = useMemo(() => {
    if (isExpanded || isMobileOpen || isHovered) {
      return "w-[290px]";
    }

    return "w-[90px]";
  }, [isExpanded, isHovered, isMobileOpen]);

  return (
    <aside
      className={`fixed left-0 top-0 z-50 flex h-screen flex-col border-r border-gray-200 bg-white px-5 text-gray-900 transition-all duration-300 ease-in-out dark:border-gray-800 dark:bg-gray-900 ${widthClass} ${isMobileOpen ? "translate-x-0" : "-translate-x-full lg:translate-x-0"}`}
      onMouseEnter={() => !isExpanded && setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      <div className={`flex py-8 ${isExpanded || isHovered || isMobileOpen ? "justify-start" : "lg:justify-center"}`}>
        <NavLink to="/" className="flex items-center gap-3">
          {isExpanded || isHovered || isMobileOpen ? (
            <>
              <img className="h-8 dark:hidden" src="/images/logo/logo.svg" alt="OSINT Platform" />
              <img className="hidden h-8 dark:block" src="/images/logo/logo-dark.svg" alt="OSINT Platform" />
            </>
          ) : (
            <img className="h-8 w-8" src="/images/logo/logo-icon.svg" alt="OSINT Platform" />
          )}
        </NavLink>
      </div>

      <div className="flex-1 overflow-y-auto pb-6">
        <div className="mb-6 rounded-2xl border border-brand-100 bg-brand-50 p-4 dark:border-brand-500/20 dark:bg-brand-500/10">
          {(isExpanded || isHovered || isMobileOpen) && (
            <>
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-brand-600 dark:text-brand-300">
                Migration
              </p>
              <p className="mt-2 text-sm text-gray-700 dark:text-gray-300">
                Shared shell running in Vite. Domain routes are mapped and ready for porting.
              </p>
            </>
          )}
        </div>

        <nav className="space-y-4">
          {navigation.map((item) => {
            const expanded = openGroup === item.name || isGroupActive(location.pathname, item);

            return (
              <div key={item.name}>
                <button
                  type="button"
                  onClick={() => setOpenGroup((current) => (current === item.name ? null : item.name))}
                  className={`menu-item group ${expanded ? "menu-item-active" : "menu-item-inactive"} ${!isExpanded && !isHovered && !isMobileOpen ? "lg:justify-center" : "lg:justify-start"}`}
                >
                  <span className={expanded ? "menu-item-icon-active" : "menu-item-icon-inactive"}>◦</span>
                  {(isExpanded || isHovered || isMobileOpen) && <span className="menu-item-text">{item.name}</span>}
                  {(isExpanded || isHovered || isMobileOpen) && (
                    <span className={`ml-auto transition-transform ${expanded ? "rotate-180 text-brand-500 dark:text-brand-400" : "text-gray-500 dark:text-gray-400"}`}>
                      ▾
                    </span>
                  )}
                </button>

                {(isExpanded || isHovered || isMobileOpen) && item.subItems && (
                  <div
                    className="grid overflow-hidden transition-all duration-300 ease-in-out"
                    style={{ gridTemplateRows: expanded ? "1fr" : "0fr" }}
                  >
                    <div className="overflow-hidden">
                      <ul className="ml-9 mt-2 space-y-1">
                        {item.subItems.map((subItem) => (
                          <li key={subItem.path}>
                            <NavLink
                              to={subItem.path}
                              className={({ isActive }) =>
                                `menu-dropdown-item ${isActive ? "menu-dropdown-item-active" : "menu-dropdown-item-inactive"}`
                              }
                            >
                              {subItem.name}
                              {subItem.badge ? (
                                <span className="menu-dropdown-badge ml-auto rounded-full bg-brand-500 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-white dark:bg-brand-400 dark:text-gray-950">
                                  {subItem.badge}
                                </span>
                              ) : null}
                            </NavLink>
                          </li>
                        ))}
                      </ul>
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </nav>
      </div>
    </aside>
  );
}