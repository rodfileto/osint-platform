# Frontend Vite Migration Plan

## Decision

Use a parallel migration into a dedicated `frontend-vite/` app based on the TailAdmin React + Vite structure.

This is lower risk than converting the current Next.js tree in place because:

- the current frontend uses Next.js mostly as a client shell
- the TailAdmin vendor stack already supports React + Vite
- the project-specific value is concentrated in a limited set of OSINT modules
- a parallel app lets us validate routing, layout, and API contracts before retiring the Next.js frontend

## Phase Plan

### Phase 1: Parallel Vite Bootstrap

Goal: create a working Vite frontend with the shared shell and route skeleton.

Scope:

- create `frontend-vite/`
- configure Vite, TypeScript, Tailwind CSS, path aliases, and shared public assets
- port theme and sidebar providers
- port the admin shell layout
- add route placeholders for the OSINT sections
- adapt API environment access from `process.env.NEXT_PUBLIC_*` to `import.meta.env.VITE_*`

Status: in progress

### Phase 2: Cross-Cutting Infrastructure

Goal: make the Vite shell production-ready before moving domain-heavy screens.

Scope:

- finalize navigation model and route structure
- decide auth/session model against Django
- add Axios interceptors and error handling
- define WebSocket client boundary and reconnect behavior
- wire deployment and Docker integration

### Phase 3: Product Route Porting

Goal: move the real OSINT product surface before any template/demo pages.

Priority order:

1. CNPJ search
2. Empresa detail
3. Pessoa search and detail
4. FINEP overview
5. INPI overview

### Phase 4: Feature Completion

Goal: port advanced visualizations and remaining app behaviors.

Scope:

- network graphs
- Leaflet map integrations
- route-level loading and empty states
- WebSocket-driven views where needed
- template pages only if still useful

### Phase 5: Cutover

Goal: switch the repo from Next.js frontend to Vite frontend.

Scope:

- update Dockerfiles and compose
- update docs and local run scripts
- validate static asset serving and reverse proxy rules
- remove or archive Next.js frontend after parity is reached

## Current To Target File Mapping

### Shared infrastructure

- `frontend/src/context/SidebarContext.tsx` -> `frontend-vite/src/contexts/SidebarContext.tsx`
- `frontend/src/context/ThemeContext.tsx` -> `frontend-vite/src/contexts/ThemeContext.tsx`
- `frontend/src/lib/apiClient.ts` -> `frontend-vite/src/lib/apiClient.ts`
- `frontend/src/app/globals.css` -> `frontend-vite/src/styles/index.css`

### Shared shell

- `frontend/src/layout/AppHeader.tsx` -> `frontend-vite/src/layout/AppHeader.tsx`
- `frontend/src/layout/AppSidebar.tsx` -> `frontend-vite/src/layout/AppSidebar.tsx`
- `frontend/src/layout/Backdrop.tsx` -> `frontend-vite/src/layout/Backdrop.tsx`
- `frontend/src/app/(admin)/layout.tsx` -> `frontend-vite/src/layout/AppLayout.tsx`
- `frontend/src/components/common/PageBreadCrumb.tsx` -> `frontend-vite/src/components/common/PageBreadcrumb.tsx`

### Product route wrappers

- `frontend/src/app/(admin)/page.tsx` -> route `/`
- `frontend/src/app/(admin)/pessoa/page.tsx` -> route `/pessoa`
- `frontend/src/app/(admin)/finep/page.tsx` -> route `/finep`
- `frontend/src/app/(admin)/inpi/page.tsx` -> route `/inpi`
- `frontend/src/app/(admin)/cnpj/[cnpjBasico]/page.tsx` -> route `/cnpj/:cnpjBasico`
- `frontend/src/app/(admin)/pessoa/[cpf]/page.tsx` -> route `/pessoa/:cpf`
- `frontend/src/app/(admin)/pessoa/detalhe/page.tsx` -> route `/pessoa/detalhe`

### Product modules to port after the shell

- `frontend/src/components/cnpj/*`
- `frontend/src/components/finep/*`
- `frontend/src/components/inpi/*`

### Explicitly deferred for now

- `frontend/src/app/(admin)/(others-pages)/*`
- `frontend/src/app/(admin)/(ui-elements)/*`
- `frontend/src/app/(full-width-pages)/*`

## Immediate Next Steps

1. Finish the Vite shell bootstrap in `frontend-vite/`.
2. Validate local build and route navigation.
3. Port the CNPJ search flow first, since it is the primary entry route.