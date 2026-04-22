# Frontend Vite Migration Record

## Status

Completed.

The repository no longer uses a parallel `frontend-vite/` application. The Vite app was promoted to the canonical `frontend/` directory and the old Next.js frontend was removed.

## Outcome

- the active frontend now lives in `frontend/`
- the stack uses React 19 and Vite as the default frontend runtime
- compose files and local startup flows point to `./frontend`
- frontend runtime variables use the `VITE_*` convention
- the historical Next.js app is no longer part of the active repository layout

## What Changed

### Runtime cutover

- `docker-compose.yml` now builds the frontend from `./frontend`
- `compose.dev.yml` mounts and builds `./frontend`
- `compose.prod.yml` builds production assets from `./frontend`
- frontend package metadata was renamed from `osint-platform-frontend-vite` to `osint-platform-frontend`

### Frontend structure

- shared shell, contexts, layout, and domain routes were consolidated into the Vite app
- environment access now uses `import.meta.env.VITE_*`
- migration placeholder text was updated to reference `frontend/src/components`

### Documentation updates

- primary docs now describe the frontend as React + Vite
- operational references to the old Next.js frontend were removed from active documentation

## Historical Rationale

The migration was originally executed as a parallel app to reduce cutover risk. That approach allowed the team to validate routing, layout, environment wiring, and domain screens before replacing the old frontend path.

## Remaining Notes

- this file is retained as historical context for the frontend migration decision
- references to `frontend-vite/` should be treated as archival only if they appear in older notes or git history