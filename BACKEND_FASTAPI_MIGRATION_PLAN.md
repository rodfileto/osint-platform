# Plan: Migrate Backend from Django to FastAPI

Replaces the existing Django backend with a lightweight, high-performance FastAPI application. This perfectly aligns with the architecture's stateless gateway pattern, leveraging asynchronous I/O to query PostgreSQL (materialized views) and Neo4j concurrently without the overhead of an unused ORM.

## Steps
1. **Bootstrap FastAPI Structure:** Initialize the new directory layout in `/backend` (core configuration, database drivers, and modular API routers).
2. **Setup Dependencies:** Replace `backend/requirements.txt` with FastAPI stack dependencies (`fastapi`, `uvicorn`, `pydantic`, `pydantic-settings`, `asyncpg`, `sqlalchemy`, `neo4j`).
3. **Database Connections:** Implement async connection managers for PostgreSQL (using SQLAlchemy 2.0 Core / asyncpg) and Neo4j (using the official async Python driver) in `backend/core/database.py`. *Depends on 2.*
4. **Implement Domain Routers:** Recreate the `cnpj`, `finep`, and `inpi` modules as FastAPI routers (`backend/api/routers/<domain>.py`) with Pydantic response models. *Depends on 3.*
5. **Concurrent Search Endpoint:** Create a unified search endpoint that utilizes `asyncio.gather()` to fetch data from both Postgres and Neo4j simultaneously. *Depends on 4.*
6. **Docker Refactor:** Update `backend/Dockerfile`, `backend/entrypoint.prod.sh`, and the `docker-compose.yml` files (dev/prod/tunnel) to start the app using `uvicorn` instead of `gunicorn/wsgi`.
7. **Cleanup:** Delete legacy Django files (`manage.py`, `config/`, Django apps). *Parallel with step 6.*

## Relevant files
- `/backend/requirements.txt` — Dependencies list
- `/backend/main.py` — New FastAPI application entrypoint
- `/backend/core/config.py` — Settings and environment variables management via Pydantic
- `/backend/core/database.py` — Async driver instantiations
- `/backend/api/routers/` — Directory for `cnpj.py`, `finep.py`, `inpi.py` routes
- `/docker-compose.yml` (and other compose files) — Update backend service commands/healthchecks
- `/ARCHITECTURE_PLAN.md` — Update the API Layer documentation to reflect the new framework

## Verification
1. Bring up the backend container using `docker compose -f docker-compose.yml -f compose.dev.yml up -d backend`.
2. Access the auto-generated Swagger UI at `http://localhost:8000/docs` to verify endpoints and schemas load successfully.
3. Manually execute a search query via Swagger to ensure both PostgreSQL and Neo4j drivers connect and return unified JSON data.

## Decisions
- **ORM Choice:** We will bypass traditional ORM object-mapping for Postgres. Since Flyway handles DB migrations and data comes from Airflow, we will use raw async SQL or SQLAlchemy Core to query materialized views directly.
- **Validation:** Pydantic will be the strict source of truth for all API outputs, which will directly translate to TypeScript interfaces on the frontend.
- **Scope:** This phase only touches the `backend/` directory and Docker files. Frontend and Pipeline layers remain completely untouched.

## Further Considerations
1. Since we are dropping Django, were you planning to use Django Admin for user management or auth, or should we plan for a lightweight JWT/OAuth2 flow later on using FastAPI's security features?
