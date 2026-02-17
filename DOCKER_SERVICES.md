# Docker Services - OSINT Platform

## Running Services

| Service | Container Name | Image | Status | Ports | Access URL |
|---------|---------------|-------|--------|-------|------------|
| **Backend** | osint_backend | osint-platform-backend | Up | 8000 | http://localhost:8000 |
| **Frontend** | osint_frontend | osint-platform-frontend | Up | 3000 | http://localhost:3000 |
| **PostgreSQL** | osint_postgres | postgres:16 | Up (healthy) | 5432 | localhost:5432 |
| **Neo4j** | osint_neo4j | neo4j:5-community | Up | 7474, 7687 | http://localhost:7474 |
| **Redis** | osint_redis | redis:7-alpine | Up (healthy) | 6379 | localhost:6379 |
| **Airflow Webserver** | osint-platform-airflow-webserver-1 | apache/airflow:2.10.4 | Up (healthy) | 8080 | http://localhost:8080 |
| **Airflow Scheduler** | osint-platform-airflow-scheduler-1 | apache/airflow:2.10.4 | Up (unhealthy) | - | - |
| **Airflow Triggerer** | osint-platform-airflow-triggerer-1 | apache/airflow:2.10.4 | Up (healthy) | - | - |

## Service Details

### Backend (Django)
- **Container:** osint_backend
- **Port:** 8000:8000
- **Purpose:** Django REST API
- **Command:** `python manage.py runserver 0.0.0.0:8000`

### Frontend (Next.js)
- **Container:** osint_frontend
- **Port:** 3000:3000
- **Purpose:** Next.js 16 with Turbopack
- **Environment:** `NEXT_PUBLIC_API_URL=http://backend:8000`

### PostgreSQL
- **Container:** osint_postgres
- **Port:** 5432:5432
- **Version:** 16
- **Schemas:**
  - `public` - Auth, Users
  - `cnpj` - CNPJ data
  - `sanctions` - Sanctions data (future)
  - `contracts` - Contracts data (future)
- **Credentials:** Check `.env` file

### Neo4j
- **Container:** osint_neo4j
- **Ports:** 
  - 7474 (HTTP)
  - 7687 (Bolt)
- **Version:** 5 Community
- **Purpose:** Graph database for entity relationships
- **Browser:** http://localhost:7474
- **Credentials:** Check `.env` file

### Redis
- **Container:** osint_redis
- **Port:** 6379:6379
- **Version:** 7 Alpine
- **Purpose:** Caching and Celery message broker

### Airflow
- **Webserver Port:** 8080:8080
- **Version:** 2.10.4
- **Components:**
  - Webserver (UI)
  - Scheduler (DAG execution)
  - Triggerer (Event-driven tasks)
- **Login:** airflow / airflow
- **Purpose:** ETL pipeline orchestration

## Quick Commands

### View Running Containers
```bash
/snap/bin/docker compose ps
```

### View Logs
```bash
# All services
/snap/bin/docker compose logs -f

# Specific service
/snap/bin/docker compose logs -f backend
/snap/bin/docker compose logs -f airflow-scheduler
```

### Execute Commands in Containers
```bash
# Django management commands
/snap/bin/docker compose exec backend python manage.py <command>

# Database access
/snap/bin/docker compose exec postgres psql -U osint_user -d osint_db

# Neo4j shell
/snap/bin/docker compose exec neo4j cypher-shell -u neo4j -p your_password
```

### Start/Stop Services
```bash
# Start all
/snap/bin/docker compose up -d

# Stop all
/snap/bin/docker compose down

# Restart specific service
/snap/bin/docker compose restart backend
```

### Rebuild Services
```bash
# Rebuild backend after code changes
/snap/bin/docker compose up -d --build backend

# Rebuild all
/snap/bin/docker compose up -d --build
```

## Health Status
- ✅ PostgreSQL: Healthy
- ✅ Redis: Healthy
- ✅ Neo4j: Up
- ✅ Backend: Up
- ✅ Frontend: Up
- ✅ Airflow Webserver: Healthy
- ✅ Airflow Triggerer: Healthy
- ⚠️ Airflow Scheduler: Unhealthy (check logs if needed)

## Notes
- Docker is installed via snap: `/snap/bin/docker`
- All services are defined in `docker-compose.yml`
- Environment variables are in `.env` file
- Persistent data is stored in Docker volumes

---
*Last updated: February 16, 2026*
