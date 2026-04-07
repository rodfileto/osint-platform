# Cloudflare Tunnel Deployment Guide

## What this does

A `cloudflared` container joins your Docker network and creates an outbound-only encrypted tunnel to Cloudflare's edge. No exposed ports or inbound firewall rules are needed on the server.

Traffic flow:

```
Internet → Cloudflare Edge → cloudflared (container) → frontend:3000 / backend:8000 / airflow-webserver:8080
```

---

## Prerequisites

1. A **Cloudflare account** with at least one domain.
2. **Zero Trust** enabled (free tier is fine).

---

## Step-by-step setup

### 1. Create the tunnel

In the [Cloudflare Zero Trust dashboard](https://one.dash.cloudflare.com):

1. Go to **Networks → Tunnels → Create a tunnel**.
2. Choose **Cloudflared** as the connector type.
3. Name the tunnel (e.g. `osint-platform`).
4. Copy the **tunnel token** shown on the install screen.

### 2. Configure DNS (public hostnames)

Still in the tunnel configuration, add **Public Hostnames**:

| Subdomain | Domain | Service |
|---|---|---|
| `app` | `yourdomain.com` | `http://frontend:3000` |
| `api` | `yourdomain.com` | `http://backend:8000` |
| `airflow` | `yourdomain.com` | `http://airflow-webserver:8080` |

Cloudflare will automatically create the CNAME records.

> **Tip:** Protect the `airflow` route with a **Cloudflare Access policy** (Zero Trust → Access → Applications) so only authorized users can reach it.

### 3. Configure environment variables

Copy `.env.example` to `.env` and fill in:

```dotenv
CLOUDFLARE_TUNNEL_TOKEN=eyJhIjoiMTIzN...   # from step 1
CF_APP_DOMAIN=app.yourdomain.com
CF_API_DOMAIN=api.yourdomain.com
CF_AIRFLOW_DOMAIN=airflow.yourdomain.com

# These must reflect the HTTPS URLs served through the tunnel
NEXT_PUBLIC_API_URL=https://api.yourdomain.com
DJANGO_ALLOWED_HOSTS=api.yourdomain.com,localhost,127.0.0.1
CORS_ALLOWED_ORIGINS=https://app.yourdomain.com
CSRF_TRUSTED_ORIGINS=https://app.yourdomain.com,https://api.yourdomain.com
```

### 4. Build and start with the tunnel overlay

```bash
docker compose \
  -f docker-compose.yml \
  -f compose.prod.yml \
  -f compose.tunnel.yml \
  up -d --build
```

Check tunnel status:

```bash
docker compose -f docker-compose.yml -f compose.prod.yml -f compose.tunnel.yml logs -f cloudflared
```

You should see `Connection established` lines for each Cloudflare PoP.

---

## Security recommendations

| Action | Why |
|---|---|
| Never expose ports 3000 / 8000 / 8080 in `compose.prod.yml` in production | All traffic should flow through the tunnel |
| Enable Cloudflare Access policy on `airflow.*` | Airflow has no fine-grained auth by default |
| Enable Cloudflare Access policy on `api.*` if the API is internal-only | Adds an extra auth layer in front of Django |
| Use `DJANGO_DEBUG=false` in production | Prevents stack traces leaking |
| Rotate the tunnel token if compromised | Zero Trust dashboard → your tunnel → Revoke |

---

## Disabling the tunnel (dev / local work)

Simply omit the `-f compose.tunnel.yml` file:

```bash
docker compose -f docker-compose.yml -f compose.dev.yml up -d
```
