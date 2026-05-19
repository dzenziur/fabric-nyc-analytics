# Makefile for Phase 7 external app (docker-compose).
# Run from repo root. Requires Docker Desktop + `make` (Git Bash on Windows).

COMPOSE ?= docker compose

# ---------------- Lifecycle ----------------

up:  ## Start everything in detached mode (influxdb + grafana + weather-sync)
	$(COMPOSE) up -d

up-data:  ## Start only InfluxDB + Grafana (no app containers) — useful for early smoke tests
	$(COMPOSE) up -d influxdb grafana

down:  ## Stop and remove all containers (keeps volumes, so data survives)
	$(COMPOSE) down

stop:  ## Stop services without removing containers
	$(COMPOSE) stop

restart:  ## Restart all services
	$(COMPOSE) restart

clean:  ## Stop everything and DELETE volumes (destroys InfluxDB + Grafana data!)
	$(COMPOSE) down -v

# ---------------- Build ----------------

build:  ## Build the app image (uses Docker layer cache)
	$(COMPOSE) build app-weather-sync

rebuild:  ## Force-rebuild the app image without cache (use after dep changes)
	$(COMPOSE) build --no-cache app-weather-sync

# ---------------- Inspection ----------------

ps:  ## List service status
	$(COMPOSE) ps

logs:  ## Tail logs from all services (Ctrl+C to exit)
	$(COMPOSE) logs -f --tail=100

logs-sync:  ## Tail logs from app-weather-sync only
	$(COMPOSE) logs -f --tail=200 app-weather-sync

logs-influx:  ## Tail logs from InfluxDB only
	$(COMPOSE) logs -f --tail=200 influxdb

logs-grafana:  ## Tail logs from Grafana only
	$(COMPOSE) logs -f --tail=200 grafana

# ---------------- Ad-hoc runs (one-shot, exit when done) ----------------

weather-sync-once:  ## Run one weather sync now (overrides scheduler, exits on completion)
	$(COMPOSE) run --rm -e WEATHER_SYNC_INTERVAL_SECONDS=0 app-weather-sync python -m app weather-sync

ge-report:  ## Run Great Expectations checks and print the DQ report to stdout
	$(COMPOSE) run --rm app-weather-sync python -m app ge-report
