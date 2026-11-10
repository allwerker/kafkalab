# Controlboard web UI (React + Phaser) — controlboard/web. Requires Bun (>= 1.4) on the host.
WEB_DIR := $(ROOT)/controlboard/web
BUN     ?= bun

.PHONY: web-install web-lint web-test web-build web-smoke web-dev web-mock web-record

web-install: ## Install web UI dependencies (bun, frozen lockfile)
	@cd $(WEB_DIR) && $(BUN) install --frozen-lockfile

web-lint: web-install ## Type-check the web UI and enforce test naming (should…When… + GIVEN/WHEN/THEN)
	@cd $(WEB_DIR) && $(BUN) run lint

web-test: web-install ## Web UI unit tests with coverage thresholds (see controlboard/web/COVERAGE.md)
	@cd $(WEB_DIR) && $(BUN) run test

web-build: web-install ## Production build into controlboard/web/dist (embedded by the Go backend)
	@cd $(WEB_DIR) && $(BUN) run build

web-smoke: web-install ## Headless-browser smoke test against the mock backend (writes smoke/screenshot.png)
	@cd $(WEB_DIR) && $(BUN) run smoke

web-dev: web-install ## Run mock backend (:8090) and Vite dev server (:5173) together
	@cd $(WEB_DIR) && trap 'kill 0' EXIT; $(BUN) run mock & $(BUN) run dev

web-mock: web-install ## Run only the mock controlboard backend on :8090
	@cd $(WEB_DIR) && $(BUN) run mock

web-record: web-install ## Re-record controlboard/api/fixtures from the deterministic mock simulation
	@cd $(WEB_DIR) && $(BUN) run record

# Hook into the umbrella quality gates (recipes are defined elsewhere).
lint: web-lint
test: web-test
