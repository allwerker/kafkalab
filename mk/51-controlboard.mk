# Controlboard backend: embed the web UI build into the Go binary (controlboard/backend/web/dist).
CB_EMBED_DIR := $(ROOT)/controlboard/backend/web/dist

.PHONY: web-embed controlboard-image

web-embed: web-build ## Build the web UI and copy it into the backend for //go:embed
	@test -f $(WEB_DIR)/dist/index.html || (echo "web build produced no index.html"; exit 1)
	@find $(CB_EMBED_DIR) -mindepth 1 -delete
	@cp -R $(WEB_DIR)/dist/. $(CB_EMBED_DIR)/
	@echo "embedded $$(find $(CB_EMBED_DIR) -type f | wc -l) files into controlboard/backend/web/dist"

controlboard-image: web-embed images ## Build the UI, embed it and push all images (incl. controlboard)
