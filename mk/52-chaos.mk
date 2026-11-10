# Chaos helper images (owner: controlboard experiments). The tag is the content hash of the
# image directory and is written to .tools/chaos-net-tag (not image-tags.yaml: `make images`
# rewrites that file from the Go binaries only). The controlboard reads it via CHAOS_NET_IMAGE.
CHAOS_NET_DIR := $(ROOT)/images/chaos-net
CHAOS_NET_TAG_FILE := $(ROOT)/.tools/chaos-net-tag

.PHONY: chaos-images
chaos-images: ## Build and push the chaos-net helper image; writes .tools/chaos-net-tag
	@tag=$$(cat $(CHAOS_NET_DIR)/Dockerfile $(CHAOS_NET_DIR)/chaos-net.sh | sha256sum | cut -c1-12); \
	if ! curl -sf -o /dev/null http://127.0.0.1:$(REGISTRY_PORT)/v2/chaos-net/manifests/$$tag -H 'Accept: application/vnd.docker.distribution.manifest.v2+json,application/vnd.oci.image.manifest.v1+json'; then \
	  docker build -q -t 127.0.0.1:$(REGISTRY_PORT)/chaos-net:$$tag $(CHAOS_NET_DIR) >/dev/null; \
	  docker push -q 127.0.0.1:$(REGISTRY_PORT)/chaos-net:$$tag >/dev/null; \
	  echo "  pushed chaos-net:$$tag"; \
	fi; \
	echo "$(REGISTRY)/chaos-net:$$tag" > $(CHAOS_NET_TAG_FILE); \
	cat $(CHAOS_NET_TAG_FILE)
