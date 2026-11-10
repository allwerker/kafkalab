# Container images for Go binaries: build on the host, wrap in distroless, push to the k3d registry.
# Every main package under services/ and controlboard/backend/cmd/ becomes one image.
ALL_IMAGE_DIRS = $(sort $(dir $(wildcard $(ROOT)/services/*/main.go $(ROOT)/controlboard/backend/cmd/*/main.go)))
# SERVICES="payment shipping" limits the build; tags of other images in .tools/image-tags.yaml are kept.
IMAGE_DIRS = $(if $(SERVICES),$(foreach s,$(SERVICES),$(filter %/$(s)/,$(ALL_IMAGE_DIRS))),$(ALL_IMAGE_DIRS))
PUSH_REGISTRY := 127.0.0.1:$(REGISTRY_PORT)
IMAGE_TAGS := $(ROOT)/.tools/image-tags.yaml

.PHONY: images
images: ## Build and push all Go service images; writes .tools/image-tags.yaml for helmfile
	@mkdir -p $(ROOT)/dist
	@if [ -f $(IMAGE_TAGS) ]; then grep -v '^images:' $(IMAGE_TAGS) > $(IMAGE_TAGS).old || true; else : > $(IMAGE_TAGS).old; fi
	@echo "images:" > $(IMAGE_TAGS).tmp
	@for dir in $(IMAGE_DIRS); do \
	  name=$$(basename $$dir); \
	  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -ldflags="-s -w" -o $(ROOT)/dist/$$name $$dir || exit 1; \
	  tag=$$(sha256sum $(ROOT)/dist/$$name | cut -c1-12); \
	  if ! curl -sf -o /dev/null http://$(PUSH_REGISTRY)/v2/$$name/manifests/$$tag -H 'Accept: application/vnd.docker.distribution.manifest.v2+json,application/vnd.oci.image.manifest.v1+json'; then \
	    docker build -q -f $(ROOT)/deploy/images/Dockerfile.go --build-arg BIN=$$name -t $(PUSH_REGISTRY)/$$name:$$tag $(ROOT)/dist >/dev/null || exit 1; \
	    docker push -q $(PUSH_REGISTRY)/$$name:$$tag >/dev/null || exit 1; \
	    echo "  pushed $$name:$$tag"; \
	  fi; \
	  echo "  $$name: \"$$tag\"" >> $(IMAGE_TAGS).tmp; \
	  sed -i "/^  $$name: /d" $(IMAGE_TAGS).old; \
	done
	@cat $(IMAGE_TAGS).old >> $(IMAGE_TAGS).tmp && mv $(IMAGE_TAGS).tmp $(IMAGE_TAGS) && rm -f $(IMAGE_TAGS).old
	@cat $(IMAGE_TAGS)
