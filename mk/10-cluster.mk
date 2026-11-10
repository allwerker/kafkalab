# Cluster lifecycle (k3d).
DATA_DIR := $(ROOT)/.data/$(CLUSTER_NAME)
CLUSTER_CONFIG := $(ROOT)/.tools/k3d-$(CLUSTER_NAME).yaml

.PHONY: cluster
cluster: preflight ## Create (or start) the k3d cluster
	@mkdir -p $(DATA_DIR)
	@DATA_DIR=$(DATA_DIR) envsubst < $(ROOT)/deploy/k3d/cluster.yaml.tpl > $(CLUSTER_CONFIG)
	@if k3d cluster list $(CLUSTER_NAME) >/dev/null 2>&1; then \
	  k3d cluster start $(CLUSTER_NAME) --wait; \
	else \
	  k3d cluster create --config $(CLUSTER_CONFIG); \
	fi
	@grep -q "$(CLUSTER_NAME)-registry.localhost" /etc/hosts 2>/dev/null || \
	  getent hosts $(CLUSTER_NAME)-registry.localhost >/dev/null || \
	  echo "note: '$(CLUSTER_NAME)-registry.localhost' does not resolve; images are pushed via 127.0.0.1:$(REGISTRY_PORT)"
	@$(KCTL) get nodes

.PHONY: up
up: cluster images deploy ## Create cluster, build images, deploy the whole stack
	@echo "Kafka Lab is up: http://board.localhost:$(LB_PORT)"

.PHONY: down
down: ## Stop the cluster (keeps all data)
	k3d cluster stop $(CLUSTER_NAME)

.PHONY: reset
reset: nuke up ## Delete everything and rebuild from scratch

.PHONY: nuke
nuke: ## Delete cluster, registry and persistent data
	-k3d cluster delete $(CLUSTER_NAME)
	-docker run --rm -v $(ROOT)/.data:/d alpine:3 rm -rf /d/$(CLUSTER_NAME)
	rm -f $(ROOT)/.tools/kubeconfig-$(CLUSTER_NAME)

.PHONY: status
status: ## Show cluster, pods and URLs
	@$(ROOT)/tools/kubectl-plugins/kubectl-lab status
