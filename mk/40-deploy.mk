# Deployment via helmfile.
HELMFILE := helmfile --file $(ROOT)/deploy/helmfile.yaml.gotmpl --environment $(PROFILE)

.PHONY: deploy
deploy: ## Deploy / update every release (PROFILE=default|lite)
	@KUBECONFIG=$$($(KCTL) --kubeconfig-path) $(HELMFILE) sync

.PHONY: deploy-one
deploy-one: ## Deploy a single release: make deploy-one R=kafka-cluster
	@KUBECONFIG=$$($(KCTL) --kubeconfig-path) $(HELMFILE) --selector name=$(R) sync

.PHONY: template
template: ## Render all manifests without a cluster (used by CI)
	@$(HELMFILE) template --skip-deps >/dev/null && echo "helmfile template OK"
