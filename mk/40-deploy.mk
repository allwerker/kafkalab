# Deployment via helmfile.
HELMFILE := helmfile --file $(ROOT)/deploy/helmfile.yaml.gotmpl --environment $(PROFILE)

.PHONY: deploy
deploy: namespaces ## Deploy / update every release (PROFILE=default|lite)
	@KUBECONFIG=$$($(KCTL) --kubeconfig-path) $(HELMFILE) sync

.PHONY: deploy-one
deploy-one: namespaces ## Deploy a single release: make deploy-one R=kafka-cluster
	@KUBECONFIG=$$($(KCTL) --kubeconfig-path) $(HELMFILE) --selector name=$(R) sync

.PHONY: template
template: ## Render all manifests without a cluster (used by CI)
	@$(HELMFILE) template >/dev/null && echo "helmfile template OK"

.PHONY: namespaces
namespaces:
	@$(KCTL) apply -f $(ROOT)/deploy/namespaces.yaml >/dev/null
