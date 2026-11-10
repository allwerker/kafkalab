# End-to-end tests against the running stack (make up first).
.PHONY: e2e
e2e: ## End-to-end checks of the running stack through the ingress
	@cd $(ROOT) && $(GO) test -tags e2e -count=1 -timeout 30m -v ./test/e2e/... 2>&1 | grep -E '^(=== RUN|--- |PASS|FAIL|ok|\s+\S+_test.go)'

.PHONY: footprint
footprint: ## Working-set memory per namespace and total
	@curl -s -H 'Host: vm.localhost' http://127.0.0.1:$(LB_PORT)/api/v1/query --data-urlencode 'query=sum by (namespace) (container_memory_working_set_bytes{container!="",container!="POD"})' \
	  | jq -r '.data.result[] | "\(.metric.namespace)\t\(.value[1] | tonumber / 1048576 | floor) MiB"' | sort -k2 -n -r
	@curl -s -H 'Host: vm.localhost' http://127.0.0.1:$(LB_PORT)/api/v1/query --data-urlencode 'query=sum(container_memory_working_set_bytes{container!="",container!="POD"})' \
	  | jq -r '.data.result[] | "TOTAL\t\(.value[1] | tonumber / 1073741824 * 100 | floor / 100) GiB"'
	@echo "k3d node container (k3s itself + all pods, as seen by Docker):"
	@docker stats --no-stream --format '  {{.Name}}  {{.MemUsage}}' k3d-$(CLUSTER_NAME)-server-0
