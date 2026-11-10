# Monitoring as code: dashboards, alert rules, rule unit tests, runbooks (monitoring/dashgen).
DASHGEN := cd $(ROOT) && $(GO) run ./monitoring/dashgen
BUNDLE_ALERTS := $(ROOT)/monitoring/bundle/alerts
DEMO_ALERTS := $(ROOT)/monitoring/demo/alerts

.PHONY: dashboards
dashboards: ## Generate dashboards, alert rules, rule tests and runbooks (commit the output)
	@$(DASHGEN) -root $(ROOT)

.PHONY: alerts
alerts: dashboards ## Alias: rules, rule tests and runbooks are generated together with the dashboards

.PHONY: dashboards-lint
dashboards-lint: ## Lint dashboards, rules and metric catalog (queries parse, metrics documented, units, thresholds)
	@test -z "$$(gofmt -l $(ROOT)/monitoring/dashgen)" || (gofmt -l $(ROOT)/monitoring/dashgen; echo "gofmt needed"; exit 1)
	@$(DASHGEN) -root $(ROOT) -lint

.PHONY: monitoring-tools
monitoring-tools: ## Install vmalert-tool and promtool into .tools/bin
	@$(ROOT)/tools/bootstrap-monitoring.sh

# vmalert-tool runs every test file (bundle + demo, MetricsQL allowed); promtool additionally
# checks and tests the PromQL-portable bundle rules, proving they run on plain Prometheus.
.PHONY: alerts-test
alerts-test: monitoring-tools ## Unit-test the alert rules (vmalert-tool for all, promtool for the bundle)
	@for d in $(BUNDLE_ALERTS) $(DEMO_ALERTS); do \
	  cd $$d && for t in *.test.yaml; do \
	    out="$$($(TOOLS_BIN)/vmalert-tool unittest --disableAlertgroupLabel --files=$$t 2>&1)" || { echo "$$out"; echo "vmalert-tool: $$d/$$t FAILED"; exit 1; }; \
	    echo "vmalert-tool: $$t ok"; \
	  done; \
	done
	@cd $(BUNDLE_ALERTS) && for f in kafka-production.yaml kafka-demo.yaml; do \
	  $(TOOLS_BIN)/promtool check rules $$f >/dev/null || { $(TOOLS_BIN)/promtool check rules $$f; exit 1; }; \
	  $(TOOLS_BIN)/promtool test rules $${f%.yaml}.test.yaml >/dev/null || { $(TOOLS_BIN)/promtool test rules $${f%.yaml}.test.yaml; exit 1; }; \
	  echo "promtool: $$f checked and tested"; \
	done

lint: dashboards-lint
test: alerts-test
