# Go: code generation, lint, tests, coverage gate, images.
GO_DIRS := pkg services controlboard/backend monitoring/dashgen tools/gocmd
GO_PKGS = $(foreach d,$(GO_DIRS),$(if $(wildcard $(ROOT)/$(d)),./$(d)/...))

.PHONY: gen
gen: ## Regenerate schemas/gen/*.avsc and pkg/events from schemas/*.avdl
	@$(ROOT)/tools/gen-events.sh

.PHONY: gen-check
gen-check: gen ## Fail if generated files are not committed
	@git -C $(ROOT) diff --exit-code -- schemas/gen pkg/events || (echo "generated files are stale: run make gen"; exit 1)

.PHONY: go-lint
go-lint:
	@test -z "$$(gofmt -l $(ROOT)/pkg $(ROOT)/services $(ROOT)/controlboard/backend 2>/dev/null)" || (gofmt -l $(ROOT)/pkg $(ROOT)/services $(ROOT)/controlboard/backend; echo "gofmt needed"; exit 1)
	@cd $(ROOT) && $(GO) vet $$($(GO) list $(GO_PKGS))
	@cd $(ROOT) && $(TOOLS_BIN)/staticcheck $$($(GO) list $(GO_PKGS))
	@python3 $(ROOT)/tools/lint-tests.py $(ROOT)/pkg $(ROOT)/services $(ROOT)/controlboard/backend $(ROOT)/monitoring

.PHONY: go-test
go-test: ## Go unit + integration tests (needs Docker for PostgreSQL), with race detector and coverage gate
	@cd $(ROOT) && $(GO) test -race -count=1 -coverprofile=coverage.out $$($(GO) list $(GO_PKGS))
	@cd $(ROOT) && $(TOOLS_BIN)/go-test-coverage --config .testcoverage.yml

.PHONY: lint test
lint: go-lint
test: go-test
