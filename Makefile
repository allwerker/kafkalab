# Kafka Lab — top-level Makefile. Area targets live in mk/*.mk (one file per owner to avoid merge conflicts).
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
include $(ROOT)/tools/versions.env
export

TOOLS_BIN := $(ROOT)/.tools/bin
GO        := $(ROOT)/.tools/go/bin/go
export PATH := $(TOOLS_BIN):$(ROOT)/.tools/go/bin:$(PATH)
export GOTOOLCHAIN := local
export GOPATH := $(ROOT)/.tools/gopath
export GOFLAGS := -mod=mod

CLUSTER_NAME  ?= kafka-lab
LB_PORT       ?= 8080
API_PORT      ?= 6550
REGISTRY_PORT ?= 5111
PROFILE       ?= default
REGISTRY      := $(CLUSTER_NAME)-registry.localhost:$(REGISTRY_PORT)
KCTL          := $(ROOT)/kctl
export CLUSTER_NAME LB_PORT API_PORT REGISTRY_PORT PROFILE REGISTRY

include $(sort $(wildcard $(ROOT)/mk/*.mk))

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_.-]+:.*?## ' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

.PHONY: bootstrap
bootstrap: ## Install the pinned toolchain into .tools/ (no sudo)
	@$(ROOT)/tools/bootstrap.sh

.PHONY: preflight
preflight: ## Check host prerequisites
	@$(ROOT)/tools/preflight.sh

.PHONY: ci
ci: lint test ## Full quality gate (what GitHub Actions runs)
