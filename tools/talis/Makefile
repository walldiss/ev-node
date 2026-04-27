# Build helpers for the talis fibre-experiment deploy.
#
# Targets:
#   make build-bins         build all three: celestia-appd, celestia, evnode-fibre
#   make build-app          celestia-appd from $(CELESTIA_APP_REPO) at $(CELESTIA_APP_REF)
#   make build-node         celestia from $(CELESTIA_NODE_REPO) at $(CELESTIA_NODE_REF)
#   make build-evnode       evnode-fibre runner from this repo (tools/celestia-node-fiber/cmd/evnode-fibre)
#   make clean              wipe build/
#
# All binaries are cross-compiled to linux/amd64 — talis instances are
# Ubuntu 24.04 amd64 regardless of provider — and dropped into ./build/.
# Pass that directory to `talis genesis -b ./build` and the validator,
# bridge, and ev-node deploys all pick up the same artefacts.
#
# Repo locations and refs are overridable via env / make var, with
# sensible defaults that match the rest of fibre-experiment:
#
#   CELESTIA_APP_REPO   default: ../../../celestia-app  (sibling clone)
#   CELESTIA_APP_REF    default: feat/fibre-payments
#   CELESTIA_NODE_REPO  default: ../../../celestia-node (sibling clone)
#   CELESTIA_NODE_REF   default: feature/fibre-experimental
#
# If the repos are not cloned next to this checkout, override the path
# variables at the make invocation:
#
#   make build-bins CELESTIA_APP_REPO=/path/to/celestia-app

CELESTIA_APP_REPO  ?= ../../../celestia-app
CELESTIA_APP_REF   ?= feat/fibre-payments

CELESTIA_NODE_REPO ?= ../../../celestia-node
CELESTIA_NODE_REF  ?= feature/fibre-experimental

EVNODE_RUNNER_PATH ?= ../celestia-node-fiber/cmd/evnode-fibre

BUILD_DIR := $(CURDIR)/build

GOOS   ?= linux
GOARCH ?= amd64

# -trimpath strips local paths from binaries so they're reproducible
# across machines. -s -w drops debug + symbol tables — saves ~30% size,
# matters for upload over slow links.
GOFLAGS_COMMON := -trimpath -ldflags='-s -w'

.PHONY: build-bins build-app build-node build-evnode clean check-repos

build-bins: build-app build-node build-evnode
	@echo
	@echo "✅ All binaries built into $(BUILD_DIR):"
	@ls -lh $(BUILD_DIR)

$(BUILD_DIR):
	@mkdir -p $(BUILD_DIR)

# celestia-appd from celestia-app's feat/fibre-payments. We compile
# from cmd/celestia-appd; the make target in celestia-app's Makefile
# does the same plus version-stamps but we don't need that for the
# experiment (any commit on feat/fibre-payments will do).
build-app: $(BUILD_DIR) check-repos
	@echo "==> Building celestia-appd ($(CELESTIA_APP_REF))"
	@cd $(CELESTIA_APP_REPO) && \
		git fetch --quiet origin $(CELESTIA_APP_REF) && \
		git checkout --quiet $(CELESTIA_APP_REF) && \
		GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 \
		go build $(GOFLAGS_COMMON) -o $(BUILD_DIR)/celestia-appd ./cmd/celestia-appd
	@echo "    -> $(BUILD_DIR)/celestia-appd"

# celestia (bridge / light node binary) from celestia-node's
# feature/fibre-experimental.
build-node: $(BUILD_DIR) check-repos
	@echo "==> Building celestia ($(CELESTIA_NODE_REF))"
	@cd $(CELESTIA_NODE_REPO) && \
		git fetch --quiet origin $(CELESTIA_NODE_REF) && \
		git checkout --quiet $(CELESTIA_NODE_REF) && \
		GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 \
		go build $(GOFLAGS_COMMON) -o $(BUILD_DIR)/celestia ./cmd/celestia
	@echo "    -> $(BUILD_DIR)/celestia"

# evnode-fibre is the long-lived ev-node aggregator wired to
# celestia-node-fiber. Lives next to the adapter so it inherits the
# adapter's go.mod and avoids dragging celestia-node into testapp's
# tree. We rename the artefact to "evnode" so talis genesis --build-dir
# picks it up under that name (same convention as celestia / celestia-appd).
build-evnode: $(BUILD_DIR)
	@echo "==> Building evnode-fibre (this repo, current HEAD)"
	@cd $(EVNODE_RUNNER_PATH) && \
		GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 \
		go build $(GOFLAGS_COMMON) -o $(BUILD_DIR)/evnode .
	@echo "    -> $(BUILD_DIR)/evnode"

# Sanity check: make sure the sibling clones exist so we fail with a
# friendly message instead of a deep go-tool error.
check-repos:
	@if [ ! -d "$(CELESTIA_APP_REPO)/.git" ]; then \
		echo "ERROR: celestia-app repo not found at $(CELESTIA_APP_REPO)"; \
		echo "       set CELESTIA_APP_REPO=/path/to/celestia-app or clone the repo there"; \
		exit 1; \
	fi
	@if [ ! -d "$(CELESTIA_NODE_REPO)/.git" ]; then \
		echo "ERROR: celestia-node repo not found at $(CELESTIA_NODE_REPO)"; \
		echo "       set CELESTIA_NODE_REPO=/path/to/celestia-node or clone the repo there"; \
		exit 1; \
	fi

clean:
	@rm -rf $(BUILD_DIR)
