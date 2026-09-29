BINARY  := go-reseed
BIN_DIR := bin
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
GOFLAGS := -trimpath
PLATFORMS := linux/amd64 linux/arm64 darwin/amd64 darwin/arm64

.DEFAULT_GOAL := build

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: build
build: agents ## Build ./bin/go-reseed for this machine, plus the Linux agents it uploads
	@mkdir -p $(BIN_DIR)
	go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/$(BINARY) .
	@echo "built $(BIN_DIR)/$(BINARY) ($(VERSION))"

.PHONY: agents
agents: ## Build the Linux agent binaries (./bin/go-reseed-linux-{amd64,arm64})
	@mkdir -p $(BIN_DIR)
	@for arch in amd64 arm64; do \
		CGO_ENABLED=0 GOOS=linux GOARCH=$$arch go build $(GOFLAGS) -ldflags '$(LDFLAGS)' \
			-o $(BIN_DIR)/$(BINARY)-linux-$$arch . || exit 1; \
	done
	@echo "built $(BIN_DIR)/$(BINARY)-linux-amd64 and -arm64 (uploaded to hosts as the agent)"

.PHONY: release
release: ## Cross-compile for all platforms into ./bin/
	@mkdir -p $(BIN_DIR)
	@for p in $(PLATFORMS); do \
		os=$${p%/*}; arch=$${p#*/}; \
		echo "building $(BIN_DIR)/$(BINARY)-$$os-$$arch"; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch go build $(GOFLAGS) -ldflags '$(LDFLAGS)' \
			-o $(BIN_DIR)/$(BINARY)-$$os-$$arch . || exit 1; \
	done

.PHONY: test
test: ## Run unit tests with the race detector
	go test -race ./...

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: fmt
fmt: ## Format the code with gofmt
	gofmt -w .

.PHONY: fmt-check
fmt-check: ## Fail if any file needs gofmt
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "needs gofmt:"; echo "$$out"; exit 1; fi

.PHONY: check
check: fmt-check vet test ## Run fmt-check, vet and test

.PHONY: tidy
tidy: ## Tidy go.mod and go.sum
	go mod tidy

INSTALL_DIR := $(or $(shell go env GOBIN),$(shell go env GOPATH)/bin)

.PHONY: install
install: ## Install go-reseed and its Linux agents into $GOBIN (or $GOPATH/bin)
	go install $(GOFLAGS) -ldflags '$(LDFLAGS)' .
	@for arch in amd64 arm64; do \
		CGO_ENABLED=0 GOOS=linux GOARCH=$$arch go build $(GOFLAGS) -ldflags '$(LDFLAGS)' \
			-o $(INSTALL_DIR)/$(BINARY)-linux-$$arch . || exit 1; \
	done
	@echo "installed $(BINARY) and its agents in $(INSTALL_DIR)"

.PHONY: dry-run
dry-run: build ## Dry run with example hosts (override SOURCE, REPLICA, ARGS)
	$(BIN_DIR)/$(BINARY) --dry-run --source $(or $(SOURCE),db002) --replica $(or $(REPLICA),db003) $(ARGS)

TEST_COMPOSE := docker compose -f docker-compose.test.yml
LAB_KEY      := testlab/.ssh/id_ed25519

$(LAB_KEY):
	@mkdir -p $(dir $@)
	ssh-keygen -q -t ed25519 -N '' -C go-reseed-lab -f $@

.PHONY: test-docker
test-docker: ## Run vet and unit tests in a container
	$(TEST_COMPOSE) run --rm --build unit

.PHONY: test-integration
test-integration: $(LAB_KEY) ## Reseed the lab from a test container and verify it (LAB_VERBOSE=1 for xtrabackup output)
	$(TEST_COMPOSE) run --rm --build integration; rc=$$?; $(TEST_COMPOSE) down -v --remove-orphans >/dev/null 2>&1; exit $$rc

.PHONY: lab-up
lab-up: ## Start the Docker test lab (source + replica with an errant GTID)
	testlab/setup.sh

.PHONY: lab-test
lab-test: build ## Reseed the lab replica under write load and verify it (ARGS for extra flags)
	testlab/run-test.sh $(ARGS)

.PHONY: lab-down
lab-down: ## Destroy the test lab and its volumes
	$(TEST_COMPOSE) down -v --remove-orphans

.PHONY: clean
clean: ## Remove ./bin and run logs
	rm -rf $(BIN_DIR) go-reseed-*.log
