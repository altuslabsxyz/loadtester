# loadtester build & config helpers.
#
#   make install   # build + install `loadtester` onto PATH (go install)
#   make config    # scaffold an editable target.yaml from the template
#   loadtester start            # reads target.yaml by default
#   loadtester start -t other.yaml

BINARY      := loadtester
TARGET_FILE := target.yaml
TEMPLATE    := target.example.yaml

.DEFAULT_GOAL := help

.PHONY: help build install config vet clean

help: ## Show available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

build: ## Build the binary into ./bin/loadtester
	go build -o bin/$(BINARY) .

install: ## Install loadtester onto PATH (go install -> $GOBIN / $GOPATH/bin)
	go install .
	@echo "installed $(BINARY) to $$(go env GOBIN 2>/dev/null || echo $$(go env GOPATH)/bin)"

config: ## Scaffold an editable target.yaml from the template (keeps an existing one)
	@if [ -f $(TARGET_FILE) ]; then \
		echo "$(TARGET_FILE) already exists; leaving it untouched."; \
	else \
		cp $(TEMPLATE) $(TARGET_FILE); \
		echo "created $(TARGET_FILE) from $(TEMPLATE) - edit it, then run '$(BINARY) start'."; \
	fi

vet: ## Run go vet
	go vet ./...

clean: ## Remove build artifacts
	rm -rf bin
