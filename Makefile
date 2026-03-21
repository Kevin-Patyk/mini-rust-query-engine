.DEFAULT_GOAL := help
SHELL=bash

.PHONY: help
help:  ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: fmt
fmt:  ## Run rustfmt
	cargo fmt --all

.PHONY: check
check:  ## Run cargo check
	cargo check --all

.PHONY: clippy
clippy:  ## Run clippy
	cargo clippy --all-targets -- -W clippy::dbg_macro

.PHONY: test
test:  ## Run all tests
	cargo test --all

.PHONY: fix
fix:  ## Run cargo fix
	cargo fix --allow-dirty

.PHONY: clean
clean:  ## Clean build artifacts
	cargo clean

.PHONY: all
all: fmt clippy test  ## Run fmt, clippy, and test
