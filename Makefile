# Single entry point for the handbook. Run `make help` for the list of targets.
SHELL := /bin/bash

COMPOSE_FILE := infra/docker-compose.yml
# PROFILE adds the optional Redis topologies to `make up`: one profile (PROFILE=sentinel) or several
# (PROFILE="sentinel cluster"; chapter 04 needs both).
PROFILE ?=
COMPOSE := docker compose -f $(COMPOSE_FILE)

# golangci-lint: use the installed binary, else run a pinned version through `go run`.
GOLANGCI_LINT_VERSION := v2.14.0
GOLANGCI_LINT := $(or $(shell command -v golangci-lint),go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION))

TOPICS := $(wildcard [0-9][0-9]-*/)

# Chapter 04 labs that need the sentinel and cluster profiles. They hold every chaos test
# (describe("chaos") in TypeScript, TestChaos* in Go), which stops and restarts Redis containers.
# `make test-fast` skips these labs, `make test-chaos` runs only these labs.
TOPOLOGY_LABS := 04-redis-advanced/lab-01-sentinel 04-redis-advanced/lab-02-cluster

.PHONY: help up down lab-ts lab-go test test-ts test-go test-scripts test-fast test-fast-ts test-fast-go test-chaos test-chaos-ts test-chaos-go lint lint-ts lint-go docs-check mermaid-check

help:
	@echo "make up [PROFILE=...]               start brokers (docker compose up -d --wait)"
	@echo "                                     PROFILE=sentinel, PROFILE=cluster or PROFILE=\"sentinel cluster\" (chapter 04)"
	@echo "make down                            stop brokers and remove volumes"
	@echo "make lab-ts LAB=<NN-ten/lab-NN-ten>  run a TypeScript lab demo"
	@echo "make lab-go LAB=<NN-ten/lab-NN-ten>  run a Go lab demo"
	@echo "make test | test-ts | test-go        run all tests (needs 'make up PROFILE=\"sentinel cluster\"' first)"
	@echo "make test-fast                       all tests except the sentinel and cluster labs (needs plain 'make up')"
	@echo "make test-chaos                      only the sentinel and cluster labs, chaos tests included (needs PROFILE=\"sentinel cluster\")"
	@echo "make lint                            eslint, prettier, tsc, golangci-lint, no-sleep gate, compose check"
	@echo "make docs-check                      validate theory.md and lab READMEs"
	@echo "make mermaid-check [FILES=...]       check that every mermaid block parses (mmdc, needs Chromium from pnpm install)"

up:
	$(COMPOSE) $(foreach p,$(PROFILE),--profile $(p)) up -d --wait

down:
	$(COMPOSE) --profile sentinel --profile cluster down -v --remove-orphans

lab-ts:
	@test -n "$(LAB)" || { echo "usage: make lab-ts LAB=<NN-ten/lab-NN-ten>"; exit 2; }
	pnpm exec tsx $(LAB)/ts/demo.ts

lab-go:
	@test -n "$(LAB)" || { echo "usage: make lab-go LAB=<NN-ten/lab-NN-ten>"; exit 2; }
	go run ./$(LAB)/go/demo

test: test-scripts test-ts test-go

test-scripts:
	bash scripts/check-docs.test.sh
	bash scripts/check-compose.test.sh
	bash scripts/check-no-sleep.test.sh
	bash scripts/check-tsconfig.test.sh
	bash scripts/check-mermaid.test.sh

test-ts:
	pnpm exec vitest run

test-go:
	# -p 1: packages share one Redis, and lab-03-eviction changes its maxmemory settings for a few seconds.
	go test -p 1 ./...

# Fast loop and CI: everything that runs on the base stack (no sentinel or cluster profile).
test-fast: test-fast-ts test-fast-go

test-fast-ts:
	pnpm exec vitest run $(foreach lab,$(TOPOLOGY_LABS),--exclude '$(lab)/**')

test-fast-go:
	go test -p 1 $$(go list ./... | grep -v $(foreach lab,$(TOPOLOGY_LABS),-e '/$(lab)/go'))

# Chaos and topology tests: need `make up PROFILE="sentinel cluster"`. Sequential, they share the topology.
test-chaos: test-chaos-ts test-chaos-go

test-chaos-ts:
	pnpm exec vitest run $(TOPOLOGY_LABS)

test-chaos-go:
	go test -p 1 $(foreach lab,$(TOPOLOGY_LABS),./$(lab)/...)

lint: lint-ts lint-go
	bash scripts/check-no-sleep.sh
	bash scripts/check-compose.sh

lint-ts:
	pnpm exec eslint .
	pnpm exec prettier --check .
	pnpm exec tsc --noEmit

lint-go:
	@test -z "$$(gofmt -l .)" || { echo "gofmt needed:"; gofmt -l .; exit 1; }
	$(GOLANGCI_LINT) run ./...

docs-check:
	bash scripts/check-docs.sh $(TOPICS)

mermaid-check:
	bash scripts/check-mermaid.sh $(FILES)
