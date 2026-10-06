# Single entry point for the handbook. Run `make help` for the list of targets.
SHELL := /bin/bash

COMPOSE_FILE := infra/docker-compose.yml
# PROFILE=sentinel|cluster adds the optional Redis topologies to `make up`.
PROFILE ?=
COMPOSE := docker compose -f $(COMPOSE_FILE)

# golangci-lint: use the installed binary, else run a pinned version through `go run`.
GOLANGCI_LINT_VERSION := v2.14.0
GOLANGCI_LINT := $(or $(shell command -v golangci-lint),go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION))

TOPICS := $(wildcard [0-9][0-9]-*/)

.PHONY: help up down lab-ts lab-go test test-ts test-go test-scripts lint lint-ts lint-go docs-check

help:
	@echo "make up [PROFILE=sentinel|cluster]   start brokers (docker compose up -d --wait)"
	@echo "make down                            stop brokers and remove volumes"
	@echo "make lab-ts LAB=<NN-ten/lab-NN-ten>  run a TypeScript lab demo"
	@echo "make lab-go LAB=<NN-ten/lab-NN-ten>  run a Go lab demo"
	@echo "make test | test-ts | test-go        run tests (needs 'make up' first for labs)"
	@echo "make lint                            eslint, prettier, tsc, golangci-lint, no-sleep gate, compose check"
	@echo "make docs-check                      validate theory.md and lab READMEs"

up:
	$(COMPOSE) $(if $(PROFILE),--profile $(PROFILE)) up -d --wait

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

test-ts:
	pnpm exec vitest run

test-go:
	# -p 1: packages share one Redis, and lab-03-eviction changes its maxmemory settings for a few seconds.
	go test -p 1 ./...

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
