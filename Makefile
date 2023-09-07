# Makefile for building Tork
GITCOMMIT:=$(shell git describe --dirty --always)
BINARY:=tork
SYSTEM:=
CHECKS:=check
BUILDOPTS:=-v
GOPATH?=$(HOME)/go
MAKEPWD:=$(dir $(realpath $(firstword $(MAKEFILE_LIST))))
CGO_ENABLED?=0

.PHONY: all
all: tork

.PHONY: tork
tork: 
	CGO_ENABLED=$(CGO_ENABLED) $(SYSTEM) go build $(BUILDOPTS) -ldflags="-s -w -X github.com/runabol/tork/version.GitCommit=$(GITCOMMIT)" -o $(BINARY) cmd/main.go 

.PHONY: clean
clean:
	go clean
	rm -f tork