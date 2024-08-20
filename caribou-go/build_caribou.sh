#!/bin/bash

# Building Caribou Go
go build -buildmode=c-shared -o caribougo.so ./src/main/

# Run all tests
go test ./... -gcflags=-l