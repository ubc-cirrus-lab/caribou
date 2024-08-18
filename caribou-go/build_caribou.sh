#!/bin/bash

# Building Caribou Go
go build -buildmode=c-shared -o caribougo.so ./src/main/

# Define pipes
PY_GO_PIPE="data_py_go"
GO_PY_PIPE="data_go_py"

# Check if the named pipes exists
if [ ! -p "$PY_GO_PIPE" ]; then
  # Create the named pipe
  mkfifo "$PY_GO_PIPE"
fi
if [ ! -p "$GO_PY_PIPE" ]; then
  # Create the named pipe
  mkfifo "$GO_PY_PIPE"
fi

# Run all tests
go test ./... -gcflags=-l