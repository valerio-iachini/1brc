#!/usr/bin/env bash

perf record --call-graph dwarf cargo run --release
