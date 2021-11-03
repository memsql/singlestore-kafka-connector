#!/usr/bin/env bash
set -eu
export SINGLESTORE_IMAGE="memsql/cluster-in-a-box:centos-7.1.13-11ddea2a3a-3.0.0-1.9.3"
./scripts/ensure-test-singlestore-cluster-password.sh
