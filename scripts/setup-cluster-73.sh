#!/usr/bin/env bash
set -eu
export SINGLESTORE_IMAGE="memsql/cluster-in-a-box:centos-7.3.2-a364d4b31f-3.0.0-1.9.3"
./scripts/setup-cluster.sh
