#!/usr/bin/env bash
set -eu
export MEMSQL_IMAGE="memsql/cluster-in-a-box:6.7.18-db1caffe94-1.6.1-1.1.1"
./scripts/ensure-test-memsql-cluster.sh
