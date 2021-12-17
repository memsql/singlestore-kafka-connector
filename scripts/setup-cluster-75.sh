#!/usr/bin/env bash
set -eu
export SINGLESTORE_IMAGE="memsql/cluster-in-a-box:centos-7.5.8-12c73130aa-3.2.11-1.11.11"
./scripts/setup-cluster.sh
