#!/usr/bin/env bash
set -eu
export SINGLESTORE_IMAGE="singlestore/cluster-in-a-box:alma-8.0.15-0b9b66384f-4.0.11-1.15.2"
./scripts/setup-cluster.sh
