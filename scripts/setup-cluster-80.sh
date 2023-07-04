#!/usr/bin/env bash
set -eu
export SINGLESTORE_IMAGE="singlestore/cluster-in-a-box:alma-8.0.19-f48780d261-4.0.11-1.16.0"
./scripts/setup-cluster.sh
