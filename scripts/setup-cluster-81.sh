#!/usr/bin/env bash
set -eu
export SINGLESTORE_IMAGE="singlestore/cluster-in-a-box:alma-8.1.8-967dab3a92-4.0.12-1.16.1"
./scripts/setup-cluster.sh
