#!/usr/bin/env bash
set -eu
export SINGLESTORE_IMAGE="singlestore/cluster-in-a-box:alma-7.6.14-6f67cb4355-4.0.4-1.13.6"
./scripts/setup-cluster.sh
