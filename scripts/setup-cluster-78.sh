#!/usr/bin/env bash
set -eu
export SINGLESTORE_IMAGE="singlestore/cluster-in-a-box:alma-7.8.19-4263b2d130-4.0.10-1.14.4"
./scripts/setup-cluster.sh
