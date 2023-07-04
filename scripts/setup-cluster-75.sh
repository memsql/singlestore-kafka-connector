#!/usr/bin/env bash
set -eu
export SINGLESTORE_IMAGE="singlestore/cluster-in-a-box:centos-7.5.12-3112a491c2-4.0.0-1.12.5"
./scripts/setup-cluster.sh
