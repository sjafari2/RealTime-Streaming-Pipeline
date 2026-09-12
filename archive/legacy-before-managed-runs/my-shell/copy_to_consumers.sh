#!/usr/bin/env bash
set -euo pipefail
# This replaces the old fixed 0..7 loop and checks the shared files in every pod.
exec bash "$(dirname "$0")/sync-code.sh" --role consumer
