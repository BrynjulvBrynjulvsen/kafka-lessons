#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
exec ./gradlew -PkafkaDemoCore="${KAFKA_DEMO_CORE:-../kafka-demo}" bootRun "$@"
