#!/usr/bin/env bash
set -euo pipefail
project_root=$(cd "$(dirname "$0")/.." && pwd)
core_root=${KAFKA_DEMO_CORE:-"$project_root/../kafka-demo"}
core_root=$(cd "$core_root" && pwd)
docker run --rm \
  -v "$project_root:/projects/kafka-lessons" -v "$core_root:/projects/kafka-demo" \
  -v kafka-demo-gradle-cache:/home/gradle/.gradle -w /projects/kafka-lessons \
  gradle:8.14.3-jdk21 gradle --no-daemon -PkafkaDemoCore=../kafka-demo bootJar
docker build -t kafka-lessons:local "$project_root"
