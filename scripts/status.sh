#!/usr/bin/env bash
set -euo pipefail
curl --fail --silent --show-error "${DEMO_URL:-http://localhost:8080}/api/topics"
