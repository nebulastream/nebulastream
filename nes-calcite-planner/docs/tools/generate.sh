#!/usr/bin/env bash
# Regenerates docs/physical-plans.html from the planner jar (mvn package first). Usage: docs/tools/generate.sh [java]
set -euo pipefail
cd "$(dirname "$0")/../.."
java=${1:-java}
json=$(mktemp)
"$java" -cp target/nes-calcite-planner.jar docs/tools/DumpPlans.java "$json" \
  "SELECT id, val * 2 AS v2 FROM events WHERE val > 20" \
  "SELECT ts, id FROM events WHERE id = 3 AND val <> 30" \
  "SELECT id, val FROM events UNION ALL SELECT id, val FROM events2" \
  "SELECT id + 1 AS a, CASE WHEN val > 30 THEN val ELSE NULL END AS b FROM (SELECT * FROM events UNION ALL SELECT * FROM events2) WHERE id < 3" \
  "SELECT id, CASE WHEN val > 20 THEN val END AS v FROM events UNION ALL SELECT id, val FROM events2"
python3 docs/tools/render.py "$json" docs/physical-plans.html
rm -f "$json"
