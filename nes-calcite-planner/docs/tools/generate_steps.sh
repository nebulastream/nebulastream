#!/usr/bin/env bash
# Regenerates docs/planner-steps.html from the planner jar (mvn package first). Usage: docs/tools/generate_steps.sh [java]
set -euo pipefail
cd "$(dirname "$0")/../.."
java=${1:-java}
json=$(mktemp)
"$java" -cp target/nes-calcite-planner.jar docs/tools/DumpSteps.java "$json" \
  "SELECT id, val * 2 AS v2 FROM events2 WHERE val > 20" \
  "SELECT id + 1 AS a, CASE WHEN val > 30 THEN val ELSE NULL END AS b FROM (SELECT * FROM events UNION ALL SELECT * FROM events2) WHERE id < 3" \
  "SELECT window_start, COUNT(*) AS n, SUM(val) AS s FROM TABLE(TUMBLE(TABLE events2, DESCRIPTOR(ts), INTERVAL '1' SECOND)) WHERE val > 5 GROUP BY window_start, window_end" \
  "SELECT window_start, id % 4 AS k, AVG(val) AS a FROM TABLE(HOP(TABLE events, DESCRIPTOR(ts), INTERVAL '1' SECOND, INTERVAL '2' SECOND)) GROUP BY window_start, window_end, id % 4" \
  "finite: SELECT id, val FROM events e WHERE val > (SELECT AVG(val) FROM events2 e2 WHERE e2.id = e.id)" \
  "finite: SELECT id FROM events e WHERE EXISTS (SELECT 1 FROM events2 e2 WHERE e2.id = e.id AND e2.val > e.val)"
python3 docs/tools/render_steps.py "$json" docs/planner-steps.html
rm -f "$json"
