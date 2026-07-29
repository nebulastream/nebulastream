#!/usr/bin/env bats

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

source "$NES_BATS_LIB"

NES_REPL="$NES_REPL_EMBEDDED"

setup_file()    { nes_offline_setup_file; }
teardown_file() { nes_offline_teardown_file; }
setup()         { nes_offline_setup; }




@test "EXPLAIN over 2-node topology places sources upstream and join downstream" {
  run $NES_REPL -f JSON <tests/sql-file-tests/good/explain_distributed.sql
  [ "$status" -eq 0 ]

  # The six EXPLAIN statements sit at the tail of the JSON output stream.
  local n=${#lines[@]}
  local i_logical_text=$((n - 6))
  local i_optimized_text=$((n - 5))
  local i_distributed_text=$((n - 4))
  local i_logical_visual=$((n - 3))
  local i_optimized_visual=$((n - 2))
  local i_distributed_visual=$((n - 1))

  # `sed` inside extract_explain right-trims each line so VISUAL padding does not need to
  # live as trailing whitespace in the .bats source.
  extract_explain() {
    echo "$1" | jq -j '.[0].explain' | sed 's/[[:space:]]*$//'
  }

  assert_equal "$(extract_explain "${lines[$i_logical_text]}")" "$(cat <<'EOF'
== Initial Logical Plan ==
ANONYMOUS_SINK(AnonymousSink)
  PROJECTION(fields: [START, END, ID, VALUE, TIMESTAMP, ID2, VALUE2, TIMESTAMP2])
    PROJECTION(fields: [*])
      Join(INNER_JOIN, ID = ID2)
        WATERMARK_ASSIGNER(Event time)
          PROJECTION(fields: [*])
            SOURCE(STREAM)
        WATERMARK_ASSIGNER(Event time)
          PROJECTION(fields: [*])
            SOURCE(STREAM2)
EOF
)"

  assert_equal "$(extract_explain "${lines[$i_optimized_text]}")" "$(cat <<'EOF'
== Optimized Global Plan ==
SINK(VOID)
  Join(INNER_JOIN, ID = ID2)
    WATERMARK_ASSIGNER(Event time)
      SOURCE(STREAM)
    WATERMARK_ASSIGNER(Event time)
      SOURCE(STREAM2)
EOF
)"

  assert_equal "$(extract_explain "${lines[$i_distributed_text]}")" "$(cat <<'EOF'
== Decomposed Plans ==
-- 1 plan(s) on sink-node:8080 --
0:
SINK(VOID)
  Join(INNER_JOIN, ID = ID2)
    SOURCE(NETWORK)
    SOURCE(NETWORK)


-- 2 plan(s) on source-node:8080 --
0:
SINK(NETWORK)
  WATERMARK_ASSIGNER(Event time)
    SOURCE(STREAM)


1:
SINK(NETWORK)
  WATERMARK_ASSIGNER(Event time)
    SOURCE(STREAM2)
EOF
)"

  assert_equal "$(extract_explain "${lines[$i_logical_visual]}")" "$(cat <<'EOF'
== Initial Logical Plan ==

                ANONYMOUS_SINK(AnonymousSink)
                              │
PROJECTION(fields: [START, END, ID, VALUE, TIMESTAMP, ID2...
                              │
                   PROJECTION(fields: [*])
                              │
                 Join(INNER_JOIN, ID = ID2)
               ┌──────────────┴───────────────┐
WATERMARK_ASSIGNER(Event time) WATERMARK_ASSIGNER(Event time)
               │                              │
   PROJECTION(fields: [*])        PROJECTION(fields: [*])
              ┌┘                             ┌┘
       SOURCE(STREAM)                SOURCE(STREAM2)
EOF
)"

  assert_equal "$(extract_explain "${lines[$i_optimized_visual]}")" "$(cat <<'EOF'
== Optimized Global Plan ==

                         SINK(VOID)
                              │
                 Join(INNER_JOIN, ID = ID2)
               ┌──────────────┴───────────────┐
WATERMARK_ASSIGNER(Event time) WATERMARK_ASSIGNER(Event time)
              ┌┘                             ┌┘
       SOURCE(STREAM)                SOURCE(STREAM2)
EOF
)"

  assert_equal "$(extract_explain "${lines[$i_distributed_visual]}")" "$(cat <<'EOF'
== Decomposed Plans ==
-- 1 plan(s) on sink-node:8080 --
0:

          SINK(VOID)
               │
  Join(INNER_JOIN, ID = ID2)
       ┌───────┴───────┐
SOURCE(NETWORK) SOURCE(NETWORK)


-- 2 plan(s) on source-node:8080 --
0:

        SINK(NETWORK)
               │
WATERMARK_ASSIGNER(Event time)
               │
        SOURCE(STREAM)


1:

        SINK(NETWORK)
               │
WATERMARK_ASSIGNER(Event time)
               │
       SOURCE(STREAM2)
EOF
)"
}


@test "Verify that individual optimizer rules can successfully be disabled" {
  run $NES_REPL -f JSON <tests/sql-file-tests/good/disabled_optimizer_rule.sql
  [ "$status" -eq 0 ]

  # The six EXPLAIN statements sit at the tail of the JSON output stream.
  local n=${#lines[@]}
  local i_without_disabled_rules=$((n - 3))
  local i_set_disabled_rules=$((n - 2))
  local i_with_disabled_rules=$((n - 1))

  # `sed` inside extract_explain right-trims each line so VISUAL padding does not need to
  # live as trailing whitespace in the .bats source.
  extract_explain() {
    echo "$1" | jq -j '.[0].explain' | sed 's/[[:space:]]*$//'
  }

  assert_equal "$(extract_explain "${lines[$i_without_disabled_rules]}")" "$(cat <<'EOF'
== Optimized Global Plan ==
SINK(VOID)
  PROJECTION(fields: [ID])
    Join(INNER_JOIN, ID = ID2)
      SELECTION(ID % 2 = 0 AND ID > 2)
        WATERMARK_ASSIGNER(Event time)
          PROJECTION(fields: [ID, TIMESTAMP])
            SOURCE(STREAM)
      WATERMARK_ASSIGNER(Event time)
        PROJECTION(fields: [ID2, TIMESTAMP2])
          SOURCE(STREAM2)
EOF
)"

  assert_json_contains '[{"option":"optimizer.disabledRules","value":"PredicatePushdown,ProjectionPushdown"}]' "${lines[$i_set_disabled_rules]}"

  assert_equal "$(extract_explain "${lines[$i_with_disabled_rules]}")" "$(cat <<'EOF'
== Optimized Global Plan ==
SINK(VOID)
  PROJECTION(fields: [ID])
    SELECTION(ID > 2)
      Join(INNER_JOIN, ID = ID2)
        SELECTION(ID % 2 = 0)
          WATERMARK_ASSIGNER(Event time)
            SOURCE(STREAM)
        WATERMARK_ASSIGNER(Event time)
          SOURCE(STREAM2)
EOF
)"

}

