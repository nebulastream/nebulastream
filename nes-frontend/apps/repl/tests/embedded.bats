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

@test "nes-repl shows help" {
  run $NES_REPL --help
  [ "$status" -eq 0 ]
}

@test "basic test" {
  ls >&3
  run $NES_REPL -f JSON <tests/sql-file-tests/good/test_large.sql
  [ "$status" -eq 0 ]
  [ ${#lines[@]} -eq 8 ]

  assert_json_equal '[{"schema":[{"name":"TS","type":"UINT64"}],"source_name":"ENDLESS"}]' "${lines[0]}"
  assert_json_equal '[{"host":"localhost:8080","input_formatter_config":{"allowCommasInStrings":true,"fieldDelimiter":44,"tupleDelimiter":10,"valueDeserializers":"","type":"CSV"},"physical_source_id":1,"schema":[{"name":"TS","type":"UINT64"}],"source_config":"{\"seed\":1,\"maxRuntime\":10000000,\"generatorSchemaRaw\":\"SEQUENCE UINT64 0 10000000 1\",\"stopGeneratorWhenSequenceFinishes\":0,\"flushInterval\":10,\"generatorRateConfig\":{\"index\":0,\"value\":{\"emitRate\":10.0}}}","source_name":"ENDLESS","source_type":"GENERATOR"}]' "${lines[1]}"
  assert_json_equal '[{"host":"localhost:8080","output_formatter_config":{"config":{"fieldDelimiter":",","quoteStrings":false,"tupleDelimiter":"\n","valueSerializers":""},"outputFormatterType":"CSV"},"schema":[{"name":"TS","type":"UINT64"}],"sink_config":"{\"filePath\":\"out.csv\",\"append\":false}","sink_name":"SOMESINK","sink_type":"FILE"}]' "${lines[2]}"
  assert_json_equal '[]' "${lines[3]}"
  QUERY_ID=$(echo ${lines[4]} | jq -r '.[0].query_id')

  # One global and one local query
  echo "${lines[5]}" | jq -e '(. | length) == 2'
  echo "${lines[5]}" | jq -e '.[].query_status | test("^Running|Registered|Started$")'

  assert_json_equal "[{\"query_id\":\"${QUERY_ID}\"}]" "${lines[6]}"
  assert_json_contains "[]" "${lines[7]}"
}

@test "show version reports the embedded worker build info" {
  run $NES_REPL -f JSON <tests/sql-file-tests/good/show_version.sql
  [ "$status" -eq 0 ]

  [ "$(echo "${lines[0]}" | jq -r '.[0].worker')" = "localhost:8080" ]
  version=$(echo "${lines[0]}" | jq -r '.[0].version')
  [[ "$(echo "$version" | sed -n '1p')" == "nes-single-node-worker "* ]]
  echo "$version" | grep -q "commit:"
}

@test "launch multiple queries distributed" {
  run $NES_REPL -f JSON <tests/sql-file-tests/good/multiple_queries_distributed.sql
  [ "$status" -eq 0 ]
}

@test "launch bad query should fail distributed" {
  run $NES_REPL -f JSON <tests/sql-file-tests/bad/invalid_projection_distributed.sql
  [ "$status" -ne 0 ]
  grep "invalid query syntax" nes-repl.log
}

@test "launch multiple queries" {
  run $NES_REPL -f JSON <tests/sql-file-tests/good/multiple_queries.sql
  [ "$status" -eq 0 ]
}

@test "launch bad query should fail" {
  run $NES_REPL -f JSON <tests/sql-file-tests/bad/invalid_projection.sql
  [ "$status" -ne 0 ]
  grep "invalid query syntax" nes-repl.log
}

@test "Fail on invalid optimizer config name" {
  run $NES_REPL -- --optimizer.test_invalid_config_name=INVALID
  [ "$status" -ne 0 ]
  grep -i "Unresolvable fields: optimizer.test_invalid_config_name" nes-repl.log
}

@test "Fail on invalid optimizer config value" {
  run $NES_REPL -- --optimizer.join_strategy=INVALID
  [ "$status" -ne 0 ]
  grep "Invalid join strategy, must be NESTED_LOOP_JOIN, HASH_JOIN or OPTIMIZER_CHOOSES: INVALID" nes-repl.log
}



@test "EXPLAIN over 2-node topology places the pipelines on both workers" {
  run $NES_REPL -f JSON <tests/sql-file-tests/good/explain_distributed.sql
  [ "$status" -eq 0 ]

  # The EXPLAIN statement is the last line of the JSON output stream.
  assert_equal "$(echo "${lines[-1]}" | jq -j '.[0].explain' | sed 's/[[:space:]]*$//')" "$(cat <<'EOF'
== Placed Pipelines ==
on sink-node:8080:
Sink(names=[[START, END, ID, VALUE, TIMESTAMP, ID2, VALUE2, TIMESTAMP2]])
  Emit(fields=[[START, END, ID, VALUE, TIMESTAMP, ID2, VALUE2, TIMESTAMP2]], layout=[ROW(START, END, ID, VALUE, TIMESTAMP, ID2, VALUE2, TIMESTAMP2)])
    Scan(fields=[[START, END, ID, VALUE, TIMESTAMP, ID2, VALUE2, TIMESTAMP2]], layout=[ROW(START, END, ID, VALUE, TIMESTAMP, ID2, VALUE2, TIMESTAMP2)])
      NetworkSource(from=[source-node:8080], channel=[channel-1])
on source-node:8080:
NetworkSink(to=[sink-node:8080], channel=[channel-1])
  Emit(fields=[[START, END, ID, VALUE, TIMESTAMP, ID2, VALUE2, TIMESTAMP2]], layout=[ROW(START, END, ID, VALUE, TIMESTAMP, ID2, VALUE2, TIMESTAMP2)])
    JoinProbe(join=[1], type=[INNER], implementation=[HASH_JOIN], kind=[TUMBLING], size=[1000], condition=[=($0, $3)], fields=[[START, END, ID, VALUE, TIMESTAMP, ID2, VALUE2, TIMESTAMP2]])
      JoinBuild(join=[1], side=[left], time=[TIMESTAMP], stored=[[ID, VALUE, TIMESTAMP]])
        WatermarkAssigner(time=[TIMESTAMP])
          Scan(fields=[[ID, VALUE, TIMESTAMP]], layout=[CSV])
            SourceScan(table=[[STREAM]], physicalSource=[1])
      JoinBuild(join=[1], side=[right], time=[TIMESTAMP2], stored=[[ID2, VALUE2, TIMESTAMP2]])
        WatermarkAssigner(time=[TIMESTAMP2])
          Scan(fields=[[ID2, VALUE2, TIMESTAMP2]], layout=[CSV])
            SourceScan(table=[[STREAM2]], physicalSource=[2])
EOF
)"
}

