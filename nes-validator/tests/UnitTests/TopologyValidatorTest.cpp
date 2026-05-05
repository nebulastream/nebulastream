/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <string>

#include <Validator/TopologyValidator.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace NES
{

class TopologyValidatorTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("TopologyValidatorTest.log", LogLevel::LOG_DEBUG);
    }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

// ============================================================================
// YAML Fixture Strings
// ============================================================================

/// Complete valid topology: logical source, physical source, sink, and valid SQL query.
static constexpr auto validTopologyYaml = R"(
query: |
  SELECT * FROM SENSOR INTO VOID_SINK
sinks:
  - name: VOID_SINK
    host: node1:8080
    schema:
      - name: SENSOR$ID
        type: INT64
      - name: SENSOR$VALUE
        type: FLOAT64
      - name: SENSOR$TIMESTAMP
        type: INT64
    type: Void
    config: {}
    parser_config: {}
logical:
  - name: sensor
    schema:
      - name: id
        type: INT64
      - name: value
        type: FLOAT64
      - name: timestamp
        type: INT64
physical:
  - logical: sensor
    host: node1:8080
    type: Generator
    parser_config:
      type: CSV
      fieldDelimiter: ","
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 10
      stop_generator_when_sequence_finishes: NONE
      seed: 1
      generator_schema: |
        SEQUENCE INT64 0 100 1
        NORMAL_DISTRIBUTION FLOAT64 0 1
        SEQUENCE INT64 0 100000 1000
workers:
  - host: node1:8080
    data_address: node1:9090
    max_operators: 10000
)";

/// Query references a source name that does not exist in logical sources.
static constexpr auto missingSourceYaml = R"(
query: |
  SELECT * FROM NONEXISTENT_SOURCE INTO VOID_SINK
sinks:
  - name: VOID_SINK
    host: node1:8080
    schema:
      - name: SENSOR$ID
        type: INT64
    type: Void
    config: {}
    parser_config: {}
logical:
  - name: sensor
    schema:
      - name: id
        type: INT64
physical:
  - logical: sensor
    host: node1:8080
    type: Generator
    parser_config:
      type: CSV
      fieldDelimiter: ","
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 10
      stop_generator_when_sequence_finishes: NONE
      seed: 1
      generator_schema: |
        SEQUENCE INT64 0 100 1
workers:
  - host: node1:8080
    data_address: node1:9090
    max_operators: 10000
)";

/// Invalid SQL syntax in the query (SELCT instead of SELECT, FORM instead of FROM).
static constexpr auto invalidSqlYaml = R"(
query: |
  SELCT * FORM sensor INTO VOID_SINK
sinks:
  - name: VOID_SINK
    schema:
      - name: SENSOR$ID
        type: INT64
    type: Void
    config: {}
    parser_config: {}
logical:
  - name: sensor
    schema:
      - name: id
        type: INT64
physical:
  - logical: sensor
    type: Generator
    parser_config:
      type: CSV
      fieldDelimiter: ","
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 10
      stop_generator_when_sequence_finishes: NONE
      seed: 1
      generator_schema: |
        SEQUENCE INT64 0 100 1
)";

/// Sources and sinks but no queries (valid case -- queries are optional).
static constexpr auto noQueryYaml = R"(
sinks:
  - name: VOID_SINK
    host: node1:8080
    schema:
      - name: SENSOR$ID
        type: INT64
    type: Void
    config: {}
    parser_config: {}
logical:
  - name: sensor
    schema:
      - name: id
        type: INT64
physical:
  - logical: sensor
    host: node1:8080
    type: Generator
    parser_config:
      type: CSV
      fieldDelimiter: ","
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 10
      stop_generator_when_sequence_finishes: NONE
      seed: 1
      generator_schema: |
        SEQUENCE INT64 0 100 1
workers:
  - host: node1:8080
    data_address: node1:9090
    max_operators: 10000
)";

/// Topology with multiple queries provided as a YAML sequence. Both queries
/// must produce the same output schema since they share VOID_SINK.
static constexpr auto multipleQueriesYaml = R"(
query:
  - SELECT * FROM SENSOR INTO VOID_SINK
  - SELECT * FROM SENSOR INTO VOID_SINK
sinks:
  - name: VOID_SINK
    host: node1:8080
    schema:
      - name: SENSOR$ID
        type: INT64
      - name: SENSOR$VALUE
        type: FLOAT64
      - name: SENSOR$TIMESTAMP
        type: INT64
    type: Void
    config: {}
    parser_config: {}
logical:
  - name: sensor
    schema:
      - name: id
        type: INT64
      - name: value
        type: FLOAT64
      - name: timestamp
        type: INT64
physical:
  - logical: sensor
    host: node1:8080
    type: Generator
    parser_config:
      type: CSV
      fieldDelimiter: ","
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 10
      stop_generator_when_sequence_finishes: NONE
      seed: 1
      generator_schema: |
        SEQUENCE INT64 0 100 1
        NORMAL_DISTRIBUTION FLOAT64 0 1
        SEQUENCE INT64 0 100000 1000
workers:
  - host: node1:8080
    data_address: node1:9090
    max_operators: 10000
)";

/// Topology with a physical source referencing a non-existent logical source.
static constexpr auto orphanedPhysicalSourceYaml = R"(
sinks:
  - name: VOID_SINK
    host: node1:8080
    schema:
      - name: SENSOR$ID
        type: INT64
    type: Void
    config: {}
    parser_config: {}
logical:
  - name: sensor
    schema:
      - name: id
        type: INT64
physical:
  - logical: NONEXISTENT_LOGICAL
    host: node1:8080
    type: Generator
    parser_config:
      type: CSV
      fieldDelimiter: ","
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 10
      stop_generator_when_sequence_finishes: NONE
      seed: 1
      generator_schema: |
        SEQUENCE INT64 0 100 1
workers:
  - host: node1:8080
    data_address: node1:9090
    max_operators: 10000
)";

/// Topology with an invalid data type name in schema.
static constexpr auto invalidDataTypeYaml = R"(
sinks:
  - name: VOID_SINK
    host: node1:8080
    schema:
      - name: SENSOR$ID
        type: INT64
    type: Void
    config: {}
    parser_config: {}
logical:
  - name: sensor
    schema:
      - name: id
        type: INVALID_TYPE
physical:
  - logical: sensor
    host: node1:8080
    type: Generator
    parser_config:
      type: CSV
      fieldDelimiter: ","
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 10
      stop_generator_when_sequence_finishes: NONE
      seed: 1
      generator_schema: |
        SEQUENCE INT64 0 100 1
workers:
  - host: node1:8080
    data_address: node1:9090
    max_operators: 10000
)";

// ============================================================================
// Tests
// ============================================================================

TEST_F(TopologyValidatorTest, ValidTopologyReturnsEmptyString)
{
    auto result = Validator::validateTopology(validTopologyYaml);
    EXPECT_EQ(result, "") << "Valid topology should return empty string, got: " << result;
}

TEST_F(TopologyValidatorTest, MissingLogicalSourceReturnsError)
{
    auto result = Validator::validateTopology(missingSourceYaml);
    EXPECT_NE(result, "") << "Missing source should return non-empty error string";
    EXPECT_THAT(result, testing::HasSubstr("NONEXISTENT_SOURCE"));
}

TEST_F(TopologyValidatorTest, InvalidSqlSyntaxReturnsError)
{
    auto result = Validator::validateTopology(invalidSqlYaml);
    EXPECT_NE(result, "") << "Invalid SQL syntax should return non-empty error string";
}

TEST_F(TopologyValidatorTest, EmptyYamlReturnsError)
{
    auto result = Validator::validateTopology("");
    EXPECT_NE(result, "") << "Empty YAML should return non-empty error string";
}

TEST_F(TopologyValidatorTest, ValidTopologyNoQueriesReturnsEmpty)
{
    auto result = Validator::validateTopology(noQueryYaml);
    EXPECT_EQ(result, "") << "Topology with no queries should be valid, got: " << result;
}

TEST_F(TopologyValidatorTest, MultipleQueriesAllValid)
{
    auto result = Validator::validateTopology(multipleQueriesYaml);
    EXPECT_EQ(result, "") << "Multiple valid queries should return empty string, got: " << result;
}

TEST_F(TopologyValidatorTest, OrphanedPhysicalSourceReturnsError)
{
    auto result = Validator::validateTopology(orphanedPhysicalSourceYaml);
    EXPECT_NE(result, "") << "Physical source referencing non-existent logical source should return error";
    EXPECT_THAT(result, testing::HasSubstr("NONEXISTENT_LOGICAL"));
}

TEST_F(TopologyValidatorTest, InvalidDataTypeReturnsError)
{
    auto result = Validator::validateTopology(invalidDataTypeYaml);
    EXPECT_NE(result, "") << "Invalid data type name should return error";
    EXPECT_THAT(result, testing::HasSubstr("INVALID_TYPE"));
}

TEST_F(TopologyValidatorTest, MalformedYamlReturnsError)
{
    auto result = Validator::validateTopology("this is not valid yaml: [[[");
    EXPECT_NE(result, "") << "Malformed YAML should return error";
}

// ============================================================================
// Config Field Validation Tests (Plan 08-02)
// ============================================================================

/// File source missing the required file_path config parameter should be rejected.
TEST_F(TopologyValidatorTest, FileSourceMissingFilePathReturnsError)
{
    static constexpr auto yaml = R"(
sinks: []
logical:
  - name: test_source
    schema:
      - name: id
        type: UINT64
physical:
  - logical: test_source
    host: node1:8080
    type: File
    parser_config:
      type: CSV
      fieldDelimiter: ","
    source_config: {}
workers:
  - host: node1:8080
    data_address: node1:9090
    max_operators: 10000
)";
    auto result = Validator::validateTopology(yaml);
    EXPECT_NE(result, "") << "File source with empty config should return error";
    EXPECT_THAT(result, testing::HasSubstr("file_path")) << "Error should mention missing file_path, got: " << result;
}

/// TCP source missing required socket_host and socket_port should be rejected.
TEST_F(TopologyValidatorTest, TCPSourceMissingHostPortReturnsError)
{
    static constexpr auto yaml = R"(
sinks: []
logical:
  - name: test_source
    schema:
      - name: id
        type: UINT64
physical:
  - logical: test_source
    host: node1:8080
    type: TCP
    parser_config:
      type: CSV
      fieldDelimiter: ","
    source_config: {}
workers:
  - host: node1:8080
    data_address: node1:9090
    max_operators: 10000
)";
    auto result = Validator::validateTopology(yaml);
    EXPECT_NE(result, "") << "TCP source with empty config should return error";
    // TCP requires socket_host and socket_port (both have no default)
    EXPECT_THAT(result, testing::AnyOf(testing::HasSubstr("socket_host"), testing::HasSubstr("socket_port")))
        << "Error should mention missing TCP config parameter, got: " << result;
}

/// File source with valid file_path should pass config validation (no file_path error).
TEST_F(TopologyValidatorTest, FileSourceWithFilePathPassesValidation)
{
    static constexpr auto yaml = R"(
sinks: []
logical:
  - name: test_source
    schema:
      - name: id
        type: UINT64
physical:
  - logical: test_source
    host: node1:8080
    type: File
    parser_config:
      type: CSV
      fieldDelimiter: ","
    source_config:
      file_path: /tmp/test.csv
workers:
  - host: node1:8080
    data_address: node1:9090
    max_operators: 10000
)";
    auto result = Validator::validateTopology(yaml);
    EXPECT_EQ(result, "") << "File source with valid file_path should pass, got: " << result;
}

} // namespace NES
