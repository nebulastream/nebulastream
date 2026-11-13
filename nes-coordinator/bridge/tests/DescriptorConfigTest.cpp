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

#include <Identifiers/Identifiers.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sources/FileSourceConfig.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <PlanningTestUtil.hpp>
#include <TCPSink.hpp>

namespace NES
{
class DescriptorConfigTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("DescriptorConfigTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

///NOLINTBEGIN(bugprone-unchecked-optional-access)
TEST_F(DescriptorConfigTest, AnonymousMQTTSourceAcceptsEmptyMessageDelimiter)
{
    const auto prepared = loadAndBind(
        R"(
        CREATE WORKER 'worker-1:8080' SET ('worker-1:9090' AS DATA, 10 AS "MAX_OPERATORS");
        CREATE SINK output (id UINT64 NOT NULL) TYPE VOID SET ('worker-1:8080' AS "SINK"."HOST");
        )",
        R"(
        SELECT * FROM MQTT(
            'worker-1:8080' AS "SOURCE".HOST,
            'mqtt-source-test' AS "SOURCE".TOPIC,
            'mqtt-broker' AS "SOURCE".SERVER_URI,
            100 AS "SOURCE".FLUSH_INTERVAL_MS,
            '' AS "SOURCE".IMPLICIT_MESSAGE_DELIMITER,
            'CSV' AS INPUT_FORMATTER."TYPE",
            SCHEMA(id UINT64 NOT NULL) AS "SOURCE"."SCHEMA") INTO output)");
    auto plan = prepared.optimize();

    const auto sources = getOperatorByType<SourceDescriptorLogicalOperator>(plan[Host{"worker-1:8080"}].front());
    ASSERT_EQ(sources.size(), 1);
    EXPECT_EQ(sources.front().get().getSourceDescriptor().getSourceType(), Identifier::parse("MQTT"));
}

TEST_F(DescriptorConfigTest, PhysicalSourcePreservesStringConfigLiterals)
{
    for (const std::string path : {"", " ", "\n", "123", "true", "null"})
    {
        SCOPED_TRACE(path);
        const auto prepared = loadAndBind(
            fmt::format(
                R"(
                CREATE WORKER 'worker-1:8080' SET ('worker-1:9090' AS DATA, 10 AS "MAX_OPERATORS");
                CREATE LOGICAL SOURCE input (id UINT64 NOT NULL);
                CREATE PHYSICAL SOURCE FOR input TYPE File SET (
                    '{}' AS "SOURCE".FILE_PATH,
                    'worker-1:8080' AS "SOURCE".HOST,
                    2 AS "SOURCE".MAX_INFLIGHT_BUFFERS,
                    'CSV' AS INPUT_FORMATTER."TYPE",
                    '1' AS INPUT_FORMATTER.FIELD_DELIMITER,
                    '{}' AS INPUT_FORMATTER.TUPLE_DELIMITER);
                CREATE SINK output (id UINT64 NOT NULL) TYPE VOID SET ('worker-1:8080' AS "SINK"."HOST");
                )",
                path,
                "\n"),
            "SELECT * FROM input INTO output");
        auto plan = prepared.optimize();

        const auto sources = getOperatorByType<SourceDescriptorLogicalOperator>(plan[Host{"worker-1:8080"}].front());
        ASSERT_EQ(sources.size(), 1);
        const auto& source = sources.front().get().getSourceDescriptor();
        EXPECT_EQ(source.getPluginData().getAs<FileSourceConfig>().filePath.string(), path);
        EXPECT_EQ(source.getMaxInflightBuffers(), 2);
        EXPECT_EQ(source.getHost(), Host{"worker-1:8080"});
    }
}

TEST_F(DescriptorConfigTest, BindCreateTCPSink)
{
    const auto prepared = loadAndBind(
        R"(
        CREATE WORKER 'worker-1:8080' SET ('worker-1:9090' AS DATA, 10 AS "MAX_OPERATORS");
        CREATE LOGICAL SOURCE input (id UINT64 NOT NULL);
        CREATE PHYSICAL SOURCE FOR input TYPE File SET (
            'does_not_exist' AS "SOURCE".FILE_PATH,
            'worker-1:8080' AS "SOURCE".HOST,
            'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE SINK tcpOutput (id UINT64 NOT NULL) TYPE TCP SET (
            'worker-1:8080' AS "SINK".HOST,
            'tcp-client' AS "SINK".SOCKET_HOST,
            9000 AS "SINK".SOCKET_PORT,
            'CSV' AS "SINK".OUTPUT_FORMAT,
            4 AS "SINK".MAX_QUEUED_BUFFERS,
            1000 AS "SINK".CONNECT_TIMEOUT_MS,
            2000 AS "SINK".CLOSE_TIMEOUT_MS,
            2 AS "SINK".BACKPRESSURE_LOWER_THRESHOLD,
            4 AS "SINK".BACKPRESSURE_UPPER_THRESHOLD);
        )",
        "SELECT * FROM input INTO tcpOutput");
    auto plan = prepared.optimize();

    const auto sinks = getOperatorByType<SinkLogicalOperator>(plan[Host{"worker-1:8080"}].front());
    ASSERT_EQ(sinks.size(), 1);
    const auto sink = sinks.front().get().getSinkDescriptor();
    const auto [socketHost, socketPort, connectTimeoutMs, closeTimeoutMs, maxQueuedBuffers] = sink->getPluginData().getAs<TCPSinkConfig>();
    EXPECT_EQ(socketHost, "tcp-client");
    EXPECT_EQ(socketPort, 9000U);
    EXPECT_EQ(maxQueuedBuffers, 4U);
    EXPECT_EQ(connectTimeoutMs, 1000U);
    EXPECT_EQ(closeTimeoutMs, 2000U);
    EXPECT_EQ(sink->getBackpressureLowerThreshold(), 2U);
    EXPECT_EQ(sink->getBackpressureUpperThreshold(), 4U);
    EXPECT_EQ(sink->getFormatType(), "CSV");
}

///NOLINTEND(bugprone-unchecked-optional-access)
}
