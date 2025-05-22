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

#include <chrono>
#include <thread>
#include <vector>
#include <Nautilus/NautilusBackend.hpp>
#include <fmt/core.h>
#include <grpcpp/support/status.h>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>
#include <GrpcService.hpp>
#include <IntegrationTestUtil.hpp>
#include <SingleNodeWorkerRPCService.pb.h>

namespace NES::Testing
{
using namespace ::testing;

/**
 * @brief Integration tests for the SingleNodeWorker. Tests query status after registration, running, stopping and unregistration.
 */
class SingleNodeIntegrationTest : public BaseIntegrationTest
{
public:
    static void SetUpTestCase()
    {
        BaseIntegrationTest::SetUpTestCase();
        Logger::setupLogging("SingleNodeIntegrationTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup SingleNodeIntegrationTest test class.");
    }

    const std::string dataInputFile = "oneToThirtyOneDoubleColumn.csv";
};

/// TODO (#34): Right now restarting a stopped query is not possible
TEST_F(SingleNodeIntegrationTest, DISABLED_TestQueryStatus)
{
    const auto* const resultFileName = "TestQueryStatus";
    const std::string queryInputFile = fmt::format("{}.bin", "qOneSourceCSV");
    const std::string queryResultFile = fmt::format("{}.csv", resultFileName);
    IntegrationTestUtil::removeFile(queryResultFile); /// remove outputFile if exists

    SerializableDecomposedQueryPlan queryPlan;
    const auto querySpecificDataFileName = fmt::format("{}_{}", "TestQueryStatus", dataInputFile);
    if (!IntegrationTestUtil::loadFile(queryPlan, queryInputFile, dataInputFile, querySpecificDataFileName))
    {
        GTEST_SKIP();
    }
    IntegrationTestUtil::replaceFileSinkPath(queryPlan, fmt::format("{}.csv", resultFileName));
    IntegrationTestUtil::replaceInputFileInFileSources(queryPlan, querySpecificDataFileName);


    Configuration::SingleNodeWorkerConfiguration configuration{};
    configuration.workerConfiguration.queryCompiler.nautilusBackend = Nautilus::Configurations::NautilusBackend::COMPILER;

    GRPCServer uut{SingleNodeWorker{configuration}};

    auto queryId = IntegrationTestUtil::registerQueryPlan(queryPlan, uut);
    IntegrationTestUtil::startQuery(queryId, uut);
    IntegrationTestUtil::stopQuery(queryId, StopQueryRequest::HardStop, uut);
    IntegrationTestUtil::startQuery(queryId, uut);
    IntegrationTestUtil::stopQuery(queryId, StopQueryRequest::HardStop, uut);
    IntegrationTestUtil::unregisterQuery(queryId, uut);

    auto reply = IntegrationTestUtil::querySummary(queryId, uut);
    EXPECT_EQ(reply.status(), QueryStatus::Stopped);
    EXPECT_EQ(reply.runs().size(), 1);
    EXPECT_FALSE(reply.runs().at(0).has_error());

    auto log = IntegrationTestUtil::queryLog(queryId, uut);
    const std::vector<QueryStatus> expected = {Registered, Running, Stopped, Running, Stopped};

    for (size_t i = 0; i < log.size(); ++i)
    {
        EXPECT_EQ(log[i].first, static_cast<Runtime::Execution::QueryStatus>(expected[i]));
    }
}

TEST_F(SingleNodeIntegrationTest, TestQueryStatusSimple)
{
    const auto* const resultFileName = "TestQueryStatusSimple";
    /// Todo 396: as soon as system level tests support multiple sources, we get rid of the CSV integration tests and cannot depend on this .bin anymore.
    const std::string queryInputFile = fmt::format("{}.bin", "qTwoCSVSourcesWithFilter");
    const std::string queryResultFile = fmt::format("{}.csv", resultFileName);
    IntegrationTestUtil::removeFile(queryResultFile); /// remove outputFile if exists

    SerializableDecomposedQueryPlan queryPlan;
    const auto querySpecificDataFileName = fmt::format("{}_{}", "TestQueryStatusSimple", dataInputFile);
    if (!IntegrationTestUtil::loadFile(queryPlan, queryInputFile, dataInputFile, querySpecificDataFileName))
    {
        GTEST_SKIP();
    }
    IntegrationTestUtil::replaceFileSinkPath(queryPlan, fmt::format("{}.csv", resultFileName));
    IntegrationTestUtil::replaceInputFileInFileSources(queryPlan, querySpecificDataFileName);

    Configuration::SingleNodeWorkerConfiguration configuration{};
    configuration.workerConfiguration.queryCompiler.nautilusBackend = Nautilus::Configurations::NautilusBackend::INTERPRETER;

    GRPCServer uut{SingleNodeWorker{configuration}};

    auto queryId = IntegrationTestUtil::registerQueryPlan(queryPlan, uut);
    IntegrationTestUtil::startQuery(queryId, uut);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    IntegrationTestUtil::stopQuery(queryId, StopQueryRequest::HardStop, uut);
    IntegrationTestUtil::unregisterQuery(queryId, uut);

    /// Test for query status after registration
    auto summary = IntegrationTestUtil::querySummary(queryId, uut);
    EXPECT_EQ(summary.status(), Stopped);
    EXPECT_EQ(summary.runs().size(), 1);
    EXPECT_FALSE(summary.runs().at(0).has_error());

    /// Test for invalid queryId
    auto invalidQueryId = QueryId{42};
    IntegrationTestUtil::querySummaryFailure(invalidQueryId, uut, grpc::NOT_FOUND);

    IntegrationTestUtil::removeFile(queryResultFile);
    IntegrationTestUtil::removeFile(querySpecificDataFileName);
}
}
