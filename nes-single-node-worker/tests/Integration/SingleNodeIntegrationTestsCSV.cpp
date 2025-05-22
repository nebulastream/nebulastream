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


#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <numeric>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/NautilusBackend.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <fmt/core.h>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>
#include <GrpcService.hpp>
#include <IntegrationTestUtil.hpp>
#include <SingleNodeWorkerRPCService.pb.h>

namespace NES::Testing
{
using namespace ::testing;

struct QueryTestParam
{
    std::string queryFile;
    int expectedNumTuples;
    int expectedCheckSum;

    /// Add this method to your QueryTestParam struct
    friend std::ostream& operator<<(std::ostream& os, const QueryTestParam& param)
    {
        return os << "QueryTestParam{queryFile: \"" << param.queryFile << "\", expectedTuples: " << param.expectedNumTuples << "}";
    }
};

class SingleNodeIntegrationTest : public BaseIntegrationTest, public testing::WithParamInterface<QueryTestParam>
{
public:
    static void SetUpTestCase()
    {
        BaseIntegrationTest::SetUpTestCase();
        Logger::setupLogging("SingleNodeIntegrationTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup SingleNodeIntegrationTest test class.");
    }

    const std::string idFieldName = "default_source$id";
    const std::string dataInputFile = "oneToThirtyOneDoubleColumn.csv";
};

/// Takes a serialized fully-specified query plan, executes it and checks if the correct results are produced.
TEST_P(SingleNodeIntegrationTest, IntegrationTestWithSourcesCSV)
{
    using ResultSchema = struct
    {
        uint64_t id;
        uint64_t id2;
    };

    const auto& [queryName, expectedNumTuples, expectedCheckSum] = GetParam();
    const auto testSpecificIdentifier = IntegrationTestUtil::getUniqueTestIdentifier();
    const auto testSpecificResultFileName = fmt::format("{}.csv", testSpecificIdentifier);
    const auto testSpecificDataFileName = fmt::format("{}_{}", testSpecificIdentifier, dataInputFile);

    const std::string queryInputFile = fmt::format("{}.bin", queryName);
    IntegrationTestUtil::removeFile(testSpecificResultFileName);

    SerializableDecomposedQueryPlan queryPlan;

    if (!IntegrationTestUtil::loadFile(queryPlan, queryInputFile, dataInputFile, testSpecificDataFileName))
    {
        GTEST_SKIP();
    }
    IntegrationTestUtil::replaceFileSinkPath(queryPlan, testSpecificResultFileName);
    IntegrationTestUtil::replaceInputFileInFileSources(queryPlan, testSpecificDataFileName);

    Configuration::SingleNodeWorkerConfiguration configuration{};
    configuration.workerConfiguration.queryCompiler.nautilusBackend = Nautilus::Configurations::NautilusBackend::COMPILER;

    GRPCServer uut{SingleNodeWorker{configuration}};

    auto queryId = IntegrationTestUtil::registerQueryPlan(queryPlan, uut);
    ASSERT_NE(queryId.getRawValue(), QueryId::INVALID);
    IntegrationTestUtil::startQuery(queryId, uut);
    IntegrationTestUtil::waitForQueryToEnd(queryId, uut);
    IntegrationTestUtil::unregisterQuery(queryId, uut);

    auto bufferManager = Memory::BufferManager::create();
    const auto sinkSchema = IntegrationTestUtil::loadSinkSchema(queryPlan);
    auto buffers = IntegrationTestUtil::createBuffersFromCSVFile(testSpecificResultFileName, sinkSchema, *bufferManager, 0, "", true);

    size_t numProcessedTuples = 0;
    size_t checkSum = 0; /// simple summation of all values
    for (const auto& buffer : buffers)
    {
        numProcessedTuples += buffer.getNumberOfTuples();
        for (size_t i = 0; i < buffer.getNumberOfTuples(); ++i)
        {
            checkSum += buffer.getBuffer<ResultSchema>()[i].id;
        }
    }
    EXPECT_EQ(numProcessedTuples, expectedNumTuples) << "Query did not produce the expected number of tuples";
    EXPECT_EQ(checkSum, expectedCheckSum) << "Query did not produce the expected expected checksum";

    IntegrationTestUtil::removeFile(testSpecificResultFileName);
    IntegrationTestUtil::removeFile(testSpecificDataFileName);
}

INSTANTIATE_TEST_CASE_P(
    QueryTests,
    SingleNodeIntegrationTest,
    testing::Values(
        /// Todo 396: as soon as system level tests support multiple sources, we can get rid of the CSV integration tests
        QueryTestParam{"qTwoCSVSourcesWithFilter", 62, 960 /*  2 * (SUM(0, 1, ..., 32) - 16) */}));
}
