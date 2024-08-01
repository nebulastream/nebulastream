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
#include <TestUtils/UtilityFunctions.hpp>
#include <fmt/core.h>
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

/**
 * @brief Integration tests for the SingleNodeWorker. Tests entire code path from registration to running the query, stopping and
 * unregistration.
 */
class SingleNodeIntegrationTest : public BaseIntegrationTest, public testing::WithParamInterface<QueryTestParam>, public IntegrationTestUtil
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

/**
 * @brief Takes a serialized fully-specified query plan, executes it and checks if the correct results are produced.
 */
TEST_P(SingleNodeIntegrationTest, TestQueryRegistration)
{
    using ResultSchema = struct
    {
        uint64_t id;
        uint64_t id2;
    };

    const auto& [queryname, expectedNumTuples, expectedCheckSum] = GetParam();
    const std::string queryInputFile = fmt::format("{}.bin", queryname);
    const std::string queryResultFile = fmt::format("{}.csv", queryname);
    removeFile(queryResultFile); /// remove outputFile if exists

    SerializableDecomposedQueryPlan queryPlan;
    if (!loadFile(queryPlan, queryInputFile, dataInputFile))
    {
        GTEST_SKIP();
    }
    replaceFileSinkPath(queryPlan, fmt::format("{}.csv", queryname));

    Configuration::SingleNodeWorkerConfiguration configuration{};
    configuration.queryCompilerConfiguration.nautilusBackend = QueryCompilation::NautilusBackend::MLIR_COMPILER_BACKEND;

    GRPCServer uut{SingleNodeWorker{configuration}};

    auto queryId = registerQueryPlan(queryPlan, uut);
    startQuery(queryId, uut);
    unregisterQuery(queryId, uut);

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>();
    const auto sinkSchema = loadSinkSchema(queryPlan);
    auto buffers = Runtime::Execution::Util::createBuffersFromCSVFile(queryResultFile, sinkSchema, bufferManager, 0, "", true);

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

    removeFile(queryResultFile);
    removeFile(dataInputFile);
}

INSTANTIATE_TEST_CASE_P(
    QueryTests,
    SingleNodeIntegrationTest,
    testing::Values(
        QueryTestParam{"qOneCSVSource", 32, 496 /* SUM(0, 1, ..., 31) */},
        QueryTestParam{"qOneCSVSourceWithFilter", 16, 120 /* SUM(0, 1, ..., 15) */},
        QueryTestParam{"qTwoCSVSourcesWithFilter", 32, 240 /* 2*SUM(0, 1, ..., 15) */}));
} /// namespace NES::Testing