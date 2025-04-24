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


#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <ostream>
#include <random>
#include <span>
#include <string>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/NautilusBackend.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/core.h>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>
#include <GrpcService.hpp>
#include <IntegrationTestUtil.hpp>
#include <SerializableDecomposedQueryPlan.pb.h>
#include <SingleNodeWorkerConfiguration.hpp>

namespace NES::Testing
{
using namespace ::testing;

struct QueryTestParam
{
    std::string queryFile;
    size_t expectedNumTuplesKey1{0};
    size_t expectedNumTuplesKey2{0};
    size_t expectedCheckSumKey1{0};
    size_t expectedCheckSumKey2{0};
    float mean{0};
    float stddev{0};
    uint64_t seed{0};

    friend std::ostream& operator<<(std::ostream& os, const QueryTestParam& param)
    {
        return os << "QueryTestParam{queryFile: \"" << param.queryFile << "\", expectedNumTuplesKey1: " << param.expectedNumTuplesKey1
                  << ", expectedNumTuplesKey2: " << param.expectedNumTuplesKey2 << ", expectedCheckSumKey1: " << param.expectedCheckSumKey1
                  << ", expectedCheckSumKey2: " << param.expectedCheckSumKey2 << "}";
    }
};

class SingleNodeIntegrationTestSequence : public BaseIntegrationTest, public testing::WithParamInterface<QueryTestParam>
{
public:
    static void SetUpTestSuite()
    {
        BaseIntegrationTest::SetUpTestSuite();
        Logger::setupLogging("SingleNodeIntegrationTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup SingleNodeIntegrationTest test class.");
    }

    const std::string idFieldName = "default_source$id";
    const std::string dataInputFile = "oneToThirtyOneDoubleColumn.csv";
};

class SingleNodeIntegrationTestNormalDistribution : public BaseIntegrationTest, public testing::WithParamInterface<QueryTestParam>
{
public:
    static void SetUpTestSuite()
    {
        BaseIntegrationTest::SetUpTestSuite();
        Logger::setupLogging("SingleNodeIntegrationTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup SingleNodeIntegrationTest test class.");
    }

    const std::string idFieldName = "default_source$id";
    const std::string dataInputFile = "oneToThirtyOneDoubleColumn.csv";
};

TEST_P(SingleNodeIntegrationTestSequence, IntegrationTestWithGeneratorSourceSequenceStop)
{
    struct ResultSchema
    {
        uint64_t key1;
        uint64_t key2;
    };

    const auto& [queryName, expectedNumTuplesKey1, expectedNumTuplesKey2, expectedCheckSumKey1, expectedCheckSumKey2, unusedParameter1, unusedParameter2, unusedParameter3]
        = GetParam();
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

    Configuration::SingleNodeWorkerConfiguration configuration;
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

    uint64_t numProcessedTuplesKey1 = 0;
    uint64_t numProcessedTuplesKey2 = 0;
    size_t checkSum1 = 0; /// simple summation of all values
    size_t checkSum2 = 0;
    for (const auto& buffer : buffers)
    {
        for (const auto& [key1, key2] : std::span(buffer.getBuffer<ResultSchema>(), buffer.getNumberOfTuples()))
        {
            numProcessedTuplesKey1++;
            numProcessedTuplesKey2++;
            checkSum1 += key1;
            checkSum2 += key2;
        }
    }

    EXPECT_EQ(numProcessedTuplesKey1, expectedNumTuplesKey1) << "Query did not produce the expected number of tuples";
    EXPECT_EQ(numProcessedTuplesKey2, expectedNumTuplesKey2) << "Query did not produce the expected number of tuples";
    EXPECT_EQ(checkSum1, expectedCheckSumKey1) << "Query did not produce the expected expected checksum";
    EXPECT_EQ(checkSum2, expectedCheckSumKey2) << "Query did not produce the expected expected checksum";

    if (!::testing::Test::HasFailure())
    {
        std::cout << "Test had no failures! Removing files!" << '\n';
        IntegrationTestUtil::removeFile(testSpecificResultFileName);
        IntegrationTestUtil::removeFile(testSpecificDataFileName);
    }
}

TEST_P(SingleNodeIntegrationTestNormalDistribution, NormalDistributionParameters)
{
    struct ResultSchema
    {
        uint64_t id;
        double data;
    } __attribute__((packed));

    const auto& [queryName, expectedNumTuplesKey1, expectedNumTuplesKey2, expectedCheckSumKey1, expectedCheckSumKey2, mean, stddev, seed]
        = GetParam();

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

    uint64_t numProcessedTuplesKey1 = 0;
    uint64_t numProcessedTuplesKey2 = 0;
    size_t checkSum1 = 0; /// simple summation of all values
    float actualMean = 0;
    float actualStdDev = 0;
    std::map<uint64_t, double> values;
    std::vector<uint64_t> ids;
    ids.reserve(expectedNumTuplesKey1);
    for (const auto& buffer : buffers)
    {
        for (const auto& [id, data] : std::span(buffer.getBuffer<ResultSchema>(), buffer.getNumberOfTuples()))
        {
            numProcessedTuplesKey1++;
            numProcessedTuplesKey2++;
            ids.emplace_back(id);
            checkSum1 += id;
            actualMean += data;
            /// Calculate the std dev based on the expected mean. If the actual mean does not match the expected mean the actual std dev is meaningless
            actualStdDev += (mean - static_cast<float>(data)) * (mean - static_cast<float>(data));
            values.emplace(id, data);
        }
    }

    std::ranges::sort(ids);
    for (size_t i = 1; i < ids.size(); i++)
    {
        EXPECT_EQ(ids[i - 1], ids[i] - 1) << ids[i] << " is not the successor to " << ids[i - 1];
    }

    actualMean = actualMean / static_cast<float>(numProcessedTuplesKey1);
    actualStdDev = std::sqrt(actualStdDev / static_cast<float>(numProcessedTuplesKey1));
    EXPECT_EQ(numProcessedTuplesKey1, numProcessedTuplesKey2);
    EXPECT_EQ(numProcessedTuplesKey1, expectedNumTuplesKey1);
    EXPECT_EQ(expectedCheckSumKey1, checkSum1);

    EXPECT_NEAR(actualMean, mean, 0.1F) << "Mean does not match";
    EXPECT_NEAR(actualStdDev, stddev, 0.1F) << "StdDev does not match";
}

INSTANTIATE_TEST_CASE_P(
    GeneratorTests,
    SingleNodeIntegrationTestSequence,
    testing::Values(
        QueryTestParam{"qOneGeneratorSourceStopAll", 200, 200, 4950 + (100 * 100), 19900, 0.0F, 0.0F, 0},
        QueryTestParam{"qOneGeneratorSourceStopOne", 100, 100, 4950, 4950, 0.0F, 0.0F, 0}));

INSTANTIATE_TEST_CASE_P(
    GeneratorTests,
    SingleNodeIntegrationTestNormalDistribution,
    testing::Values(QueryTestParam{
        "qOneGeneratorSource", 100000, 100000, static_cast<size_t>(99999) * static_cast<size_t>(100000) / 2, 0, 0.0F, 15.0F, 1}));

}
