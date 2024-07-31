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
#include <BaseIntegrationTest.hpp>
#include <IntegrationTestUtil.hpp>
#include <SerializableDecomposedQueryPlan.pb.h>
#include <NebuLI.hpp>

namespace NES::Testing
{
struct QueryTestParam
{
    std::string cachedQueryPlanFile; /// cached query plan file .pb
    std::string testFile; /// test file .test
    uint64_t testId; /// test in file
};

class SystemTestFactory : public BaseIntegrationTest, public testing::WithParamInterface<QueryTestParam>
{
public:
    static void SetUpTestSuite()
    {
        BaseIntegrationTest::SetUpTestSuite();
        Logger::setupLogging("SystemTest.log", LogLevel::LOG_DEBUG);

        Configuration::SingleNodeWorkerConfiguration configuration{};
        configuration.queryCompilerConfiguration.nautilusBackend = QueryCompilation::NautilusBackend::MLIR_COMPILER_BACKEND;
        uut = std::make_unique<GRPCServer>(SingleNodeWorker{configuration});

        IntegrationTestUtil::removeFile(CMAKE_BINARY_DIR "/test/result/" SYSTEM_TEST_NAME ".csv");
    }
    static std::unique_ptr<GRPCServer> uut;
};
std::unique_ptr<GRPCServer> SystemTestFactory::uut = nullptr;

TEST_P(SystemTestFactory, SystemTest)
{
    const auto& [cachedQueryPlanFile, testFile, testId] = GetParam();
    SerializableDecomposedQueryPlan queryPlan;
    std::ifstream file(cachedQueryPlanFile);
    if (!file || !queryPlan.ParseFromIstream(&file))
    {
        NES_ERROR("Could not load protobuffer file: {}", cachedQueryPlanFile);
        GTEST_SKIP();
    }

    auto queryId = IntegrationTestUtil::registerQueryPlan(queryPlan, *SystemTestFactory::uut);
    IntegrationTestUtil::startQuery(queryId, *SystemTestFactory::uut);
    while (!IntegrationTestUtil::isQueryFinished(queryId, *SystemTestFactory::uut))
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    IntegrationTestUtil::unregisterQuery(queryId, *SystemTestFactory::uut);

    ASSERT_TRUE(NES::CLI::checkResult(testFile, SYSTEM_TEST_NAME, testId + 1));
}

INSTANTIATE_TEST_CASE_P(SystemTest, SystemTestFactory, testing::Values(SQLLogicTest_PLACEHOLDER));
} /// namespace NES::Testing