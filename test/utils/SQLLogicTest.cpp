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

#include <regex>
#include <string>
#include <filesystem>
//#include <BaseIntegrationTest.hpp>
#include <gtest/gtest.h>
#include <IntegrationTestUtil.hpp>
#include <NebuLI.hpp>
#include <SerializableDecomposedQueryPlan.pb.h>

using namespace NES;

class SystemTestFactory : public testing::Test
{
public:
    static void SetUpTestSuite()
    {
        ///BaseIntegrationTest::SetUpTestSuite();
        Logger::setupLogging("SystemTest.log", LogLevel::LOG_DEBUG);

        Configuration::SingleNodeWorkerConfiguration configuration{};
        uut = std::make_unique<GRPCServer>(SingleNodeWorker{configuration});

        IntegrationTestUtil::removeFile(CMAKE_BINARY_DIR "/test/result/" SYSTEM_TEST_NAME ".csv");
    }
    static std::unique_ptr<GRPCServer> uut;
};
std::unique_ptr<GRPCServer> SystemTestFactory::uut = nullptr;

class SystemTest : public SystemTestFactory
{
public:
    explicit SystemTest(std::string cachedQueryPlanFile, std::string testFile, uint64_t testId) : cachedQueryPlanFile(std::move(cachedQueryPlanFile)), testFile(std::move(testFile)),  testId(testId){}

    void TestBody() override
    {
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

private:
    std::string cachedQueryPlanFile;
    std::string testFile;
    uint64_t testId;
};

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);

    for(int64_t i = 0;; ++i) {
        auto cacheFilePath = CACHE_DIR  SYSTEM_TEST_NAME  "_" + std::to_string(i) + ".pb";
        std::filesystem::path const file = std::filesystem::path(cacheFilePath);

        if (!std::filesystem::is_regular_file(file)) {
            break;
        }

        /// We register our value-parameterized tests programmatically
        /// Reference: https://google.github.io/googletest/advanced.html#registering-tests-programmatically
        testing::RegisterTest(
            "SystemTest", (SYSTEM_TEST_NAME + std::to_string(i)).c_str(), nullptr, nullptr, __FILE__, __LINE__,
            [=]() -> SystemTestFactory* { return new SystemTest(cacheFilePath, SYSTEM_TEST_FILE_PATH, i); });
    }

    return RUN_ALL_TESTS();
}
