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
#include <filesystem>
#include <regex>
#include <string>
#include <gtest/gtest.h>
#include <IntegrationTestUtil.hpp>
#include <NebuLI.hpp>
#include <SerializableDecomposedQueryPlan.pb.h>

/// DEBUG option: set to true to log as well to stdout, otherwise only the log file is populated
auto constexpr LOG_TO_STDOUT = false;

using namespace NES;
class SystemTestTemplate : public testing::Test
{
public:
    static void SetUpTestSuite()
    {
        Configuration::SingleNodeWorkerConfiguration const configuration{};
        uut = std::make_unique<GRPCServer>(SingleNodeWorker{configuration});
    }
    static std::unique_ptr<GRPCServer> uut;
};
std::unique_ptr<GRPCServer> SystemTestTemplate::uut = nullptr;

class SystemTest : public SystemTestTemplate
{
public:
    explicit SystemTest(std::string systemTestName, std::string cachedQueryPlanFile, std::string testFile, uint64_t testId)
        : systemTestName(std::move(systemTestName))
        , cachedQueryPlanFile(std::move(cachedQueryPlanFile))
        , testFile(std::move(testFile))
        , testId(testId)
    {
    }

    void TestBody() override
    {
        /// We do not log to the console to reduce the amount of output. Instead we provide a link to the log. This is okay as these
        /// system tests are intended to run locally.
        std::time_t now_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        char timestamp[20]; /// Buffer large enough for "YYYY-MM-DD_HH-MM-SS"
        std::strftime(timestamp, sizeof(timestamp), "%Y-%m-%d_%H-%M-%S", std::localtime(&now_time_t));
        /// Ordering: time -> system test name -> test id
        std::string logFileName = std::string("SystemTest_") + timestamp + "_" + systemTestName + "_" + std::to_string(testId) + ".log";

        Logger::setupLogging(logFileName, LogLevel::LOG_DEBUG, LOG_TO_STDOUT);
        std::filesystem::path logPath = std::filesystem::current_path() / "test" / logFileName;
        /// file:// to make the link clickable in the console
        std::cout << "Find the test log at: file://" << logPath.string() << std::endl;

        IntegrationTestUtil::removeFile(CMAKE_BINARY_DIR "/test/result/" + systemTestName + std::to_string(testId + 1) + ".csv");

        SerializableDecomposedQueryPlan queryPlan;
        std::ifstream file(cachedQueryPlanFile);
        if (!file || !queryPlan.ParseFromIstream(&file))
        {
            NES_ERROR("Could not load protobuffer file: {}", cachedQueryPlanFile);
            GTEST_SKIP();
        }

        auto queryId = IntegrationTestUtil::registerQueryPlan(queryPlan, *SystemTestTemplate::uut);
        ASSERT_NE(queryId, INVALID_QUERY_ID);
        IntegrationTestUtil::startQuery(queryId, *SystemTestTemplate::uut);
        while (!IntegrationTestUtil::isQueryStopped(queryId, *SystemTestTemplate::uut))
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        IntegrationTestUtil::unregisterQuery(queryId, *SystemTestTemplate::uut);

        ASSERT_TRUE(NES::CLI::checkResult(testFile, systemTestName, testId + 1));
    }

private:
    const std::string systemTestName;
    const std::string cachedQueryPlanFile;
    const std::string testFile;
    const uint64_t testId;
};

namespace
{
std::vector<std::string> tokenize(std::string const& str)
{
    std::vector<std::string> result;
    std::istringstream iss(str);
    std::string word;
    while (iss >> word)
    {
        result.push_back(word);
    }
    return result;
}
}

int main(int argc, char** argv)
{
    testing::InitGoogleTest(&argc, argv);

    auto systemTestNames = tokenize(SYSTEM_TEST_NAME);
    auto systemTestFiles = tokenize(SYSTEM_TEST_FILE_PATH);

    NES_ASSERT(systemTestNames.size() == systemTestFiles.size(), "The number of system test names and files must be equal.");

    for (size_t j = 0; j < systemTestNames.size(); ++j)
    {
        const auto& systemTestName = systemTestNames[j];
        const auto& systemTestFile = systemTestFiles[j];

        uint64_t testNumber = 0;
        while (true)
        {
            auto cacheFilePath = CACHE_DIR + systemTestName + "_" + std::to_string(testNumber) + ".pb";
            std::filesystem::path const file = std::filesystem::path(cacheFilePath);

            if (!std::filesystem::is_regular_file(file))
            {
                break;
            }

            /// We register our value-parameterized tests programmatically
            /// Reference: https://google.github.io/googletest/advanced.html#registering-tests-programmatically
            testing::RegisterTest(
                "SystemTest",
                (systemTestName + "_" + std::to_string(testNumber)).c_str(),
                nullptr,
                nullptr,
                __FILE__,
                __LINE__,
                [=]() -> SystemTestTemplate* { return new SystemTest(systemTestName, cacheFilePath, systemTestFile, testNumber); });

            testNumber++;
        }
    }

    return RUN_ALL_TESTS();
}
