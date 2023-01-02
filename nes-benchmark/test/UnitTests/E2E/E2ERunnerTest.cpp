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

#include <gtest/gtest.h>
#include <Util/Logger/Logger.hpp>
#include <NesBaseTest.hpp>
#include <E2E/E2ERunner.hpp>


namespace NES::Benchmark {
    class E2ERunnerTest : public Testing::NESBaseTest {
    public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("E2ERunnerTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup E2ERunnerTest test class.");
        }

        /* Will be called before a test is executed. */
        void SetUp() override {
            Testing::NESBaseTest::SetUp();
            NES_INFO("Setup E2ERunnerTest test case.");
        }

        /* Will be called after all tests in this class are finished. */
        static void TearDownTestCase() {
            NES_INFO("Tear down E2ERunnerTest test class.");
        }
    };

    TEST_F(E2ERunnerTest, filterOneSource) {
        std::string configPath = std::string(TEST_CONFIGS_DIRECTORY) + "/filter_one_source.yaml";
        std::string logPath = "E2ERunnerTest_filterOneSource.log";

        auto e2EBenchmarkConfig = parseYamlConfig(configPath, logPath);
        NES::Benchmark::writeHeaderToCsvFile(e2EBenchmarkConfig);

        for (auto& configPerRun : e2EBenchmarkConfig.getAllConfigPerRuns()) {
            NES::Benchmark::runSingleRun(configPerRun, e2EBenchmarkConfig.getConfigOverAllRuns(), *rpcCoordinatorPort, *restPort);
        }
    }

    TEST_F(E2ERunnerTest, multipleSources) {
        std::string configPath = "/filter_one_source.yaml";
        std::string logPath = "E2ERunnerTest_multipleSources.log";

        auto e2EBenchmarkConfig = parseYamlConfig(configPath, logPath);
        NES::Benchmark::writeHeaderToCsvFile(e2EBenchmarkConfig);

        for (auto& configPerRun : e2EBenchmarkConfig.getAllConfigPerRuns()) {
            NES::Benchmark::runSingleRun(configPerRun, e2EBenchmarkConfig.getConfigOverAllRuns(), *rpcCoordinatorPort, *restPort);
        }
    }

    TEST_F(E2ERunnerTest, multiplePhysicalLogicalSources) {
        std::string configPath = "/filter_one_source.yaml";
        std::string logPath = "E2ERunnerTest_multiplePhysicalLogicalSources.log";

        auto e2EBenchmarkConfig = parseYamlConfig(configPath, logPath);
        NES::Benchmark::writeHeaderToCsvFile(e2EBenchmarkConfig);

        for (auto& configPerRun : e2EBenchmarkConfig.getAllConfigPerRuns()) {
            NES::Benchmark::runSingleRun(configPerRun, e2EBenchmarkConfig.getConfigOverAllRuns(), *rpcCoordinatorPort, *restPort);
        }
    }

}