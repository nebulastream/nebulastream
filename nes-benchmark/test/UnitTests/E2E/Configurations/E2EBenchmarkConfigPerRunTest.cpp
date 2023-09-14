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

#include <BaseIntegrationTest.hpp>
#include <E2E/Configurations/E2EBenchmarkConfigPerRun.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

namespace NES::Benchmark {
    class E2EBenchmarkConfigPerRunTest : public Testing::BaseIntegrationTest {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("E2EBenchmarkConfigPerRunTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup E2EBenchmarkConfigPerRunTest test class.");
        }

        /* Will be called before a test is executed. */
        void SetUp() override {
            Testing::BaseIntegrationTest::SetUp();
            NES_INFO("Setup E2EBenchmarkConfigPerRunTest test case.");
        }

        /* Will be called before a test is executed. */
        void TearDown() override {
            NES_INFO("Tear down E2EBenchmarkConfigPerRunTest test case.");
            Testing::BaseIntegrationTest::TearDown();
        }

        /* Will be called after all tests in this class are finished. */
        static void TearDownTestCase() { NES_INFO("Tear down E2EBenchmarkConfigPerRunTest test class."); }
    };

    /**
     * @brief Testing if E2EBenchmarkConfigPerRun::toString() is correct by testing against a hardcoded truth
     */
    TEST_F(E2EBenchmarkConfigPerRunTest, toStringTest) {
        std::stringstream oss;
        E2EBenchmarkConfigPerRun defaultConfigPerRun;

        auto defaultString = defaultConfigPerRun.toString();

        oss << "- numWorkerOfThreads: " << defaultConfigPerRun.numberOfWorkerThreads->getValueAsString() << std::endl
            << "- bufferSizeInBytes: " << defaultConfigPerRun.bufferSizeInBytes->getValueAsString() << std::endl
            << "- numberOfQueriesToDeploy: " << defaultConfigPerRun.numberOfQueriesToDeploy->getValueAsString() << std::endl
            << "- numberOfSources: " << defaultConfigPerRun.getStringLogicalSourceToNumberOfPhysicalSources() << std::endl
            << "- numberOfBuffersInGlobalBufferManager: " << defaultConfigPerRun.numberOfBuffersInGlobalBufferManager->getValueAsString() << std::endl
            << "- numberOfBuffersInSourceLocalBufferPool: " << defaultConfigPerRun.numberOfBuffersInSourceLocalBufferPool->getValueAsString() << std::endl
            << "- pageSize: " << defaultConfigPerRun.pageSize->getValueAsString() << std::endl
            << "- preAllocPageCnt: " << defaultConfigPerRun.preAllocPageCnt->getValueAsString() << std::endl
            << "- numberOfPartitions: " << defaultConfigPerRun.numberOfPartitions->getValueAsString() << std::endl
            << "- maxHashTableSize: " << defaultConfigPerRun.maxHashTableSize->getValueAsString() << std::endl
            << "- query: " << defaultConfigPerRun.query->getValue() << std::endl
            << "- windowingStrategy: " << defaultConfigPerRun.windowingStrategy.getValue() << std::endl
            << "- joinStrategy: " << defaultConfigPerRun.joinStrategy.getValue() << std::endl;
        auto expectedString = oss.str();

        ASSERT_EQ(defaultString, expectedString);
    }

    /**
     * @brief Testing if E2EBenchmarkConfigPerRun::generateAllConfigsPerRunTest() is correct by parsing from a yaml file and
     * comparing against a hardcoded truth
     */
    TEST_F(E2EBenchmarkConfigPerRunTest, generateAllConfigsPerRunTest) {

        Yaml::Node yamlConfig;
        std::string configPath = std::string(TEST_CONFIGS_DIRECTORY) + "/join_multiple_phys_and_logical_sources.yaml";
        Yaml::Parse(yamlConfig, configPath.c_str());

        auto allE2EBenchmarkPerRun = E2EBenchmarkConfigPerRun::generateAllConfigsPerRun(yamlConfig);

        ASSERT_EQ(allE2EBenchmarkPerRun.size(), 3);

        ASSERT_EQ(allE2EBenchmarkPerRun[0].numberOfWorkerThreads->getValue(), 1);
        ASSERT_EQ(allE2EBenchmarkPerRun[1].numberOfWorkerThreads->getValue(), 2);
        ASSERT_EQ(allE2EBenchmarkPerRun[2].numberOfWorkerThreads->getValue(), 2);

        ASSERT_EQ(allE2EBenchmarkPerRun[0].bufferSizeInBytes->getValue(), 512);
        ASSERT_EQ(allE2EBenchmarkPerRun[1].bufferSizeInBytes->getValue(), 5120);
        ASSERT_EQ(allE2EBenchmarkPerRun[2].bufferSizeInBytes->getValue(), 5120);

        ASSERT_EQ(allE2EBenchmarkPerRun[0].numberOfBuffersInGlobalBufferManager->getValue(),
                  allE2EBenchmarkPerRun[0].numberOfBuffersInGlobalBufferManager->getDefaultValue());
        ASSERT_EQ(allE2EBenchmarkPerRun[1].numberOfBuffersInGlobalBufferManager->getValue(),
                  allE2EBenchmarkPerRun[1].numberOfBuffersInGlobalBufferManager->getDefaultValue());
        ASSERT_EQ(allE2EBenchmarkPerRun[2].numberOfBuffersInGlobalBufferManager->getValue(),
                  allE2EBenchmarkPerRun[2].numberOfBuffersInGlobalBufferManager->getDefaultValue());

        ASSERT_EQ(allE2EBenchmarkPerRun[0].numberOfBuffersInSourceLocalBufferPool->getValue(),
                  allE2EBenchmarkPerRun[0].numberOfBuffersInSourceLocalBufferPool->getDefaultValue());
        ASSERT_EQ(allE2EBenchmarkPerRun[1].numberOfBuffersInSourceLocalBufferPool->getValue(),
                  allE2EBenchmarkPerRun[1].numberOfBuffersInSourceLocalBufferPool->getDefaultValue());
        ASSERT_EQ(allE2EBenchmarkPerRun[2].numberOfBuffersInSourceLocalBufferPool->getValue(),
                  allE2EBenchmarkPerRun[2].numberOfBuffersInSourceLocalBufferPool->getDefaultValue());

        ASSERT_EQ(allE2EBenchmarkPerRun[0].numberOfQueriesToDeploy->getValue(),
                  allE2EBenchmarkPerRun[0].numberOfQueriesToDeploy->getDefaultValue());
        ASSERT_EQ(allE2EBenchmarkPerRun[1].numberOfQueriesToDeploy->getValue(),
                  allE2EBenchmarkPerRun[1].numberOfQueriesToDeploy->getDefaultValue());
        ASSERT_EQ(allE2EBenchmarkPerRun[2].numberOfQueriesToDeploy->getValue(),
                  allE2EBenchmarkPerRun[2].numberOfQueriesToDeploy->getDefaultValue());

        ASSERT_EQ(allE2EBenchmarkPerRun[0].getStringLogicalSourceToNumberOfPhysicalSources(), "input1: 1, input2: 2, input3: 1");
        ASSERT_EQ(allE2EBenchmarkPerRun[1].getStringLogicalSourceToNumberOfPhysicalSources(), "input1: 2, input2: 2, input3: 1");
        ASSERT_EQ(allE2EBenchmarkPerRun[2].getStringLogicalSourceToNumberOfPhysicalSources(), "input1: 3, input2: 2, input3: 1");
    }

    /**
     * @brief Testing if E2EBenchmarkConfigPerRun::getStrLogicalSrcToNumberOfPhysicalSrcTest() is correct by
     * comparing against a hardcoded truth
     */
    TEST_F(E2EBenchmarkConfigPerRunTest, getStrLogicalSrcToNumberOfPhysicalSrcTest) {
        std::stringstream expectedString;
        E2EBenchmarkConfigPerRun configPerRun;
        configPerRun.logicalSrcToNoPhysicalSrc = {{"input1", 2}, {"input2", 1}, {"some other source", 23456}};

        ASSERT_EQ(configPerRun.getStringLogicalSourceToNumberOfPhysicalSources(), "input1: 2, input2: 1, some other source: 23456");
    }

    /**
     * @brief Testing if E2EBenchmarkConfigPerRun::generateMapsLogicalSrcPhysicalSourcesTest() is correct by parsing a yaml file
     * and comparing against a hardcoded truth
     */
    TEST_F(E2EBenchmarkConfigPerRunTest, generateMapsLogicalSrcPhysicalSourcesTest) {

        Yaml::Node yamlConfig;
        std::string configPath = std::string(TEST_CONFIGS_DIRECTORY) + "/join_multiple_sources.yaml";
        Yaml::Parse(yamlConfig, configPath.c_str());

        auto allLogicalSrcPhysicalSources = E2EBenchmarkConfigPerRun::generateMapsLogicalSrcToNumberOfPhysicalSources(
                yamlConfig);

        ASSERT_EQ(allLogicalSrcPhysicalSources.size(), 3);

        ASSERT_EQ(allLogicalSrcPhysicalSources[0].size(), 2);
        ASSERT_EQ(allLogicalSrcPhysicalSources[0]["input1"], 1);
        ASSERT_EQ(allLogicalSrcPhysicalSources[0]["input2"], 3);

        ASSERT_EQ(allLogicalSrcPhysicalSources[1].size(), 2);
        ASSERT_EQ(allLogicalSrcPhysicalSources[1]["input1"], 1);
        ASSERT_EQ(allLogicalSrcPhysicalSources[1]["input2"], 2);

        ASSERT_EQ(allLogicalSrcPhysicalSources[2].size(), 2);
        ASSERT_EQ(allLogicalSrcPhysicalSources[2]["input1"], 1);
        ASSERT_EQ(allLogicalSrcPhysicalSources[2]["input2"], 1);
    }

} //namespace NES::Benchmark