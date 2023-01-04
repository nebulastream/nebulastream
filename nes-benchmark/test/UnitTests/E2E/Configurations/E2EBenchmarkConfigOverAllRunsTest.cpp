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

#include <API/Schema.hpp>
#include <E2E/Configurations/E2EBenchmarkConfigOverAllRuns.hpp>
#include <NesBaseTest.hpp>
#include <gtest/gtest.h>
#include <Util/yaml/Yaml.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Benchmark {
    class E2EBenchmarkConfigOverAllRunsTest : public Testing::NESBaseTest {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("E2EBenchmarkConfigOverAllRunsTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup E2EBenchmarkConfigOverAllRunsTest test class.");
        }

        /* Will be called before a test is executed. */
        void SetUp() override {
            Testing::NESBaseTest::SetUp();
            NES_INFO("Setup E2EBenchmarkConfigOverAllRunsTest test case.");
        }

        /* Will be called before a test is executed. */
        void TearDown() override {
            NES_INFO("Tear down E2EBenchmarkConfigOverAllRunsTest test case.");
            Testing::NESBaseTest::TearDown();
        }

        /* Will be called after all tests in this class are finished. */
        static void TearDownTestCase() { NES_INFO("Tear down E2EBenchmarkConfigOverAllRunsTest test class."); }
    };

    TEST_F(E2EBenchmarkConfigOverAllRunsTest, toStringTest) {
        std::stringstream oss;
        E2EBenchmarkConfigOverAllRuns defaultConfigOverAllRuns;

        auto defaultString = defaultConfigOverAllRuns.toString();

        oss << "- startupSleepIntervalInSeconds: " << defaultConfigOverAllRuns.startupSleepIntervalInSeconds->getValueAsString() << std::endl
            << "- numMeasurementsToCollect: " << defaultConfigOverAllRuns.numMeasurementsToCollect->getValueAsString() << std::endl
            << "- experimentMeasureIntervalInSeconds: " << defaultConfigOverAllRuns.experimentMeasureIntervalInSeconds->getValueAsString() << std::endl
            << "- outputFile: " << defaultConfigOverAllRuns.outputFile->getValue() << std::endl
            << "- benchmarkName: " << defaultConfigOverAllRuns.benchmarkName->getValue() << std::endl
            << "- inputType: " << defaultConfigOverAllRuns.inputType->getValue() << std::endl
            << "- sourceSharing: " << defaultConfigOverAllRuns.sourceSharing->getValue() << std::endl
            << "- query: " << defaultConfigOverAllRuns.query->getValue() << std::endl
            << "- numberOfPreAllocatedBuffer: " << defaultConfigOverAllRuns.numberOfPreAllocatedBuffer->getValueAsString() << std::endl
            << "- numberOfBuffersToProduce: " << defaultConfigOverAllRuns.numberOfBuffersToProduce->getValueAsString() << std::endl
            << "- batchSize: " << defaultConfigOverAllRuns.batchSize->getValueAsString() << std::endl
            << "- dataProviderMode: " << defaultConfigOverAllRuns.dataProviderMode->getValue() << std::endl
            << "- connectionString: " << defaultConfigOverAllRuns.connectionString->getValue() << std::endl
            << "- logicalSources: " << defaultConfigOverAllRuns.getStrLogicalSrcDataGenerators() << std::endl;
        auto expectedString = oss.str();

        ASSERT_EQ(defaultString, expectedString);
    }

    TEST_F(E2EBenchmarkConfigOverAllRunsTest, generateConfigOverAllRunsTest) {
        E2EBenchmarkConfigOverAllRuns expectedConfigOverAllRuns;
        E2EBenchmarkConfigOverAllRuns defaultConfigOverAllRuns;
        Yaml::Node yamlConfig;

        std::string configPath = std::string(TEST_CONFIGS_DIRECTORY) + "/filter_one_source.yaml";
        Yaml::Parse(yamlConfig, configPath.c_str());

        defaultConfigOverAllRuns = E2EBenchmarkConfigOverAllRuns::generateConfigOverAllRuns(yamlConfig);

        expectedConfigOverAllRuns.startupSleepIntervalInSeconds->setValue(yamlConfig["startupSleepIntervalInSeconds"].As<uint32_t>());
        expectedConfigOverAllRuns.numMeasurementsToCollect->setValue(yamlConfig["numberOfMeasurementsToCollect"].As<uint32_t>());
        expectedConfigOverAllRuns.experimentMeasureIntervalInSeconds->setValue(
            yamlConfig["experimentMeasureIntervalInSeconds"].As<uint32_t>());
        expectedConfigOverAllRuns.outputFile->setValue(yamlConfig["outputFile"].As<std::string>());
        expectedConfigOverAllRuns.benchmarkName->setValue(yamlConfig["benchmarkName"].As<std::string>());
        expectedConfigOverAllRuns.query->setValue(yamlConfig["query"].As<std::string>());
        expectedConfigOverAllRuns.dataProviderMode->setValue(yamlConfig["dataProviderMode"].As<std::string>());
        expectedConfigOverAllRuns.connectionString->setValue(yamlConfig["connectionString"].As<std::string>());
        expectedConfigOverAllRuns.inputType->setValue(yamlConfig["inputType"].As<std::string>());
        expectedConfigOverAllRuns.sourceSharing->setValue(yamlConfig["sourceSharing"].As<std::string>());
        expectedConfigOverAllRuns.numberOfPreAllocatedBuffer->setValue(yamlConfig["numberOfPreAllocatedBuffer"].As<uint32_t>());
        expectedConfigOverAllRuns.batchSize->setValue(yamlConfig["batchSize"].As<uint32_t>());
        expectedConfigOverAllRuns.numberOfBuffersToProduce->setValue(yamlConfig["numberOfBuffersToProduce"].As<uint32_t>());

        auto logicalSourcesNode = yamlConfig["logicalSources"];
        if (logicalSourcesNode.IsMap()) {
            for (auto entry = logicalSourcesNode.Begin(); entry != logicalSourcesNode.End(); entry++) {
                auto sourceName = (*entry).first;
                auto node = (*entry).second;
                if (expectedConfigOverAllRuns.srcNameToDataGenerator.contains(sourceName)) {
                    NES_THROW_RUNTIME_ERROR("Logical source name has to be unique. " << sourceName << " is not unique!");
                }

                auto dataGenerator = DataGeneration::DataGenerator::createGeneratorByName(sourceName, node);
                expectedConfigOverAllRuns.srcNameToDataGenerator[sourceName] = dataGenerator;
            }
        }

        ASSERT_EQ(defaultConfigOverAllRuns.startupSleepIntervalInSeconds->getValue(), expectedConfigOverAllRuns.startupSleepIntervalInSeconds->getValue());
        ASSERT_EQ(defaultConfigOverAllRuns.numMeasurementsToCollect->getValue(), expectedConfigOverAllRuns.numMeasurementsToCollect->getValue());
        ASSERT_EQ(defaultConfigOverAllRuns.experimentMeasureIntervalInSeconds->getValue(), expectedConfigOverAllRuns.experimentMeasureIntervalInSeconds->getValue());
        ASSERT_EQ(defaultConfigOverAllRuns.outputFile->getValue(), expectedConfigOverAllRuns.outputFile->getValue());
        ASSERT_EQ(defaultConfigOverAllRuns.benchmarkName->getValue(), expectedConfigOverAllRuns.benchmarkName->getValue());
        ASSERT_EQ(defaultConfigOverAllRuns.query->getValue(), expectedConfigOverAllRuns.query->getValue());
        ASSERT_EQ(defaultConfigOverAllRuns.dataProviderMode->getValue(), expectedConfigOverAllRuns.dataProviderMode->getValue());
        ASSERT_EQ(defaultConfigOverAllRuns.connectionString->getValue(), expectedConfigOverAllRuns.connectionString->getValue());
        ASSERT_EQ(defaultConfigOverAllRuns.inputType->getValue(), expectedConfigOverAllRuns.inputType->getValue());
        ASSERT_EQ(defaultConfigOverAllRuns.sourceSharing->getValue(), expectedConfigOverAllRuns.sourceSharing->getValue());
        ASSERT_EQ(defaultConfigOverAllRuns.numberOfPreAllocatedBuffer->getValue(), expectedConfigOverAllRuns.numberOfPreAllocatedBuffer->getValue());
        ASSERT_EQ(defaultConfigOverAllRuns.batchSize->getValue(), expectedConfigOverAllRuns.batchSize->getValue());
        ASSERT_EQ(defaultConfigOverAllRuns.numberOfBuffersToProduce->getValue(), expectedConfigOverAllRuns.numberOfBuffersToProduce->getValue());
        // ASSERT_EQ logicalSources
    }

    TEST_F(E2EBenchmarkConfigOverAllRunsTest, getTotalSchemaSizeTest) {
        size_t expectedSize = 0;
        E2EBenchmarkConfigOverAllRuns defaultConfigOverAllRuns;

        auto defaultSize = defaultConfigOverAllRuns.getTotalSchemaSize();

        for (auto [logicalSource, dataGenerator] : defaultConfigOverAllRuns.srcNameToDataGenerator) {
            expectedSize += dataGenerator->getSchema()->getSchemaSizeInBytes();
        }

        ASSERT_EQ(defaultSize, expectedSize);
    }

    TEST_F(E2EBenchmarkConfigOverAllRunsTest, getStrLogicalSrcDataGeneratorsTest) {
        std::stringstream expectedString;
        E2EBenchmarkConfigOverAllRuns defaultConfigOverAllRuns;

        auto defaultString = defaultConfigOverAllRuns.getStrLogicalSrcDataGenerators();

        for (auto it = defaultConfigOverAllRuns.srcNameToDataGenerator.begin(); it != defaultConfigOverAllRuns.srcNameToDataGenerator.end(); ++it) {
            if (it != defaultConfigOverAllRuns.srcNameToDataGenerator.begin()) {
                expectedString << ", ";
            }
            expectedString << it->first << ": " << it->second;
        }

        ASSERT_EQ(defaultString, expectedString.str());
    }
}//namespace NES::Benchmark