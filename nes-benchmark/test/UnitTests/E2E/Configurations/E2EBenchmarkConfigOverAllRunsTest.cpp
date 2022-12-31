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

//#include <E2E/Configurations/E2EBenchmarkConfigOverAllRuns.hpp>
#include <gtest/gtest.h>
#include <Util/Logger/Logger.hpp>

namespace NES::Benchmark {
    class E2EBenchmarkConfigOverAllRunsTest : public testing::Test {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("E2EBenchmarkConfigOverAllRunsTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup E2EBenchmarkConfigOverAllRunsTest test class.");
        }

        /* Will be called before a test is executed. */
        void SetUp() override { NES_INFO("Setup E2EBenchmarkConfigOverAllRunsTest test case."); }

        /* Will be called before a test is executed. */
        void TearDown() override { NES_INFO("Tear down E2EBenchmarkConfigOverAllRunsTest test case."); }

        /* Will be called after all tests in this class are finished. */
        static void TearDownTestCase() { NES_INFO("Tear down E2EBenchmarkConfigOverAllRunsTest test class."); }
    };

    TEST_F(E2EBenchmarkConfigOverAllRunsTest, toStringTest) {
        std::stringstream oss;

        // set default E2EBenchmarkConfigOverAllRuns
        // call function toString()

        oss << "- startupSleepIntervalInSeconds: " << startupSleepIntervalInSeconds->getValueAsString() << std::endl
            << "- numMeasurementsToCollect: " << numMeasurementsToCollect->getValueAsString() << std::endl
            << "- experimentMeasureIntervalInSeconds: " << experimentMeasureIntervalInSeconds->getValueAsString() << std::endl
            << "- outputFile: " << outputFile->getValue() << std::endl
            << "- benchmarkName: " << benchmarkName->getValue() << std::endl
            << "- inputType: " << inputType->getValue() << std::endl
            << "- sourceSharing: " << sourceSharing->getValue() << std::endl
            << "- query: " << query->getValue() << std::endl
            << "- numberOfPreAllocatedBuffer: " << numberOfPreAllocatedBuffer->getValueAsString() << std::endl
            << "- numberOfBuffersToProduce: " << numberOfBuffersToProduce->getValueAsString() << std::endl
            << "- batchSize: " << batchSize->getValueAsString() << std::endl
            << "- dataProviderMode: " << dataProviderMode->getValue() << std::endl
            << "- connectionString: " << connectionString->getValue() << std::endl
            << "- logicalSources: " << getStrLogicalSrcDataGenerators() << std::endl;
        auto expectedString = oss.str();

        // ASSERT_EQ(stringDefault, expectedString);
    }

    TEST_F(E2EBenchmarkConfigOverAllRunsTest, generateConfigOverAllRunsTest) {
        E2EBenchmarkConfigOverAllRuns expectedConfigOverAllRuns;

        // set default E2EBenchmarkConfigOverAllRuns
        // call function generateConfigOverAllRuns()

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

        // ASSERT_EQ(stringDefault, expectedConfigOverAllRuns);
    }

    TEST_F(E2EBenchmarkConfigOverAllRunsTest, getStrLogicalSrcDataGeneratorsTest) {
        std::stringstream expectedStringStream;

        // set default E2EBenchmarkConfigOverAllRuns
        // call function getStrLogicalSrcDataGenerators()

        for (auto it = srcNameToDataGenerator.begin(); it != srcNameToDataGenerator.end(); ++it) {
            if (it != srcNameToDataGenerator.begin()) {
                expectedStringStream << ", ";
            }
            expectedStringStream << it->first << ": " << it->second;
        }

        // ASSERT_EQ(stringDefault, expectedStringStream);
    }
}//namespace NES::Benchmark