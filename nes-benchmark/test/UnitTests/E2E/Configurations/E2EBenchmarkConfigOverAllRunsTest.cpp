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
#include <DataGeneration/DefaultDataGenerator.hpp>
#include <DataGeneration/ZipfianDataGenerator.hpp>
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

        oss << "- startupSleepIntervalInSeconds: 5" << std::endl
            << "- numMeasurementsToCollect: 10" << std::endl
            << "- experimentMeasureIntervalInSeconds: 1" << std::endl
            << "- outputFile: e2eBenchmarkRunner" << std::endl
            << "- benchmarkName: E2ERunner" << std::endl
            << "- inputType: Auto" << std::endl
            << "- sourceSharing: off" << std::endl
            << "- query: " << std::endl
            << "- numberOfPreAllocatedBuffer: 1" << std::endl
            << "- numberOfBuffersToProduce: 5000000" << std::endl
            << "- batchSize: 1" << std::endl
            << "- dataProviderMode: ZeroCopy" << std::endl
            << "- connectionString: " << std::endl
            << "- logicalSources: input1: Uniform" << std::endl;
        auto expectedString = oss.str();

        ASSERT_EQ(defaultString, expectedString);
    }

    TEST_F(E2EBenchmarkConfigOverAllRunsTest, generateConfigOverAllRunsTest) {
        E2EBenchmarkConfigOverAllRuns defaultConfigOverAllRuns;
        Yaml::Node yamlConfig;

        std::string configPath = std::string(TEST_CONFIGS_DIRECTORY) + "/filter_one_source.yaml";
        Yaml::Parse(yamlConfig, configPath.c_str());

        defaultConfigOverAllRuns = E2EBenchmarkConfigOverAllRuns::generateConfigOverAllRuns(yamlConfig);

        ASSERT_EQ(defaultConfigOverAllRuns.startupSleepIntervalInSeconds->getValueAsString(), "1");
        ASSERT_EQ(defaultConfigOverAllRuns.numMeasurementsToCollect->getValueAsString(), "1");
        ASSERT_EQ(defaultConfigOverAllRuns.experimentMeasureIntervalInSeconds->getValueAsString(), "1");
        ASSERT_EQ(defaultConfigOverAllRuns.outputFile->getValue(), "FilterOneSource.csv");
        ASSERT_EQ(defaultConfigOverAllRuns.benchmarkName->getValue(), "FilterOneSource");
        ASSERT_EQ(defaultConfigOverAllRuns.query->getValue(), R"(Query::from("input1").filter(Attribute("event_type") < 100).sink(NullOutputSinkDescriptor::create());)");
        ASSERT_EQ(defaultConfigOverAllRuns.dataProviderMode->getValue(), "ZeroCopy");
        ASSERT_EQ(defaultConfigOverAllRuns.connectionString->getValue(), "");
        ASSERT_EQ(defaultConfigOverAllRuns.inputType->getValue(), "Auto");
        ASSERT_EQ(defaultConfigOverAllRuns.sourceSharing->getValue(), "off");
        ASSERT_EQ(defaultConfigOverAllRuns.numberOfPreAllocatedBuffer->getValueAsString(), "100");
        ASSERT_EQ(defaultConfigOverAllRuns.batchSize->getValueAsString(), "1");
        ASSERT_EQ(defaultConfigOverAllRuns.numberOfBuffersToProduce->getValueAsString(), "500");
        ASSERT_EQ(defaultConfigOverAllRuns.getStrLogicalSrcDataGenerators(), "input1: YSB");
    }

    TEST_F(E2EBenchmarkConfigOverAllRunsTest, getTotalSchemaSizeTest) {
        size_t expectedSize = 0;
        E2EBenchmarkConfigOverAllRuns defaultConfigOverAllRuns;

        auto defaultDataGenerator = std::make_shared<DataGeneration::DefaultDataGenerator>(0, 1000);
        auto zipfianDataGenerator = std::make_shared<DataGeneration::ZipfianDataGenerator>(0.8, 0, 1000);
        defaultConfigOverAllRuns.srcNameToDataGenerator["input1"] = defaultDataGenerator;
        defaultConfigOverAllRuns.srcNameToDataGenerator["input2"] = zipfianDataGenerator;

        auto defaultSize = defaultConfigOverAllRuns.getTotalSchemaSize();

        expectedSize += defaultDataGenerator->getSchema()->getSchemaSizeInBytes();
        expectedSize += zipfianDataGenerator->getSchema()->getSchemaSizeInBytes();

        ASSERT_EQ(defaultSize, expectedSize);
    }

    TEST_F(E2EBenchmarkConfigOverAllRunsTest, getStrLogicalSrcDataGeneratorsTest) {
        std::stringstream expectedString;
        E2EBenchmarkConfigOverAllRuns defaultConfigOverAllRuns;

        defaultConfigOverAllRuns.srcNameToDataGenerator["input1"] = std::make_shared<DataGeneration::DefaultDataGenerator>(0, 1000);
        defaultConfigOverAllRuns.srcNameToDataGenerator["input2"] = std::make_shared<DataGeneration::ZipfianDataGenerator>(0.8, 0, 1000);

        auto defaultString = defaultConfigOverAllRuns.getStrLogicalSrcDataGenerators();

        expectedString << "input1: Uniform, input2: Zipfian";

        ASSERT_EQ(defaultString, expectedString.str());
    }
}//namespace NES::Benchmark