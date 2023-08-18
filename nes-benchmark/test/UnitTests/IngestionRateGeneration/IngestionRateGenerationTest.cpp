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
#include <E2E/Configurations/E2EBenchmarkConfigOverAllRuns.hpp>
#include <IngestionRateGeneration/CustomIngestionRateGenerator.hpp>
#include <IngestionRateGeneration/IngestionRateGenerator.hpp>
#include <IngestionRateGeneration/TrigonometricIngestionRateGenerator.hpp>
#include <IngestionRateGeneration/UniformIngestionRateGenerator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/BenchmarkUtils.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <typeinfo>

namespace NES::Benchmark {
    class IngestionRateGenerationTest : public Testing::BaseIntegrationTest {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("IngestionRateGenerationTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup IngestionRateGenerationTest test class.");
        }

        /* Will be called before a test is executed. */
        void SetUp() override {
            Testing::BaseIntegrationTest::SetUp();
            bufferManager =  std::make_shared<Runtime::BufferManager>();
            NES_INFO("Setup IngestionRateGenerationTest test case.");
        }

        /* Will be called before a test is executed. */
        void TearDown() override {
            NES_INFO("Tear down IngestionRateGenerationTest test case.");
            Testing::BaseIntegrationTest::TearDown();
        }

        /* Will be called after all tests in this class are finished. */
        static void TearDownTestCase() { NES_INFO("Tear down IngestionRateGenerationTest test class."); }

        std::shared_ptr<Runtime::BufferManager> bufferManager;
    };

    /**
     * @brief Testing if IngestionRateGenerator::createIngestionRateGenerator() is correct by making sure that as a default
     * the UniformIngestionRateGenerator is created
     */
    TEST_F(IngestionRateGenerationTest, createIngestionRateGeneratorTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        configOverAllRuns.dataProvider->setValue("External");

        auto defaultIngestionRateGenerator = IngestionRateGeneration::IngestionRateGenerator::createIngestionRateGenerator(configOverAllRuns);
        auto expectedIngestionRateGenerator = dynamic_cast<IngestionRateGeneration::UniformIngestionRateGenerator*>(defaultIngestionRateGenerator.get());
        ASSERT_TRUE(expectedIngestionRateGenerator != nullptr);
    }

    /**
     * @brief Testing if IngestionRateGenerator::createIngestionRateGenerator() throws an exception if the user does not provide a
     * valid ingestionRateDistribution
     */
    TEST_F(IngestionRateGenerationTest, undefinedIngestionRateTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        configOverAllRuns.dataProvider->setValue("External");
        configOverAllRuns.ingestionRateDistribution->setValue("SomeRandomType");

        ASSERT_THROW({IngestionRateGeneration::IngestionRateGenerator::createIngestionRateGenerator(configOverAllRuns);}, Exceptions::RuntimeException);
    }

    /**
     * @brief Testing if IngestionRateGenerator::generateIngestionRates() is correct for UniformIngestionRateGenerator by
     * creating ingestion rates and then checking versus a hardcoded truth
     */
    TEST_F(IngestionRateGenerationTest, uniformIngestionRateTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        configOverAllRuns.dataProvider->setValue("External");
        configOverAllRuns.ingestionRateCount->setValue(1);

        auto ingestionRateInBuffers = configOverAllRuns.ingestionRateInBuffers->getValue();
        auto ingestionRateCount = configOverAllRuns.ingestionRateCount->getValue();

        auto ingestionRateGenerator = std::make_unique<IngestionRateGeneration::UniformIngestionRateGenerator>(ingestionRateInBuffers, ingestionRateCount);
        auto defaultPredefinedIngestionRates = ingestionRateGenerator->generateIngestionRates();

        std::vector<uint64_t> expectedPredefinedIngestionRates = {50000};

        ASSERT_EQ(defaultPredefinedIngestionRates.size(), expectedPredefinedIngestionRates.size());
        ASSERT_EQ(defaultPredefinedIngestionRates, expectedPredefinedIngestionRates);
    }

    /**
     * @brief Testing if IngestionRateGenerator::generateIngestionRates() is correct for TrigonometricIngestionRateGenerator by
     * creating predefinedIngestionRates and then checking versus a hardcoded truth
     */
    TEST_F(IngestionRateGenerationTest, trigonometricIngestionRateTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        configOverAllRuns.dataProvider->setValue("External");
        configOverAllRuns.ingestionRateCount->setValue(8);
        configOverAllRuns.numberOfPeriods->setValue(2);

        auto ingestionRateDistribution = IngestionRateGeneration::IngestionRateDistribution::SINUS;
        auto ingestionRateInBuffers = configOverAllRuns.ingestionRateInBuffers->getValue();
        auto ingestionRateCount = configOverAllRuns.ingestionRateCount->getValue();
        auto numberOfPeriods = configOverAllRuns.numberOfPeriods->getValue();

        auto ingestionRateGenerator = std::make_unique<IngestionRateGeneration::TrigonometricIngestionRateGenerator>(ingestionRateDistribution, ingestionRateInBuffers, ingestionRateCount, numberOfPeriods);
        auto defaultPredefinedIngestionRates = ingestionRateGenerator->generateIngestionRates();

        std::vector<uint64_t> expectedPredefinedIngestionRates = {25000, 50000, 25000, 0, 25000, 50000, 25000, 0};

        ASSERT_EQ(defaultPredefinedIngestionRates.size(), expectedPredefinedIngestionRates.size());
        ASSERT_EQ(defaultPredefinedIngestionRates, expectedPredefinedIngestionRates);
    }

    /**
     * @brief Testing if IngestionRateGenerator::generateIngestionRates() is correct for CustomIngestionRateGenerator by
     * creating predefinedIngestionRates and then checking versus a hardcoded truth
     */
    TEST_F(IngestionRateGenerationTest, customIngestionRateTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        configOverAllRuns.dataProvider->setValue("External");
        configOverAllRuns.ingestionRateCount->setValue(5);
        configOverAllRuns.ingestionRateDistribution->setValue("Custom");
        configOverAllRuns.customValues->setValue("1000, 2000, 3000, 1000, 2000");

        auto ingestionRateCount = configOverAllRuns.ingestionRateCount->getValue();
        auto customValues = NES::Util::splitWithStringDelimiter<uint64_t>(configOverAllRuns.customValues->getValue(), ",");

        auto ingestionRateGenerator = std::make_unique<IngestionRateGeneration::CustomIngestionRateGenerator>(ingestionRateCount, customValues);
        auto defaultPredefinedIngestionRates = ingestionRateGenerator->generateIngestionRates();

        std::vector<uint64_t> expectedPredefinedIngestionRates = {1000, 2000, 3000, 1000, 2000};

        ASSERT_EQ(defaultPredefinedIngestionRates.size(), expectedPredefinedIngestionRates.size());
        ASSERT_EQ(defaultPredefinedIngestionRates, expectedPredefinedIngestionRates);
    }
}//namespace NES::Benchmark
