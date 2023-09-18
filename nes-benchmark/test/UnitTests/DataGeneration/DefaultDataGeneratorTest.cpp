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
#include <BaseIntegrationTest.hpp>
#include <DataGeneration/DefaultDataGenerator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <random>
#include <vector>

namespace NES::Benchmark::DataGeneration {
    class DefaultDataGeneratorTest : public Testing::BaseIntegrationTest {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("DefaultDataGeneratorTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup DefaultDataGeneratorTest test class.");
        }

        /* Will be called before a test is executed. */
        void SetUp() override {
            Testing::BaseIntegrationTest::SetUp();
            NES_INFO("Setup DefaultDataGeneratorTest test case.");
        }

        /* Will be called before a test is executed. */
        void TearDown() override {
            NES_INFO("Tear down DefaultDataGeneratorTest test case.");
            Testing::BaseIntegrationTest::TearDown();
        }

        /* Will be called after all tests in this class are finished. */
        static void TearDownTestCase() { NES_INFO("Tear down DefaultDataGeneratorTest test class."); }
    };

    /**
     * @brief Testing if DefaultDataGenerator::getSchema() works by comparing versus a hardcoded truth
     */
    TEST_F(DefaultDataGeneratorTest, getSchemaTest) {
        auto minValue = 0;
        auto maxValue = 1000;

        auto defaultDataGenerator = std::make_unique<DefaultDataGenerator>(minValue, maxValue);
        auto schemaDefault = defaultDataGenerator->getSchema();

        auto expectedSchema = NES::Schema::create()
                                  ->addField(createField("id", BasicType::UINT64))
                                  ->addField(createField("value", BasicType::UINT64))
                                  ->addField(createField("payload", BasicType::UINT64))
                                  ->addField(createField("timestamp", BasicType::UINT64));

        ASSERT_TRUE(expectedSchema->equals(schemaDefault, true));
    }

    /**
     * @brief Testing if DefaultDataGenerator::getName() works by comparing versus a hardcoded truth
     */
    TEST_F(DefaultDataGeneratorTest, getNameTest) {
        auto minValue = 0;
        auto maxValue = 1000;

        auto defaultDataGenerator = std::make_unique<DefaultDataGenerator>(minValue, maxValue);
        auto nameDefault = defaultDataGenerator->getName();

        auto expectedName = "Uniform";

        ASSERT_EQ(nameDefault, expectedName);
    }

    /**
     * @brief Testing if DefaultDataGenerator::toString() works by comparing versus a hardcoded truth
     */
    TEST_F(DefaultDataGeneratorTest, toStringTest) {
        auto minValue = 0;
        auto maxValue = 1000;
        std::ostringstream oss;

        auto defaultDataGenerator = std::make_unique<DefaultDataGenerator>(minValue, maxValue);
        auto stringDefault = defaultDataGenerator->toString();

        oss << defaultDataGenerator->getName() << " (" << minValue << ", " << maxValue << ")";
        auto expectedString = oss.str();

        ASSERT_EQ(stringDefault, expectedString);
    }

    /**
     * @brief Testing if DefaultDataGenerator::createData() works by creating tuples and then comparing the expected tupleBuffers
     * with the created one's from the DefaultDataGenerator
     */
    TEST_F(DefaultDataGeneratorTest, createDataTest) {
        auto minValue = 0;
        auto maxValue = 1000;
        size_t numberOfBuffers = 10;

        auto defaultDataGenerator = std::make_unique<DefaultDataGenerator>(minValue, maxValue);
        auto bufferManager =  std::make_shared<Runtime::BufferManager>();
        defaultDataGenerator->setBufferManager(bufferManager);
        auto dataDefault = defaultDataGenerator->createData(numberOfBuffers, bufferManager->getBufferSize());

        std::vector<Runtime::TupleBuffer> expectedData;
        expectedData.reserve(numberOfBuffers);

        auto memoryLayout = defaultDataGenerator->getMemoryLayout(bufferManager->getBufferSize());

        for (uint64_t curBuffer = 0; curBuffer < numberOfBuffers; ++curBuffer) {
            Runtime::TupleBuffer bufferRef = bufferManager->getBufferBlocking();
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, bufferRef);

            std::mt19937 generator(GENERATOR_SEED_DEFAULT);
            std::uniform_int_distribution<uint64_t> uniformIntDistribution(minValue, maxValue);

            for (uint64_t curRecord = 0; curRecord < dynamicBuffer.getCapacity(); ++curRecord) {
                auto value = uniformIntDistribution(generator);
                dynamicBuffer[curRecord]["id"].write<uint64_t>(curRecord);
                dynamicBuffer[curRecord]["value"].write<uint64_t>(value);
                dynamicBuffer[curRecord]["payload"].write<uint64_t>(curRecord);
                dynamicBuffer[curRecord]["timestamp"].write<uint64_t>(curRecord);
            }
            
            dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
            expectedData.emplace_back(bufferRef);
        }

        ASSERT_EQ(dataDefault.size(), expectedData.size());

        for (uint64_t i = 0; i < dataDefault.size(); ++i) {
            auto dataBuffer = dataDefault[i];
            auto expectedBuffer = expectedData[i];

            ASSERT_EQ(dataBuffer.getBufferSize(), expectedBuffer.getBufferSize());
            ASSERT_TRUE(memcmp(dataBuffer.getBuffer(), expectedBuffer.getBuffer(), dataBuffer.getBufferSize()) == 0);
        }
    }
}//namespace NES::Benchmark::DataGeneration
