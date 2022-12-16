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

#include <DataGeneration/ZipfianDataGenerator.hpp>
#include <Util/ZipfianGenerator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <API/Schema.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <random>
#include <vector>

namespace NES::Benchmark::DataGeneration {
    class ZipfianDataGeneratorTest : public testing::Test {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("ZipfianDataGeneratorTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup ZipfianDataGeneratorTest test class.");
        }

        /* Will be called before a test is executed. */
        void SetUp() override { NES_INFO("Setup ZipfianDataGeneratorTest test case."); }

        /* Will be called before a test is executed. */
        void TearDown() override { NES_INFO("Tear down ZipfianDataGeneratorTest test case."); }

        /* Will be called after all tests in this class are finished. */
        static void TearDownTestCase() { NES_INFO("Tear down ZipfianDataGeneratorTest test class."); }
    };

    /* The following test block tests the member functions of the ZipfianDataGenerator */
    TEST_F(ZipfianDataGeneratorTest, getSchemaTest) {
        auto alpha = 0.9;
        auto minValue = 0;
        auto maxValue = 1000;

        auto zipfianDataGenerator = std::make_shared<ZipfianDataGenerator>(alpha, minValue, maxValue);
        auto schemaDefault = zipfianDataGenerator->getSchema();

        auto expectedSchema = NES::Schema::create()
                                  ->addField(createField("id", NES::UINT64))
                                  ->addField(createField("value", NES::UINT64))
                                  ->addField(createField("payload", NES::UINT64))
                                  ->addField(createField("timestamp", NES::UINT64));

        ASSERT_TRUE(expectedSchema->equals(schemaDefault, true));
    }

    TEST_F(ZipfianDataGeneratorTest, getNameTest) {
        auto alpha = 0.9;
        auto minValue = 0;
        auto maxValue = 1000;

        auto zipfianDataGenerator = std::make_shared<ZipfianDataGenerator>(alpha, minValue, maxValue);
        auto nameDefault = zipfianDataGenerator->getName();

        auto expectedName = "Zipfian";
        ASSERT_EQ(nameDefault, expectedName);
    }

    TEST_F(ZipfianDataGeneratorTest, toStringTest) {
        auto alpha = 0.9;
        auto minValue = 0;
        auto maxValue = 1000;
        std::ostringstream oss;

        auto zipfianDataGenerator = std::make_shared<ZipfianDataGenerator>(alpha, minValue, maxValue);
        auto stringDefault = zipfianDataGenerator->toString();

        oss << zipfianDataGenerator->getName() << " (" << minValue << ", " << maxValue << ", " << alpha << ")";
        auto expectedString = oss.str();

        ASSERT_EQ(stringDefault, expectedString);
    }

    TEST_F(ZipfianDataGeneratorTest, createDataTest) {
        auto alpha = 0.9;
        auto minValue = 0;
        auto maxValue = 1000;
        size_t numberOfBuffers = 10;

        auto zipfianDataGenerator = std::make_shared<ZipfianDataGenerator>(alpha, minValue, maxValue);
        auto bufferManager =  std::make_shared<Runtime::BufferManager>();
        zipfianDataGenerator->setBufferManager(bufferManager);
        auto dataDefault = zipfianDataGenerator->createData(numberOfBuffers, bufferManager->getBufferSize());

        std::vector<Runtime::TupleBuffer> expectedData;
        expectedData.reserve(numberOfBuffers);

        auto memoryLayout = zipfianDataGenerator->getMemoryLayout(bufferManager->getBufferSize());

        for (uint64_t curBuffer = 0; curBuffer < numberOfBuffers; ++curBuffer) {
            Runtime::TupleBuffer bufferRef = bufferManager->getBufferBlocking();
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, bufferRef);

            std::mt19937 generator(GENERATOR_SEED_ZIPFIAN);
            ZipfianGenerator zipfianGenerator(minValue, maxValue, alpha);

            for (uint64_t curRecord = 0; curRecord < dynamicBuffer.getCapacity(); ++curRecord) {
                auto value = zipfianGenerator(generator);
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
