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
#include <DataGeneration/DefaultDataGenerator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <random>

namespace NES::Benchmark::DataGeneration {
    class DefaultDataGeneratorTest : public testing::Test {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("DefaultDataGeneratorTest.log", NES::LogLevel::LOG_DEBUG);
            std::cout << "Setup DefaultDataGeneratorTest test class." << std::endl;
        }

        /* Will be called before a test is executed. */
        void SetUp() override { std::cout << "Setup DefaultDataGeneratorTest test case." << std::endl; }

        /* Will be called before a test is executed. */
        void TearDown() override { std::cout << "Tear down DefaultDataGeneratorTest test case." << std::endl; }

        /* Will be called after all tests in this class are finished. */
        static void TearDownTestCase() { std::cout << "Tear down DefaultDataGeneratorTest test class." << std::endl; }
    };

    TEST_F(DefaultDataGeneratorTest, getSchemaTest) {
        auto defaultDataGenerator = std::make_shared<DefaultDataGenerator>(/* minValue */ 0, /* maxValue */ 1000);
        auto schemaDefault = defaultDataGenerator->getSchema();
        auto expectedSchema = NES::Schema::create()
                                  ->addField(createField("id", NES::UINT64))
                                  ->addField(createField("value", NES::UINT64))
                                  ->addField(createField("payload", NES::UINT64))
                                  ->addField(createField("timestamp", NES::UINT64));

        // TODO compare schema correctly
        ASSERT_EQ(schemaDefault, expectedSchema);
    }

    TEST_F(DefaultDataGeneratorTest, getNameTest) {
        auto defaultDataGenerator = std::make_shared<DefaultDataGenerator>(/* minValue */ 0, /* maxValue */ 1000);
        auto nameDefault = defaultDataGenerator->getName();
        auto expectedName = "Uniform";

        ASSERT_EQ(nameDefault, expectedName);
    }

    TEST_F(DefaultDataGeneratorTest, toStringTest) {
        auto minValue = 0;
        auto maxValue = 1000;
        std::ostringstream oss;

        auto defaultDataGenerator = std::make_shared<DefaultDataGenerator>(minValue, maxValue);
        auto stringDefault = defaultDataGenerator->toString();

        oss << defaultDataGenerator->getName() << " (" << minValue << ", " << maxValue << ")";
        auto expectedString = oss.str();

        ASSERT_EQ(stringDefault, expectedString);
    }

    TEST_F(DefaultDataGeneratorTest, createDataTest) {
        auto minValue = 0;
        auto maxValue = 1000;
        auto numberOfBuffers = 1000;

        auto defaultDataGenerator = std::make_shared<DefaultDataGenerator>(minValue, maxValue);
        auto bufferManager =  std::make_shared<Runtime::BufferManager>();
        defaultDataGenerator->setBufferManager(bufferManager);
        auto dataDefault = defaultDataGenerator->createData(numberOfBuffers, bufferManager->getBufferSize());

        // TODO expected Data
        std::random_device randDev;
        std::mt19937 generator(randDev());
        std::uniform_int_distribution<uint64_t> uniformIntDistribution(minValue, maxValue);

        auto expectedData = ;

        ASSERT_EQ(dataDefault, expectedData);
    }
}//namespace NES::Benchmark::DataGeneration
