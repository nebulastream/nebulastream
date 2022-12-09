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
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <API/Schema.hpp>

namespace NES::Benchmark::DataGeneration {
    class ZipfianDataGeneratorTest : public testing::Test {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("ZipfianDataGeneratorTest.log", NES::LogLevel::LOG_DEBUG);
            std::cout << "Setup ZipfianDataGeneratorTest test class." << std::endl;
        }

        /* Will be called before a test is executed. */
        void SetUp() override { std::cout << "Setup ZipfianDataGeneratorTest test case." << std::endl; }

        /* Will be called before a test is executed. */
        void TearDown() override { std::cout << "Tear down ZipfianDataGeneratorTest test case." << std::endl; }

        /* Will be called after all tests in this class are finished. */
        static void TearDownTestCase() { std::cout << "Tear down ZipfianDataGeneratorTest test class." << std::endl; }
    };

    TEST_F(ZipfianDataGeneratorTest, getSchemaTest) {
        auto alpha = 0;
        auto minValue = 0;
        auto maxValue = 1000;
        bool equal = false;

        auto zipfianDataGenerator = std::make_shared<ZipfianDataGenerator>(alpha, minValue, maxValue);
        // how to use private getSchema() function
        auto schemaDefault = zipfianDataGenerator->getSchema();

        auto expectedSchema = NES::Schema::create()
                                  ->addField(createField("id", NES::UINT64))
                                  ->addField(createField("value", NES::UINT64))
                                  ->addField(createField("payload", NES::UINT64))
                                  ->addField(createField("timestamp", NES::UINT64));

        if (expectedSchema->equals(schemaDefault, true)) {
            equal = true;
        }

        ASSERT_EQ(equal, true);
    }

    TEST_F(ZipfianDataGeneratorTest, getNameTest) {
        auto alpha = 0;
        auto minValue = 0;
        auto maxValue = 1000;

        auto zipfianDataGenerator = std::make_shared<ZipfianDataGenerator>(alpha, minValue, maxValue);
        // how to use private getName() function
        auto nameDefault = zipfianDataGenerator->getName();

        auto expectedName = "Zipfian";

        ASSERT_EQ(nameDefault, expectedName);
    }

    TEST_F(ZipfianDataGeneratorTest, toStringTest) {
        auto alpha = 0;
        auto minValue = 0;
        auto maxValue = 1000;
        std::ostringstream oss;

        auto zipfianDataGenerator = std::make_shared<ZipfianDataGenerator>(alpha, minValue, maxValue);
        // how to use private toString() function
        auto stringDefault = zipfianDataGenerator->toString();

        // how to use private getName() function
        oss << zipfianDataGenerator->getName() << " (" << minValue << ", " << maxValue << ", " << alpha << ")";
        auto expectedString = oss.str();

        ASSERT_EQ(stringDefault, expectedString);
    }

    TEST_F(ZipfianDataGeneratorTest, createDataTest) {
        auto alpha = 0;
        auto minValue = 0;
        auto maxValue = 1000;

        auto zipfianDataGenerator = std::make_shared<ZipfianDataGenerator>(alpha, minValue, maxValue);
        // TODO data default
        auto dataDefault = zipfianDataGenerator->createData(100, sizeof(char));

        // TODO expected Data
        auto expectedData = ;

        ASSERT_EQ(dataDefault, expectedData);
    }
}//namespace NES::Benchmark::DataGeneration
