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

namespace NES {
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
        auto expectedSchema = Schema::create()
                                  ->addField(createField("id", NES::UINT64))
                                  ->addField(createField("value", NES::UINT64))
                                  ->addField(createField("payload", NES::UINT64))
                                  ->addField(createField("timestamp", NES::UINT64));

        ASSERT_EQ(schemaDefault, expectedSchema);
    }

    TEST_F(DefaultDataGeneratorTest, getNameTest) {
        auto defaultDataGenerator = std::make_shared<DefaultDataGenerator>(/* minValue */ 0, /* maxValue */ 1000);
        auto nameDefault = defaultDataGenerator->getName();
        auto expectedName = "Uniform";

        ASSERT_EQ(nameDefault, expectedName);
    }
}//namespace NES
