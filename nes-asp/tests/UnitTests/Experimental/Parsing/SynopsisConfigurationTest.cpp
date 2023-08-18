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
#include <Experimental/Parsing/SynopsisConfiguration.hpp>
#include <Util/Logger/LogLevel.hpp>

namespace NES::ASP::Parsing {
    class SynopsisConfigurationTest : public Testing::BaseIntegrationTest {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("SynopsisConfigurationTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup SynopsisConfigurationTest test class.");
        }

        /* Will be called before a test is executed. */
        void SetUp() override {
            Testing::BaseIntegrationTest::SetUp();
            NES_INFO("Setup SynopsisConfigurationTest test case.");
        }
    };

    TEST_F(SynopsisConfigurationTest, testCreate) {
        /* Creating random values */
        auto numberOfTypes = magic_enum::enum_values<Synopsis_Type>().size();
        auto type = magic_enum::enum_cast<Synopsis_Type>(rand() % numberOfTypes).value();
        size_t width = rand();
        size_t height = rand();
        size_t windowSize = rand();

        auto synopsisConfig = SynopsisConfiguration::create(type, width, height, windowSize);
        ASSERT_EQ(type, synopsisConfig->type.getValue());
        ASSERT_EQ(width, synopsisConfig->width.getValue());
        ASSERT_EQ(height, synopsisConfig->height.getValue());
        ASSERT_EQ(windowSize, synopsisConfig->windowSize.getValue());

        Yaml::Node yamlNode;
        yamlNode["type"] = std::string(magic_enum::enum_name(type));
        yamlNode["width"] = std::to_string(width);
        yamlNode["height"] = std::to_string(height);
        yamlNode["windowSize"] = std::to_string(windowSize);

        auto parsedSynopsisConfig = SynopsisConfiguration::createArgumentsFromYamlNode(yamlNode);
        ASSERT_EQ(type, parsedSynopsisConfig->type.getValue());
        ASSERT_EQ(width, parsedSynopsisConfig->width.getValue());
        ASSERT_EQ(height, parsedSynopsisConfig->height.getValue());
        ASSERT_EQ(windowSize, parsedSynopsisConfig->windowSize.getValue());
    }

    TEST_F(SynopsisConfigurationTest, testingGetHeaderAndRowCSVAndToString) {
        auto type = Synopsis_Type::ECM;
        size_t width = 42;
        size_t height = 43;
        size_t windowSize = 44;

        auto synopsisConfig = SynopsisConfiguration::create(type, width, height, windowSize);
        auto headerCsvString = synopsisConfig->getHeaderAsCsv();
        auto valuesCsvString = synopsisConfig->getValuesAsCsv();
        auto toString = synopsisConfig->toString();

        EXPECT_EQ(headerCsvString, "synopsis_type,synopsis_width,synopsis_height,synopsis_windowSize");
        EXPECT_EQ(valuesCsvString, "ECM,42,43,44");
        EXPECT_EQ(toString, "type (ECM) width (42) height (43) windowSize (44)");
    }
} // namespace NES::ASP:Parsing