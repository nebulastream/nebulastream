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

#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/Enums/EnumOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Util.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>

namespace NES
{

using namespace Configurations;

class ConfigHelpMessageTest : public Testing::BaseIntegrationTest
{
public:
    static void SetUpTestSuite()
    {
        NES::Logger::setupLogging("ConfigHelpMessageTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup Configuration Help Message test class.");
    }
};

TEST_F(ConfigHelpMessageTest, ShouldGenerateHelpMessageForDifferentTypes)
{
    enum class TestEnum
    {
        YES,
        NO
    };

    struct InnerConfiguration : BaseConfiguration
    {
    protected:
        std::vector<BaseOption*> getOptions() override { return {&b}; }

    public:
        InnerConfiguration(std::string name, std::string description) : BaseConfiguration(std::move(name), std::move(description)) { }

        InnerConfiguration() = default;
        ScalarOption<size_t> b{"B", "54", "This is Inner Option B"};
    };

    struct TestConfig : BaseConfiguration
    {
        ScalarOption<std::string> a{"A", "False", "This is Option A"};
        ScalarOption<size_t> b{"B", "42", "This is Option B"};
        EnumOption<TestEnum> c{"C", TestEnum::YES, "This is Option C"};
        SequenceOption<InnerConfiguration> d{"D", "This is Option D"};
        InnerConfiguration e{"E", "This is Option E"};

    protected:
        std::vector<BaseOption*> getOptions() override { return {&a, &b, &c, &d, &e}; }
    };

    std::stringstream ss;
    Configurations::generateHelp<TestConfig>(ss);

    EXPECT_EQ(
        ss.str(),
        R"(    - A: This is Option A (Default: False)
    - B: This is Option B (Default: 42)
    - C: This is Option C (Default: YES)
    - D: This is Option D (Multiple)
        - B: This is Inner Option B (Default: 54)
    - E: This is Option E
        - B: This is Inner Option B (Default: 54)
)");
}
}
