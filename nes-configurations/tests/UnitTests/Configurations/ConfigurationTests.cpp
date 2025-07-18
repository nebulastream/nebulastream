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

#include <cstddef>
#include <string>
#include <vector>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/BaseOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>

namespace NES
{

using namespace Configurations;

class ConfigurationTest : public Testing::BaseIntegrationTest
{
public:
    static void SetUpTestSuite()
    {
        NES::Logger::setupLogging("ConfigurationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup Configuration Help Message test class.");
    }
};

TEST_F(ConfigurationTest, HappyPath)
{
    struct TestConfig : BaseConfiguration
    {
        ScalarOption<std::string> a{"A", "False", "This is Option A"};
        ScalarOption<size_t> b{"B", "42", "This is Option B"};
        IntOption c{"C", "52", "This is Option C"};

    protected:
        std::vector<BaseOption*> getOptions() override { return {&a, &b, &c}; };
    };

    TestConfig config;

    config.overwriteConfigWithCommandLineInput({
        {"--A", "yes"},
        {"--B", "32"},
        {"--C", "-32.2"},
    });

    EXPECT_EQ(config.a.getValue(), "yes");
    EXPECT_EQ(config.c.getValue(), -32);
    EXPECT_EQ(config.b.getValue(), 32);
}


TEST_F(ConfigurationTest, Failure)
{
    struct TestConfig : BaseConfiguration
    {
        ScalarOption<std::string> a{"A", "False", "This is Option A"};
        ScalarOption<size_t> b{"B", "42", "This is Option B"};
        IntOption c{"C", "52", "This is Option C"};

    protected:
        std::vector<BaseOption*> getOptions() override { return {&a, &b, &c}; };
    };

    TestConfig config;

    EXPECT_ANY_THROW(config.overwriteConfigWithCommandLineInput({
        {"--A", "yes"},
        {"--B", "32"},
        {"--C", "A32.2"},
    }));
}
}
