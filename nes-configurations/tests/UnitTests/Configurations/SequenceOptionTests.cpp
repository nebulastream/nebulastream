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
#include <Configurations/SequenceOption.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>
#include <BaseIntegrationTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

class ScalarSequenceConfiguration final : public BaseConfiguration
{
public:
    SequenceOption<StringOption> testGroups{"test_groups", "Test groups to run"};
    SequenceOption<UIntOption> testQueryNumbers{"test_query_numbers", "Test query numbers to run"};

protected:
    std::vector<BaseOption*> getOptions() override { return {&testGroups, &testQueryNumbers}; }
};

class SequenceOptionCliParsingTest : public Testing::BaseIntegrationTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("SequenceOptionCliParsingTest.log", LogLevel::LOG_DEBUG); }
};

TEST_F(SequenceOptionCliParsingTest, ParsesStringValueFromNormalKey)
{
    ScalarSequenceConfiguration config;

    EXPECT_NO_THROW(config.overwriteConfigWithCommandLineInput({{"--test_groups", "Join"}}));

    ASSERT_EQ(config.testGroups.size(), 1U);
    EXPECT_EQ(config.testGroups[0].getValue(), "Join");
}

TEST_F(SequenceOptionCliParsingTest, ParsesUnsignedIntegerValueFromNormalKey)
{
    ScalarSequenceConfiguration config;

    EXPECT_NO_THROW(config.overwriteConfigWithCommandLineInput({{"--test_query_numbers", "42"}}));

    ASSERT_EQ(config.testQueryNumbers.size(), 1U);
    EXPECT_EQ(config.testQueryNumbers[0].getValue(), 42U);
}

TEST_F(SequenceOptionCliParsingTest, RejectsTrailingDotKey)
{
    ScalarSequenceConfiguration config;

    EXPECT_THROW(config.overwriteConfigWithCommandLineInput({{"--test_groups.", "Join"}}), Exception);
    EXPECT_TRUE(config.testGroups.empty());
}

TEST_F(SequenceOptionCliParsingTest, RejectsEmptyValue)
{
    ScalarSequenceConfiguration config;

    EXPECT_THROW(config.overwriteConfigWithCommandLineInput({{"--test_groups", ""}}), Exception);
    EXPECT_TRUE(config.testGroups.empty());
}

TEST_F(SequenceOptionCliParsingTest, DoesNotSplitCommaSeparatedStringValue)
{
    ScalarSequenceConfiguration config;

    EXPECT_NO_THROW(config.overwriteConfigWithCommandLineInput({{"--test_groups", "Join,Aggregation"}}));

    ASSERT_EQ(config.testGroups.size(), 1U);
    EXPECT_EQ(config.testGroups[0].getValue(), "Join,Aggregation");
}

TEST_F(SequenceOptionCliParsingTest, PreservesYamlSequenceParsing)
{
    ScalarSequenceConfiguration config;
    const auto yaml = YAML::Load(R"(
test_groups:
  - Join
  - Aggregation
test_query_numbers:
  - 3
  - 7
)");

    EXPECT_NO_THROW(config.overwriteConfigWithYAMLNode(yaml));

    ASSERT_EQ(config.testGroups.size(), 2U);
    EXPECT_EQ(config.testGroups[0].getValue(), "Join");
    EXPECT_EQ(config.testGroups[1].getValue(), "Aggregation");
    ASSERT_EQ(config.testQueryNumbers.size(), 2U);
    EXPECT_EQ(config.testQueryNumbers[0].getValue(), 3U);
    EXPECT_EQ(config.testQueryNumbers[1].getValue(), 7U);
}

TEST_F(SequenceOptionCliParsingTest, PreservesProgrammaticAdd)
{
    ScalarSequenceConfiguration config;

    config.testGroups.add("Join");
    config.testGroups.add("Aggregation");
    config.testQueryNumbers.add(3U);
    config.testQueryNumbers.add(7U);

    ASSERT_EQ(config.testGroups.size(), 2U);
    EXPECT_EQ(config.testGroups[0].getValue(), "Join");
    EXPECT_EQ(config.testGroups[1].getValue(), "Aggregation");
    ASSERT_EQ(config.testQueryNumbers.size(), 2U);
    EXPECT_EQ(config.testQueryNumbers[0].getValue(), 3U);
    EXPECT_EQ(config.testQueryNumbers[1].getValue(), 7U);
}

}
