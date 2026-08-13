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

#include <cstdint>
#include <string>
#include <tuple>
#include <variant>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/ConfigParsing.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <yaml-cpp/node/parse.h>
#include <yaml-cpp/yaml.h> /// NOLINT(misc-include-cleaner) - full definitions required for YAML::Load

#include <gtest/gtest.h>
#include <ErrorHandling.hpp>

///NOLINTBEGIN(bugprone-unchecked-optional-access)

namespace NES
{
namespace
{

template <typename T>
void expectParsesTo(const std::string& raw, const T& expected)
{
    const auto parsed = parseConfigLiteral(raw);
    ASSERT_TRUE(parsed.has_value()) << raw << ": " << parsed.error().what();
    ASSERT_TRUE(std::holds_alternative<T>(*parsed)) << raw;
    EXPECT_EQ(std::get<T>(*parsed), expected) << raw;
}

void expectParseFails(const std::string& raw)
{
    const auto parsed = parseConfigLiteral(raw);
    ASSERT_FALSE(parsed.has_value()) << raw;
    EXPECT_EQ(parsed.error().code(), ErrorCode::InvalidConfigParameter) << raw;
}

TEST(ConfigParsingTest, parseConfigLiteralInfersTypesAndValues)
{
    expectParsesTo("42", int64_t{42});
    expectParsesTo("-5", int64_t{-5});
    expectParsesTo("3.14", 3.14);
    expectParsesTo("true", true);
    expectParsesTo("FALSE", false);
    expectParsesTo("hello", std::string{"hello"});
    /// More than one dot is not fixpoint notation, so this falls through to string.
    expectParsesTo("1.2.3", std::string{"1.2.3"});
}

TEST(ConfigParsingTest, parseConfigLiteralKeepsSingleQuotedValuesAsStrings)
{
    expectParsesTo("'42'", std::string{"42"});
    expectParsesTo("''", std::string{});
}

TEST(ConfigParsingTest, parseConfigLiteralParsesNullCaseInsensitively)
{
    expectParsesTo("null", std::monostate{});
    expectParsesTo("NULL", std::monostate{});
}

TEST(ConfigParsingTest, parseConfigLiteralTrimsWhitespace)
{
    expectParsesTo(" \t42\n", int64_t{42});
    expectParsesTo("  hello  ", std::string{"hello"});
    /// Quotes are recognized after trimming; white space inside them survives.
    expectParsesTo("  ' spaced '  ", std::string{" spaced "});
}

TEST(ConfigParsingTest, parseConfigLiteralReportsErrors)
{
    expectParseFails("");
    expectParseFails(" \t ");
    /// One past int64 max.
    expectParseFails("9223372036854775808");
}

TEST(ConfigParsingTest, parseCommandLineConfigParsesKeysAndTypedValues)
{
    const auto parsed = parseCommandLineConfig({"--engine.query.threads=8", "--engine.enabled=true"});
    ASSERT_EQ(parsed.size(), 2);
    const auto threads = parsed.getFieldByName(QualifiedIdentifier::parse("engine.query.threads"));
    ASSERT_TRUE(threads.has_value());
    EXPECT_EQ(std::get<int64_t>(threads->getValue()), 8);
    const auto enabled = parsed.getFieldByName(QualifiedIdentifier::parse("engine.enabled"));
    ASSERT_TRUE(enabled.has_value());
    EXPECT_EQ(std::get<bool>(enabled->getValue()), true);
}

TEST(ConfigParsingTest, flattenYAMLConfigFlattensNestedMapsToTypedValues)
{
    const auto parsed = flattenYAMLConfig(YAML::Load(R"(
engine:
  query:
    threads: 8
  enabled: true
  version: '1.5'
)"));
    ASSERT_EQ(parsed.size(), 3);
    const auto threads = parsed.getFieldByName(QualifiedIdentifier::parse("engine.query.threads"));
    ASSERT_TRUE(threads.has_value());
    EXPECT_EQ(std::get<int64_t>(threads->getValue()), 8);
    const auto enabled = parsed.getFieldByName(QualifiedIdentifier::parse("engine.enabled"));
    ASSERT_TRUE(enabled.has_value());
    EXPECT_EQ(std::get<bool>(enabled->getValue()), true);
    /// Quoting makes the scalar explicitly a string, so it must not be inferred as a double.
    const auto version = parsed.getFieldByName(QualifiedIdentifier::parse("engine.version"));
    ASSERT_TRUE(version.has_value());
    EXPECT_EQ(std::get<std::string>(version->getValue()), "1.5");
}

TEST(ConfigParsingTest, parseCommandLineConfigRejectsMalformedArguments)
{
    /// Missing '='.
    EXPECT_THROW(std::ignore = parseCommandLineConfig({"--engine.query.threads"}), Exception);
    /// Same name set twice.
    EXPECT_THROW(std::ignore = parseCommandLineConfig({"--a.b=1", "--a.b=2"}), Exception);
}

}
}

///NOLINTEND(bugprone-unchecked-optional-access)
