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

#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>

#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <GeneratorRate.hpp>
#include <GeneratorSource.hpp>

namespace NES
{

class GeneratorSourceValidationTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("GeneratorSourceValidationTest.log", LogLevel::LOG_DEBUG); }
};

namespace
{
std::unordered_map<std::string, std::string> validConfig()
{
    return {{"GENERATOR_SCHEMA", "SEQUENCE UINT64 0 10 1"}, {"STOP_GENERATOR_WHEN_SEQUENCE_FINISHES", "ALL"}};
}

void expectInvalidConfig(const std::unordered_map<std::string, std::string>& config, const std::string_view expectedMessagePart)
{
    const auto result = GeneratorSource::validateAndFormat(config);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::InvalidConfigParameter);
    EXPECT_THAT(result.error().what(), ::testing::HasSubstr(std::string{expectedMessagePart}));
}
}

TEST_F(GeneratorSourceValidationTest, ValidConfigIsAccepted)
{
    const auto result = GeneratorSource::validateAndFormat(validConfig());
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(std::get<std::string>(result->at("GENERATOR_SCHEMA")), "SEQUENCE UINT64 0 10 1");
    /// Unspecified parameters with defaults are filled in.
    EXPECT_EQ(std::get<EnumWrapper>(result->at("GENERATOR_RATE_TYPE")).asEnum<GeneratorRate::Type>(), GeneratorRate::Type::FIXED);
}

TEST_F(GeneratorSourceValidationTest, ValidMultiFieldSchemaIsAccepted)
{
    auto config = validConfig();
    config["GENERATOR_SCHEMA"] = "SEQUENCE UINT64 0 20 1, RANDOMSTR 1 6, NORMAL_DISTRIBUTION FLOAT64 5 1";
    EXPECT_TRUE(GeneratorSource::validateAndFormat(config).has_value());
}

TEST_F(GeneratorSourceValidationTest, ValidSinusRateConfigIsAccepted)
{
    auto config = validConfig();
    config["GENERATOR_RATE_TYPE"] = "SINUS";
    config["GENERATOR_RATE_CONFIG"] = "amplitude 10, frequency 5";
    EXPECT_TRUE(GeneratorSource::validateAndFormat(config).has_value());
}

TEST_F(GeneratorSourceValidationTest, MissingSchemaIsRejected)
{
    auto config = validConfig();
    config.erase("GENERATOR_SCHEMA");
    expectInvalidConfig(config, "Generator schema cannot be empty");
}

TEST_F(GeneratorSourceValidationTest, EmptySchemaIsRejected)
{
    auto config = validConfig();
    config["GENERATOR_SCHEMA"] = "";
    expectInvalidConfig(config, "Generator schema cannot be empty");
}

TEST_F(GeneratorSourceValidationTest, UnknownFieldTypeIsRejected)
{
    auto config = validConfig();
    config["GENERATOR_SCHEMA"] = "FOO UINT64 0 10 1";
    expectInvalidConfig(config, "Cannot identify the type of field");
}

TEST_F(GeneratorSourceValidationTest, SequenceFieldWithWrongParameterCountIsRejected)
{
    auto config = validConfig();
    config["GENERATOR_SCHEMA"] = "SEQUENCE UINT64 0 10";
    expectInvalidConfig(config, "Number of SequenceField parameters does not match");
}

TEST_F(GeneratorSourceValidationTest, SequenceFieldWithInvalidTypeIsRejected)
{
    auto config = validConfig();
    config["GENERATOR_SCHEMA"] = "SEQUENCE FOO 0 10 1";
    expectInvalidConfig(config, "Invalid SequenceField type of FOO");
}

TEST_F(GeneratorSourceValidationTest, SequenceFieldWithVarsizedTypeIsRejected)
{
    auto config = validConfig();
    config["GENERATOR_SCHEMA"] = "SEQUENCE VARSIZED 0 10 1";
    expectInvalidConfig(config, "Could not parse VARSIZED as SequenceField");
}

TEST_F(GeneratorSourceValidationTest, SequenceFieldWithUnparsableBoundIsRejected)
{
    auto config = validConfig();
    config["GENERATOR_SCHEMA"] = "SEQUENCE UINT64 abc 10 1";
    expectInvalidConfig(config, "Could not parse abc as SequenceField start");
}

TEST_F(GeneratorSourceValidationTest, NormalDistributionWithWrongParameterCountIsRejected)
{
    auto config = validConfig();
    config["GENERATOR_SCHEMA"] = "NORMAL_DISTRIBUTION FLOAT64 5";
    expectInvalidConfig(config, "Invalid NORMAL_DISTRIBUTION schema line");
}

TEST_F(GeneratorSourceValidationTest, NormalDistributionWithInvalidTypeIsRejected)
{
    auto config = validConfig();
    config["GENERATOR_SCHEMA"] = "NORMAL_DISTRIBUTION FOO 5 1";
    expectInvalidConfig(config, "Invalid Type in NORMAL_DISTRIBUTION");
}

TEST_F(GeneratorSourceValidationTest, NormalDistributionWithUnparsableMeanIsRejected)
{
    auto config = validConfig();
    config["GENERATOR_SCHEMA"] = "NORMAL_DISTRIBUTION FLOAT64 abc 1";
    expectInvalidConfig(config, "Can not parse mean or stddev");
}

TEST_F(GeneratorSourceValidationTest, NormalDistributionWithNegativeStddevIsRejected)
{
    auto config = validConfig();
    config["GENERATOR_SCHEMA"] = "NORMAL_DISTRIBUTION FLOAT64 5 -1";
    expectInvalidConfig(config, "Stddev must be non-negative");
}

TEST_F(GeneratorSourceValidationTest, WordListWithWrongParameterCountIsRejected)
{
    auto config = validConfig();
    config["GENERATOR_SCHEMA"] = "WORDLIST";
    expectInvalidConfig(config, "Invalid WORDLIST schema line");
}

TEST_F(GeneratorSourceValidationTest, WordListWithMissingFileIsRejected)
{
    auto config = validConfig();
    config["GENERATOR_SCHEMA"] = "WORDLIST this-file-does-not-exist.csv";
    expectInvalidConfig(config, "does not exist");
}

TEST_F(GeneratorSourceValidationTest, RandomStrWithWrongParameterCountIsRejected)
{
    auto config = validConfig();
    config["GENERATOR_SCHEMA"] = "RANDOMSTR 1";
    expectInvalidConfig(config, "Invalid RANDOMSTR schema line");
}

TEST_F(GeneratorSourceValidationTest, RandomStrWithUnparsableMinLengthIsRejected)
{
    auto config = validConfig();
    config["GENERATOR_SCHEMA"] = "RANDOMSTR abc 5";
    expectInvalidConfig(config, "Cannot parse MINLENGTH");
}

TEST_F(GeneratorSourceValidationTest, RandomStrWithZeroMinLengthIsRejected)
{
    auto config = validConfig();
    config["GENERATOR_SCHEMA"] = "RANDOMSTR 0 5";
    expectInvalidConfig(config, "MINLENGTH");
}

TEST_F(GeneratorSourceValidationTest, RandomStrWithMinLengthGreaterMaxLengthIsRejected)
{
    auto config = validConfig();
    config["GENERATOR_SCHEMA"] = "RANDOMSTR 6 5";
    expectInvalidConfig(config, "The MINLENGTH can not be longer than the MAXLENGTH");
}

TEST_F(GeneratorSourceValidationTest, InvalidStopBehaviorIsRejected)
{
    auto config = validConfig();
    config["STOP_GENERATOR_WHEN_SEQUENCE_FINISHES"] = "SOMETIMES";
    expectInvalidConfig(config, "STOP_GENERATOR_WHEN_SEQUENCE_FINISHES: \"SOMETIMES\"");
}

TEST_F(GeneratorSourceValidationTest, MissingStopBehaviorIsRejected)
{
    auto config = validConfig();
    config.erase("STOP_GENERATOR_WHEN_SEQUENCE_FINISHES");
    expectInvalidConfig(config, "Non-default parameter STOP_GENERATOR_WHEN_SEQUENCE_FINISHES not specified");
}

TEST_F(GeneratorSourceValidationTest, InvalidRateTypeIsRejected)
{
    auto config = validConfig();
    config["GENERATOR_RATE_TYPE"] = "QUADRATIC";
    expectInvalidConfig(config, "GENERATOR_RATE_TYPE: \"QUADRATIC\"");
}

TEST_F(GeneratorSourceValidationTest, InvalidRateConfigIsRejected)
{
    auto config = validConfig();
    config["GENERATOR_RATE_CONFIG"] = "emit_rate abc";
    expectInvalidConfig(config, "GENERATOR_RATE_CONFIG: \"emit_rate abc\"");
}

TEST_F(GeneratorSourceValidationTest, UnknownParameterIsRejected)
{
    auto config = validConfig();
    config["FOO"] = "BAR";
    expectInvalidConfig(config, "Unknown configuration parameter: FOO");
}

}
