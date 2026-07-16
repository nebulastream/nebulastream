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
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/BaseOption.hpp>
#include <Configurations/ByteAmount.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/ByteAmountValidation.hpp>
#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>
#include <ErrorHandling.hpp>

/// NOLINTBEGIN(readability-magic-numbers) -- byte-amount tests compare against literal byte counts throughout

namespace NES
{
namespace
{

void expectParses(const std::string& amount, const uint64_t expectedBytes)
{
    const auto parsed = parseByteAmount(amount);
    ASSERT_TRUE(parsed.has_value()) << "Expected \"" << amount << "\" to parse, but it was rejected";
    EXPECT_EQ(parsed.value(), expectedBytes) << "for input \"" << amount << "\"";
}

void expectRejects(const std::string& amount)
{
    EXPECT_FALSE(parseByteAmount(amount).has_value()) << "Expected \"" << amount << "\" to be rejected, but it parsed";
}

TEST(ByteAmountTest, ParsesPlainByteCounts)
{
    expectParses("0", 0);
    expectParses("1", 1);
    expectParses("4096", 4096);
    expectParses("18446744073709551615", UINT64_MAX);
    /// An optional, case-insensitive 'B' suffix is allowed
    expectParses("4096B", 4096);
    expectParses("4096b", 4096);
}

TEST(ByteAmountTest, ParsesBinarySuffixes)
{
    expectParses("4Ki", 4096);
    expectParses("1Mi", 1ULL << 20);
    expectParses("1Gi", 1ULL << 30);
    expectParses("1Ti", 1ULL << 40);
    expectParses("1Pi", 1ULL << 50);
    expectParses("1Ei", 1ULL << 60);
    expectParses("15Ei", 15ULL << 60);
}

TEST(ByteAmountTest, ParsesDecimalSuffixes)
{
    expectParses("4k", 4000);
    expectParses("4K", 4000);
    expectParses("1M", 1'000'000);
    expectParses("1G", 1'000'000'000);
    expectParses("1T", 1'000'000'000'000);
    expectParses("1P", 1'000'000'000'000'000);
    expectParses("1E", 1'000'000'000'000'000'000);
    expectParses("18E", 18'000'000'000'000'000'000ULL);
}

TEST(ByteAmountTest, SuffixMatchingIsCaseInsensitiveWithOptionalTrailingB)
{
    expectParses("4KiB", 4096);
    expectParses("4kib", 4096);
    expectParses("4KIB", 4096);
    expectParses("4kI", 4096);
    expectParses("4kb", 4000);
    expectParses("4KB", 4000);
    expectParses("4GB", 4'000'000'000);
    expectParses("4gib", 4ULL << 30);
}

TEST(ByteAmountTest, ParsesFractionalValues)
{
    expectParses("1.5Gi", 1'610'612'736);
    expectParses("0.5Ki", 512);
    expectParses("2.5k", 2500);
    expectParses("1.1k", 1100);
    expectParses(".5Ki", 512);
    /// Trailing zeros in the fraction carry no value
    expectParses("1.500Gi", 1'610'612'736);
    expectParses("2.0Ki", 2048);
}

TEST(ByteAmountTest, RejectsNegativeAndSignedValues)
{
    expectRejects("-1");
    expectRejects("-4Ki");
    expectRejects("+4Ki");
}

TEST(ByteAmountTest, RejectsOverflow)
{
    expectRejects("18446744073709551616"); /// UINT64_MAX + 1
    expectRejects("16Ei"); /// 2^64 bytes
    expectRejects("18.5E");
    expectRejects("99999999999999999999999999");
}

TEST(ByteAmountTest, RejectsNonIntegralResults)
{
    expectRejects("1.5"); /// 1.5 bytes
    expectRejects("1.5B");
    expectRejects("1.0000000001Gi");
    expectRejects("0.0001Ki");
}

TEST(ByteAmountTest, RejectsGarbage)
{
    expectRejects("");
    expectRejects(" ");
    expectRejects("Ki"); /// bare suffix
    expectRejects("B");
    expectRejects("abc");
    expectRejects("4X");
    expectRejects("4KiBB");
    expectRejects("4 Ki");
    expectRejects(" 4Ki");
    expectRejects("1..5Gi");
    expectRejects("1.Gi");
    expectRejects("1.");
    expectRejects("0x10");
    expectRejects("4Ki5");
}

TEST(ByteAmountValidationTest, AcceptsByteAmountsAndRejectsEverythingElse)
{
    const ByteAmountValidation validation;
    EXPECT_TRUE(validation.isValid("4096"));
    EXPECT_TRUE(validation.isValid("4KiB"));
    EXPECT_TRUE(validation.isValid("1.5Gi"));
    EXPECT_FALSE(validation.isValid("-1"));
    EXPECT_FALSE(validation.isValid("fourKiB"));
    EXPECT_FALSE(validation.isValid(""));
}

struct ByteOptionTestConfig : BaseConfiguration
{
    ByteOption bufferSize{"buffer_size", "4KiB", "Buffer size for the byte-option test", {std::make_shared<ByteAmountValidation>()}};

protected:
    std::vector<BaseOption*> getOptions() override { return {&bufferSize}; }
};

TEST(ByteOptionTest, DefaultValueSupportsHumanReadableAmounts)
{
    const ByteOptionTestConfig config;
    EXPECT_EQ(config.bufferSize.getValue(), 4096);
    /// The option value implicitly converts to a plain number of bytes
    const uint64_t rawBytes = config.bufferSize.getValue();
    EXPECT_EQ(rawBytes, 4096);
}

TEST(ByteOptionTest, ParsesHumanReadableAmountFromCommandLineInput)
{
    ByteOptionTestConfig config;
    std::unordered_map<std::string, std::string> inputParams{{"buffer_size", "8KiB"}};
    config.overwriteConfigWithCommandLineInput(inputParams);
    EXPECT_EQ(config.bufferSize.getValue(), 8192);
}

TEST(ByteOptionTest, ParsesHumanReadableAmountFromYAML)
{
    ByteOptionTestConfig config;
    YAML::Node node;
    node["buffer_size"] = "1.5Gi";
    config.overwriteConfigWithYAMLNode(node);
    EXPECT_EQ(config.bufferSize.getValue(), 1'610'612'736);
}

TEST(ByteOptionTest, PlainByteCountsKeepWorking)
{
    ByteOptionTestConfig config;
    YAML::Node node;
    node["buffer_size"] = "4096";
    config.overwriteConfigWithYAMLNode(node);
    EXPECT_EQ(config.bufferSize.getValue(), 4096);
}

TEST(ByteOptionTest, RejectsInvalidAmount)
{
    ByteOptionTestConfig config;
    std::unordered_map<std::string, std::string> inputParams{{"buffer_size", "4Foo"}};
    EXPECT_THROW(config.overwriteConfigWithCommandLineInput(inputParams), Exception);
}

}
}

/// NOLINTEND(readability-magic-numbers)
