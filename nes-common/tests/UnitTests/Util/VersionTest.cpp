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

#include <array>
#include <string>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>
#include <Version.hpp>

namespace NES
{

TEST(VersionTest, formatVersionContainsBinaryName)
{
    const auto output = formatVersion("my-binary");
    EXPECT_THAT(output, testing::HasSubstr("my-binary"));
}

TEST(VersionTest, formatVersionContainsAllFields)
{
    const auto output = formatVersion("nes-single-node-worker");
    /// The labels for every embedded build-info field must be present.
    EXPECT_THAT(output, testing::HasSubstr("commit:"));
    EXPECT_THAT(output, testing::HasSubstr("built:"));
    EXPECT_THAT(output, testing::HasSubstr("build:"));
    EXPECT_THAT(output, testing::HasSubstr("sanitizer:"));
    EXPECT_THAT(output, testing::HasSubstr("compiler:"));
    EXPECT_THAT(output, testing::HasSubstr("compiler flags:"));
    EXPECT_THAT(output, testing::HasSubstr("stdlib:"));
    EXPECT_THAT(output, testing::HasSubstr("log level:"));
    EXPECT_THAT(output, testing::HasSubstr("vcpkg baseline:"));
}

TEST(VersionTest, formatVersionFirstLineIsBinaryNameAndVersion)
{
    const auto output = formatVersion("checksum");
    const auto firstLineEnd = output.find('\n');
    ASSERT_NE(firstLineEnd, std::string::npos);
    const auto firstLine = output.substr(0, firstLineEnd);
    EXPECT_THAT(firstLine, testing::StartsWith("checksum "));
    /// The version placeholder must be non-empty on the first line.
    EXPECT_GT(firstLine.size(), std::string("checksum ").size());
}

TEST(VersionTest, hasVersionFlagDetectsShortAndLongForms)
{
    {
        const std::array argv{"binary", "--version"};
        EXPECT_TRUE(hasVersionFlag(static_cast<int>(argv.size()), argv.data()));
    }
    {
        const std::array argv{"binary", "-v"};
        EXPECT_TRUE(hasVersionFlag(static_cast<int>(argv.size()), argv.data()));
    }
    {
        const std::array argv{"binary", "-v", "extra"};
        EXPECT_TRUE(hasVersionFlag(static_cast<int>(argv.size()), argv.data()));
    }
}

TEST(VersionTest, hasVersionFlagReturnsFalseWithoutFlag)
{
    const std::array argv{"binary", "--help", "foo"};
    EXPECT_FALSE(hasVersionFlag(static_cast<int>(argv.size()), argv.data()));
}

TEST(VersionTest, hasVersionFlagHandlesEmptyArgv)
{
    const std::array argv{"binary"};
    EXPECT_FALSE(hasVersionFlag(static_cast<int>(argv.size()), argv.data()));
    EXPECT_FALSE(hasVersionFlag(0, nullptr));
}

}
