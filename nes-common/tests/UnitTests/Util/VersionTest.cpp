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
#include <vector>
#include <Util/VersionPluginList.hpp>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>
#include <Version.hpp>

namespace NES
{

namespace
{
/// Registers fake providers once, mirroring how the generated registrar translation units announce their plugins.
/// The two "TestSources" providers overlap to cover merging and deduplication across registries of the same kind.
/// Registration happens lazily inside the tests (instead of at static initialization time) to satisfy cert-err58-cpp.
void registerTestPluginProviders()
{
    [[maybe_unused]] static const bool Registered = []
    {
        auto& pluginList = VersionPluginList::instance();
        pluginList.addProvider("TestSources", [] { return std::vector<std::string>{"TCP", "FILE"}; });
        pluginList.addProvider("TestSources", [] { return std::vector<std::string>{"FILE", "GENERATOR"}; });
        pluginList.addProvider("TestFunctions", [] { return std::vector<std::string>{"ADD"}; });
        pluginList.addProvider("TestEmptyKind", [] { return std::vector<std::string>{}; });
        return true;
    }();
}
}

TEST(VersionTest, formatVersionContainsBinaryName)
{
    const auto output = formatVersion("my-binary");
    EXPECT_THAT(output, testing::HasSubstr("my-binary"));
}

TEST(VersionTest, formatVersionContainsAllFields)
{
    registerTestPluginProviders();
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
    EXPECT_THAT(output, testing::HasSubstr("plugins:"));
}

TEST(VersionTest, formatVersionListsPluginsGroupedByKind)
{
    registerTestPluginProviders();
    const auto output = formatVersion("nes-single-node-worker");
    /// Names of the same kind are merged across providers, deduplicated, and sorted.
    /// The kind labels are padded to the longest kind, hence no fixed spacing between label and names.
    EXPECT_THAT(output, testing::HasSubstr("TestSources:"));
    EXPECT_THAT(output, testing::HasSubstr("FILE, GENERATOR, TCP"));
    /// Padding-agnostic: the label-to-names spacing depends on the longest kind linked into THIS
    /// binary, which varies by build configuration (some axes link the real registries into the test).
    EXPECT_THAT(output, testing::ContainsRegex("TestFunctions: +ADD"));
    /// Kinds without any plugins are omitted entirely.
    EXPECT_THAT(output, testing::Not(testing::HasSubstr("TestEmptyKind")));
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
