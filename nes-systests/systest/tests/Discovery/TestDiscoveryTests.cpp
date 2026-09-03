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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>
#include <vector>

#include <gtest/gtest.h>

#include <Config/Config.hpp>
#include <Discovery/TestDiscovery.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>

namespace
{
std::filesystem::path createUniqueTempDirectory()
{
    static std::atomic_uint64_t counter = 0;
    const auto uniqueId = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path()
        / ("nes-systest-discovery-" + std::to_string(uniqueId) + "-" + std::to_string(counter.fetch_add(1)));
    std::filesystem::create_directories(path);
    return path;
}

class TemporaryDirectory
{
public:
    TemporaryDirectory() : path(createUniqueTempDirectory()) { }

    ~TemporaryDirectory()
    {
        std::error_code errorCode;
        std::filesystem::remove_all(path, errorCode);
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;
    TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;
    TemporaryDirectory(TemporaryDirectory&&) = delete;
    TemporaryDirectory& operator=(TemporaryDirectory&&) = delete;

    [[nodiscard]] const std::filesystem::path& get() const { return path; }

private:
    std::filesystem::path path;
};

void writeTextFile(const std::filesystem::path& path, const std::string& content)
{
    std::filesystem::create_directories(path.parent_path());
    std::ofstream out(path);
    ASSERT_TRUE(out.is_open()) << "Failed to open file " << path;
    out << content;
    out.close();
}
}

namespace NES
{
namespace
{
/// The name discovery settled for one file, so a test can name the file rather than its position in the result.
std::string nameOf(const DiscoveredTestFiles& discovered, const std::filesystem::path& file)
{
    const auto canonical = std::filesystem::weakly_canonical(file);
    const auto found = std::ranges::find(discovered, canonical, &DiscoveredTestFile::file);
    return found == discovered.end() ? std::string{"<not discovered>"} : found->name();
}
}

class TestDiscoveryTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("TestDiscoveryTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup TestDiscoveryTest test class.");
    }

    static void TearDownTestSuite() { NES_DEBUG("Tear down TestDiscoveryTest test class."); }
};

/// Giving a directory narrows what a run searches without moving the root a test file is keyed against.
/// Moving the root would key the same file differently depending on how the run was invoked, and would drop the
/// directory that keeps two files of the same stem apart.
TEST_F(TestDiscoveryTest, NarrowsTheSearchToADirectoryWithoutMovingTheKeyRoot)
{
    const TemporaryDirectory tempDir;
    writeTextFile(tempDir.get() / "wanted" / "a.test", "# groups:[x]\n");
    writeTextFile(tempDir.get() / "other" / "b.test", "# groups:[x]\n");

    Config config;
    config.testsDiscoverDir = tempDir.get().string();
    config.testFileExtension = ".test";
    config.searchDirs = {tempDir.get() / "wanted"};

    const auto discovered = discoverTestFiles(config);

    ASSERT_EQ(discovered.size(), 1);
    EXPECT_EQ(discovered.front().file, std::filesystem::weakly_canonical(tempDir.get() / "wanted" / "a.test"));
    EXPECT_EQ(config.testsDiscoverDir.getValue(), tempDir.get().string());
}

/// An empty subtree searches the whole root.
TEST_F(TestDiscoveryTest, SearchesTheWholeRootWhenNoDirectoryWasNamed)
{
    const TemporaryDirectory tempDir;
    writeTextFile(tempDir.get() / "wanted" / "a.test", "# groups:[x]\n");
    writeTextFile(tempDir.get() / "other" / "b.test", "# groups:[x]\n");

    Config config;
    config.testsDiscoverDir = tempDir.get().string();
    config.testFileExtension = ".test";

    EXPECT_EQ(discoverTestFiles(config).size(), 2);
}

TEST_F(TestDiscoveryTest, ExplicitlyIncludedGroupOverridesMatchingDisableConfigExclusion)
{
    const TemporaryDirectory tempDir;
    const auto largeFile = tempDir.get() / "large.test";
    const auto otherFile = tempDir.get() / "other.test";
    writeTextFile(largeFile, "# groups:[large]\n");
    writeTextFile(otherFile, "# groups:[other]\n");

    Config config;
    config.testsDiscoverDir = tempDir.get().string();
    config.testFileExtension = ".test";
    config.globalExcludedGroups = {"large"};
    config.testGroups.add("large");

    const auto discovered = discoverTestFiles(config);

    ASSERT_EQ(discovered.size(), 1);
    EXPECT_EQ(discovered.front().file, std::filesystem::weakly_canonical(largeFile));
}

TEST_F(TestDiscoveryTest, ExplicitCommandLineExclusionOverridesExplicitInclusion)
{
    const TemporaryDirectory tempDir;
    const auto joinFile = tempDir.get() / "join.test";
    writeTextFile(joinFile, "# groups:[Join]\n");

    Config config;
    config.testsDiscoverDir = tempDir.get().string();
    config.testFileExtension = ".test";
    config.testGroups.add("Join");
    config.excludeGroups.add("Join");

    const auto testMap = discoverTestFiles(config);

    EXPECT_TRUE(testMap.empty());
}

TEST_F(TestDiscoveryTest, DirectlySpecifiedTestFileOverridesDisabledTestFiles)
{
    const TemporaryDirectory tempDir;
    const auto joinFile = tempDir.get() / "join.test";
    writeTextFile(joinFile, "# groups:[Join]\n");

    Config config;
    config.directlySpecifiedTestFiles = joinFile.string();
    config.disabledTestFiles.add("join.test");

    const auto discovered = discoverTestFiles(config);

    ASSERT_EQ(discovered.size(), 1);
    EXPECT_EQ(discovered.front().file, std::filesystem::weakly_canonical(joinFile));
}

/// A stem that two files share keeps the directory that tells them apart, and a stem only one file has does not.
TEST_F(TestDiscoveryTest, OnlyDuplicateDiscoveredTestNamesIncludeRelativeDirectory)
{
    const TemporaryDirectory tempDir;
    writeTextFile(tempDir.get() / "left" / "same.test", "# groups:[Join]\n");
    writeTextFile(tempDir.get() / "right" / "same.test", "# groups:[Join]\n");
    writeTextFile(tempDir.get() / "right" / "unique.test", "# groups:[Join]\n");

    Config config;
    config.testsDiscoverDir = tempDir.get().string();
    config.testFileExtension = ".test";

    const auto discovered = discoverTestFiles(config);

    ASSERT_EQ(discovered.size(), 3);
    EXPECT_EQ(nameOf(discovered, tempDir.get() / "left" / "same.test"), "left/same");
    EXPECT_EQ(nameOf(discovered, tempDir.get() / "right" / "same.test"), "right/same");
    EXPECT_EQ(nameOf(discovered, tempDir.get() / "right" / "unique.test"), "unique");
}

/// The shortening runs after filtering, so a stem whose duplicate was filtered out is unique in what actually runs.
TEST_F(TestDiscoveryTest, FilteredDuplicateTestNameUsesStem)
{
    const TemporaryDirectory tempDir;
    writeTextFile(tempDir.get() / "included" / "same.test", "# groups:[Included]\n");
    writeTextFile(tempDir.get() / "filtered" / "same.test", "# groups:[Filtered]\n");

    Config config;
    config.testsDiscoverDir = tempDir.get().string();
    config.testFileExtension = ".test";
    config.testGroups.add("Included");

    const auto discovered = discoverTestFiles(config);

    ASSERT_EQ(discovered.size(), 1);
    EXPECT_EQ(discovered.front().name(), "same");
}

/// A file named on the command line reports under its stem, whatever directory it sits in.
TEST_F(TestDiscoveryTest, DirectlySpecifiedTestNamesUseStem)
{
    const TemporaryDirectory tempDir;
    const auto testFile = tempDir.get() / "left" / "same.test";
    writeTextFile(testFile, "# groups:[Join]\n");

    Config config;
    config.testsDiscoverDir = tempDir.get().string();
    config.directlySpecifiedTestFiles = testFile.string();

    const auto directlySpecified = discoverTestFiles(config);
    config.testQueryNumbers.add(1);
    const auto queryFiltered = discoverTestFiles(config);

    ASSERT_EQ(directlySpecified.size(), 1);
    ASSERT_EQ(queryFiltered.size(), 1);
    EXPECT_EQ(directlySpecified.front().name(), "same");
    EXPECT_EQ(queryFiltered.front().name(), "same");
}

/// Several directories narrow to the union of them, and a file below two of them is discovered once.
TEST_F(TestDiscoveryTest, SeveralSearchDirectoriesNarrowToTheirUnionWithoutRepeating)
{
    const TemporaryDirectory tempDir;
    writeTextFile(tempDir.get() / "wanted" / "a.test", "# groups:[x]\n");
    writeTextFile(tempDir.get() / "wanted" / "nested" / "b.test", "# groups:[x]\n");
    writeTextFile(tempDir.get() / "other" / "c.test", "# groups:[x]\n");

    Config config;
    config.testsDiscoverDir = tempDir.get().string();
    config.testFileExtension = ".test";
    config.searchDirs = {tempDir.get() / "wanted", tempDir.get() / "wanted" / "nested"};

    const auto discovered = discoverTestFiles(config);

    ASSERT_EQ(discovered.size(), 2);
    EXPECT_EQ(nameOf(discovered, tempDir.get() / "wanted" / "a.test"), "a");
    EXPECT_EQ(nameOf(discovered, tempDir.get() / "wanted" / "nested" / "b.test"), "b");
}
}
