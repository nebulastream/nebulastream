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
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

#include <Config/Config.hpp>
#include <Discovery/TestDiscovery.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <TemporaryDirectory.hpp>

namespace
{
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
/// This file's discovered name, so a test can refer to the file directly (instead of its position in the result).
std::string nameOf(const std::vector<DiscoveredTestFile>& discovered, const std::filesystem::path& file)
{
    const auto canonical = std::filesystem::weakly_canonical(file);
    const auto found = std::ranges::find(discovered, canonical, &DiscoveredTestFile::file);
    return found == discovered.end() ? std::string{"<not discovered>"} : found->name().getRawValue();
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

/// A name is counted from the root, not from the directory that the run searched, so a result file path does not move
/// when a run is narrowed. A run of the whole root reports `benchmark/a/DEBS`, and searching only `benchmark` reports
/// the same.
TEST_F(TestDiscoveryTest, NarrowingTheSearchKeepsTheNamesOfAFullRun)
{
    const Testing::TemporaryDirectory tempDir;
    writeTextFile(tempDir.get() / "benchmark" / "a" / "DEBS.test", "# groups:[x]\n");
    writeTextFile(tempDir.get() / "benchmark" / "b" / "DEBS.test", "# groups:[x]\n");

    SystestConfiguration config;
    config.testDiscoverRoot = tempDir.get().string();
    config.testFileExtension = ".test";
    config.testDiscoverDirs.add((tempDir.get() / "benchmark").string());

    const auto discovered = discoverTestFiles(config);

    ASSERT_EQ(discovered.size(), 2);
    EXPECT_EQ(nameOf(discovered, tempDir.get() / "benchmark" / "a" / "DEBS.test"), "benchmark/a/DEBS");
    EXPECT_EQ(nameOf(discovered, tempDir.get() / "benchmark" / "b" / "DEBS.test"), "benchmark/b/DEBS");
}

/// A relative search directory selects the same files under the same test names as its absolute form.
/// Discovery resolves it to an absolute path first, because a relative path would match neither the root nor the discovered files.
TEST_F(TestDiscoveryTest, RelativeSearchDirectoriesResolveAgainstTheWorkingDirectory)
{
    const Testing::TemporaryDirectory tempDir;
    writeTextFile(tempDir.get() / "benchmark" / "a" / "DEBS.test", "# groups:[x]\n");
    const auto relativeDir = std::filesystem::proximate(tempDir.get() / "benchmark");
    ASSERT_TRUE(relativeDir.is_relative());

    SystestConfiguration config;
    config.testDiscoverRoot = tempDir.get().string();
    config.testFileExtension = ".test";
    config.testDiscoverDirs.add(relativeDir.string());

    const auto discovered = discoverTestFiles(config);

    ASSERT_EQ(discovered.size(), 1);
    EXPECT_EQ(nameOf(discovered, tempDir.get() / "benchmark" / "a" / "DEBS.test"), "benchmark/a/DEBS");
}

/// A run without -t has no search directory, and every file below the root is a candidate.
TEST_F(TestDiscoveryTest, SearchesTheWholeRootWhenNoDirectoryWasNamed)
{
    const Testing::TemporaryDirectory tempDir;
    writeTextFile(tempDir.get() / "wanted" / "a.test", "# groups:[x]\n");
    writeTextFile(tempDir.get() / "other" / "b.test", "# groups:[x]\n");

    SystestConfiguration config;
    config.testDiscoverRoot = tempDir.get().string();
    config.testFileExtension = ".test";

    EXPECT_EQ(discoverTestFiles(config).size(), 2);
}

TEST_F(TestDiscoveryTest, ExplicitlyIncludedGroupOverridesMatchingDisableConfigExclusion)
{
    const Testing::TemporaryDirectory tempDir;
    const auto largeFile = tempDir.get() / "large.test";
    const auto otherFile = tempDir.get() / "other.test";
    writeTextFile(largeFile, "# groups:[large]\n");
    writeTextFile(otherFile, "# groups:[other]\n");

    SystestConfiguration config;
    config.testDiscoverRoot = tempDir.get().string();
    config.testFileExtension = ".test";
    config.globalExcludedGroups = {"large"};
    config.testGroups.add("large");

    const auto discovered = discoverTestFiles(config);

    ASSERT_EQ(discovered.size(), 1);
    EXPECT_EQ(discovered.front().file, std::filesystem::weakly_canonical(largeFile));
}

TEST_F(TestDiscoveryTest, ExplicitCommandLineExclusionOverridesExplicitInclusion)
{
    const Testing::TemporaryDirectory tempDir;
    const auto joinFile = tempDir.get() / "join.test";
    writeTextFile(joinFile, "# groups:[Join]\n");

    SystestConfiguration config;
    config.testDiscoverRoot = tempDir.get().string();
    config.testFileExtension = ".test";
    config.testGroups.add("Join");
    config.excludeGroups.add("Join");

    const auto testMap = discoverTestFiles(config);

    EXPECT_TRUE(testMap.empty());
}

TEST_F(TestDiscoveryTest, DirectlySpecifiedTestFileOverridesDisabledTestFiles)
{
    const Testing::TemporaryDirectory tempDir;
    const auto joinFile = tempDir.get() / "join.test";
    writeTextFile(joinFile, "# groups:[Join]\n");

    SystestConfiguration config;
    config.directlySpecifiedTestFiles = joinFile.string();
    config.disabledTestFiles.add("join.test");

    const auto discovered = discoverTestFiles(config);

    ASSERT_EQ(discovered.size(), 1);
    EXPECT_EQ(discovered.front().file, std::filesystem::weakly_canonical(joinFile));
}

/// A name is the path below the root whether or not another file shares the file name, so the name says where the
/// file is.
TEST_F(TestDiscoveryTest, NamesAreThePathBelowTheRoot)
{
    const Testing::TemporaryDirectory tempDir;
    writeTextFile(tempDir.get() / "left" / "same.test", "# groups:[Join]\n");
    writeTextFile(tempDir.get() / "right" / "same.test", "# groups:[Join]\n");
    writeTextFile(tempDir.get() / "right" / "unique.test", "# groups:[Join]\n");

    SystestConfiguration config;
    config.testDiscoverRoot = tempDir.get().string();
    config.testFileExtension = ".test";

    const auto discovered = discoverTestFiles(config);

    ASSERT_EQ(discovered.size(), 3);
    EXPECT_EQ(nameOf(discovered, tempDir.get() / "left" / "same.test"), "left/same");
    EXPECT_EQ(nameOf(discovered, tempDir.get() / "right" / "same.test"), "right/same");
    EXPECT_EQ(nameOf(discovered, tempDir.get() / "right" / "unique.test"), "right/unique");
}

TEST_F(TestDiscoveryTest, DirectlySpecifiedTestFilesKeepTheNamesOfAFullRun)
{
    const Testing::TemporaryDirectory tempDir;
    const auto testFile = tempDir.get() / "left" / "same.test";
    writeTextFile(testFile, "# groups:[Join]\n");

    SystestConfiguration config;
    config.testDiscoverRoot = tempDir.get().string();
    config.directlySpecifiedTestFiles = testFile.string();

    const auto directlySpecified = discoverTestFiles(config);
    config.testQueryNumbers.add(1);
    const auto queryFiltered = discoverTestFiles(config);

    ASSERT_EQ(directlySpecified.size(), 1);
    ASSERT_EQ(queryFiltered.size(), 1);
    EXPECT_EQ(directlySpecified.front().name().getRawValue(), "left/same");
    EXPECT_EQ(queryFiltered.front().name().getRawValue(), "left/same");
    /// Without numbers every query runs, which is a missing filter rather than an empty one.
    EXPECT_FALSE(directlySpecified.front().enabledQueries.has_value());
    EXPECT_EQ(queryFiltered.front().enabledQueries, std::optional{std::unordered_set<SystestQueryId>{SystestQueryId(1)}});
}

/// The command line can give a directly specified file as a relative path.
/// Discovery resolves it against the working directory, so the file keeps the test name of a full run.
TEST_F(TestDiscoveryTest, RelativeDirectlySpecifiedTestFilesResolveAgainstTheWorkingDirectory)
{
    const Testing::TemporaryDirectory tempDir;
    const auto testFile = tempDir.get() / "left" / "same.test";
    writeTextFile(testFile, "# groups:[Join]\n");
    const auto relativeFile = std::filesystem::proximate(testFile);
    ASSERT_TRUE(relativeFile.is_relative());

    SystestConfiguration config;
    config.testDiscoverRoot = tempDir.get().string();
    config.directlySpecifiedTestFiles = relativeFile.string();

    const auto discovered = discoverTestFiles(config);

    ASSERT_EQ(discovered.size(), 1);
    EXPECT_EQ(discovered.front().file, std::filesystem::weakly_canonical(testFile));
    EXPECT_EQ(discovered.front().name().getRawValue(), "left/same");
}

TEST_F(TestDiscoveryTest, DirectlySpecifiedTestFilesOutsideTheRootAreNamedFromTheirDirectory)
{
    const Testing::TemporaryDirectory root;
    const Testing::TemporaryDirectory outside;
    const auto testFile = outside.get() / "sub" / "Alpha.test";
    writeTextFile(testFile, "# groups:[x]\n");

    SystestConfiguration config;
    config.testDiscoverRoot = root.get().string();
    config.directlySpecifiedTestFiles = testFile.string();

    const auto discovered = discoverTestFiles(config);

    ASSERT_EQ(discovered.size(), 1);
    EXPECT_EQ(discovered.front().name().getRawValue(), "sub/Alpha");
}

/// A directory outside the root has no path below the root, so its files are named from the directory's parent: the
/// directory's own name is part of the test name, and no name contains `..`, which would put a result file outside
/// 'results/'.
TEST_F(TestDiscoveryTest, FilesOutsideTheRootAreNamedFromTheirDirectory)
{
    const Testing::TemporaryDirectory root;
    const Testing::TemporaryDirectory outside;
    writeTextFile(outside.get() / "sub" / "Alpha.test", "# groups:[x]\n");
    writeTextFile(outside.get() / "other" / "Alpha.test", "# groups:[x]\n");

    SystestConfiguration config;
    config.testDiscoverRoot = root.get().string();
    config.testFileExtension = ".test";
    config.testDiscoverDirs.add(outside.get().string());

    const auto discovered = discoverTestFiles(config);

    ASSERT_EQ(discovered.size(), 2);
    const auto outsideName = outside.get().filename().string();
    EXPECT_EQ(nameOf(discovered, outside.get() / "sub" / "Alpha.test"), outsideName + "/sub/Alpha");
    EXPECT_EQ(nameOf(discovered, outside.get() / "other" / "Alpha.test"), outsideName + "/other/Alpha");
}

/// Two directories outside the root can each hold a file of the same name. Each name starts with the directory that
/// holds the file, so the run reports them apart and their result files stay apart.
TEST_F(TestDiscoveryTest, FilesOfTheSameNameInSeparateSearchDirectoriesStayApart)
{
    const Testing::TemporaryDirectory root;
    const Testing::TemporaryDirectory outside;
    writeTextFile(outside.get() / "a" / "Dup.test", "# groups:[x]\n");
    writeTextFile(outside.get() / "b" / "Dup.test", "# groups:[x]\n");

    SystestConfiguration config;
    config.testDiscoverRoot = root.get().string();
    config.testFileExtension = ".test";
    config.testDiscoverDirs.add((outside.get() / "a").string());
    config.testDiscoverDirs.add((outside.get() / "b").string());

    const auto discovered = discoverTestFiles(config);

    ASSERT_EQ(discovered.size(), 2);
    EXPECT_EQ(nameOf(discovered, outside.get() / "a" / "Dup.test"), "a/Dup");
    EXPECT_EQ(nameOf(discovered, outside.get() / "b" / "Dup.test"), "b/Dup");
}

TEST_F(TestDiscoveryTest, OverlappingSearchDirectoriesNameFromTheOutermostInAnyOrder)
{
    const Testing::TemporaryDirectory root;
    const Testing::TemporaryDirectory outside;
    writeTextFile(outside.get() / "a" / "nested" / "F.test", "# groups:[x]\n");

    SystestConfiguration nestedFirst;
    nestedFirst.testDiscoverRoot = root.get().string();
    nestedFirst.testFileExtension = ".test";
    nestedFirst.testDiscoverDirs.add((outside.get() / "a" / "nested").string());
    nestedFirst.testDiscoverDirs.add((outside.get() / "a").string());

    SystestConfiguration outerFirst;
    outerFirst.testDiscoverRoot = root.get().string();
    outerFirst.testFileExtension = ".test";
    outerFirst.testDiscoverDirs.add((outside.get() / "a").string());
    outerFirst.testDiscoverDirs.add((outside.get() / "a" / "nested").string());

    const auto nestedFirstDiscovered = discoverTestFiles(nestedFirst);
    const auto outerFirstDiscovered = discoverTestFiles(outerFirst);

    ASSERT_EQ(nestedFirstDiscovered.size(), 1);
    ASSERT_EQ(outerFirstDiscovered.size(), 1);
    EXPECT_EQ(nestedFirstDiscovered.front().name().getRawValue(), "a/nested/F");
    EXPECT_EQ(outerFirstDiscovered.front().name().getRawValue(), "a/nested/F");
}

/// Two directories outside the root can share their own name as well, and then two files would run under one name and
/// write one result file. Discovery rejects that instead of the run failing later.
TEST_F(TestDiscoveryTest, RejectsTwoFilesThatWouldShareAName)
{
    const Testing::TemporaryDirectory root;
    const Testing::TemporaryDirectory left;
    const Testing::TemporaryDirectory right;
    writeTextFile(left.get() / "a" / "Dup.test", "# groups:[x]\n");
    writeTextFile(right.get() / "a" / "Dup.test", "# groups:[x]\n");

    SystestConfiguration config;
    config.testDiscoverRoot = root.get().string();
    config.testFileExtension = ".test";
    config.testDiscoverDirs.add((left.get() / "a").string());
    config.testDiscoverDirs.add((right.get() / "a").string());

    EXPECT_THROW(discoverTestFiles(config), Exception);
}

/// A groups header that does not parse would drop the file from every group lane without a word, so discovery rejects it.
TEST_F(TestDiscoveryTest, RejectsAGroupsLineWithoutBrackets)
{
    const Testing::TemporaryDirectory tempDir;
    writeTextFile(tempDir.get() / "a.test", "# groups: Join, Aggregation\n");

    SystestConfiguration config;
    config.testDiscoverRoot = tempDir.get().string();
    config.testFileExtension = ".test";

    EXPECT_THROW(discoverTestFiles(config), Exception);
}

TEST_F(TestDiscoveryTest, RejectsAnEmptyGroupName)
{
    const Testing::TemporaryDirectory tempDir;
    writeTextFile(tempDir.get() / "a.test", "# groups: [Join,]\n");

    SystestConfiguration config;
    config.testDiscoverRoot = tempDir.get().string();
    config.testFileExtension = ".test";

    EXPECT_THROW(discoverTestFiles(config), Exception);
}

/// Several directories narrow to their union, and a file below two of them is discovered once.
TEST_F(TestDiscoveryTest, SeveralSearchDirectoriesNarrowToTheirUnionWithoutRepeating)
{
    const Testing::TemporaryDirectory tempDir;
    writeTextFile(tempDir.get() / "wanted" / "a.test", "# groups:[x]\n");
    writeTextFile(tempDir.get() / "wanted" / "nested" / "b.test", "# groups:[x]\n");
    writeTextFile(tempDir.get() / "other" / "c.test", "# groups:[x]\n");

    SystestConfiguration config;
    config.testDiscoverRoot = tempDir.get().string();
    config.testFileExtension = ".test";
    config.testDiscoverDirs.add((tempDir.get() / "wanted").string());
    config.testDiscoverDirs.add((tempDir.get() / "wanted" / "nested").string());

    const auto discovered = discoverTestFiles(config);

    ASSERT_EQ(discovered.size(), 2);
    EXPECT_EQ(nameOf(discovered, tempDir.get() / "wanted" / "a.test"), "wanted/a");
    EXPECT_EQ(nameOf(discovered, tempDir.get() / "wanted" / "nested" / "b.test"), "wanted/nested/b");
}
}
