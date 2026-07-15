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

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <SystestConfiguration.hpp>
#include <SystestState.hpp>

namespace
{
std::filesystem::path createUniqueTempDirectory()
{
    static std::atomic_uint64_t counter = 0;
    const auto uniqueId = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path()
        / ("nes-systest-state-" + std::to_string(uniqueId) + "-" + std::to_string(counter.fetch_add(1)));
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

namespace NES::Systest
{
class SystestStateTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("SystestStateTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SystestStateTest test class.");
    }

    static void TearDownTestSuite() { NES_DEBUG("Tear down SystestStateTest test class."); }
};

TEST_F(SystestStateTest, ExplicitlyIncludedGroupOverridesMatchingDisableConfigExclusion)
{
    const TemporaryDirectory tempDir;
    const auto largeFile = tempDir.get() / "large.test";
    const auto otherFile = tempDir.get() / "other.test";
    writeTextFile(largeFile, "# groups:[large]\n");
    writeTextFile(otherFile, "# groups:[other]\n");

    SystestConfiguration config;
    config.testsDiscoverDir = tempDir.get().string();
    config.testFileExtension = ".test";
    config.globalExcludedGroups = {"large"};
    config.testGroups.add("large");

    const auto testMap = loadTestFileMap(config);

    ASSERT_EQ(testMap.size(), 1);
    EXPECT_TRUE(testMap.contains(std::filesystem::weakly_canonical(largeFile)));
}

TEST_F(SystestStateTest, ExplicitCommandLineExclusionOverridesExplicitInclusion)
{
    const TemporaryDirectory tempDir;
    const auto joinFile = tempDir.get() / "join.test";
    writeTextFile(joinFile, "# groups:[Join]\n");

    SystestConfiguration config;
    config.testsDiscoverDir = tempDir.get().string();
    config.testFileExtension = ".test";
    config.testGroups.add("Join");
    config.excludeGroups.add("Join");

    const auto testMap = loadTestFileMap(config);

    EXPECT_TRUE(testMap.empty());
}

TEST_F(SystestStateTest, DirectlySpecifiedTestFileOverridesDisabledTestFiles)
{
    const TemporaryDirectory tempDir;
    const auto joinFile = tempDir.get() / "join.test";
    writeTextFile(joinFile, "# groups:[Join]\n");

    SystestConfiguration config;
    config.directlySpecifiedTestFiles = joinFile.string();
    config.disabledTestFiles.add("join.test");

    const auto testMap = loadTestFileMap(config);

    ASSERT_EQ(testMap.size(), 1);
    EXPECT_TRUE(testMap.contains(std::filesystem::weakly_canonical(joinFile)));
}

TEST_F(SystestStateTest, DiscoveredTestNamesIncludeRelativeDirectory)
{
    const TemporaryDirectory tempDir;
    const auto leftFile = tempDir.get() / "left" / "same.test";
    const auto rightFile = tempDir.get() / "right" / "same.test";
    writeTextFile(leftFile, "# groups:[Join]\n");
    writeTextFile(rightFile, "# groups:[Join]\n");

    SystestConfiguration config;
    config.testsDiscoverDir = tempDir.get().string();
    config.testFileExtension = ".test";

    const auto testMap = loadTestFileMap(config);

    ASSERT_EQ(testMap.size(), 2);
    EXPECT_EQ(testMap.at(std::filesystem::weakly_canonical(leftFile)).name(), "left/same");
    EXPECT_EQ(testMap.at(std::filesystem::weakly_canonical(rightFile)).name(), "right/same");
}

TEST_F(SystestStateTest, DirectlySpecifiedTestNamesMatchDiscoveredTestNames)
{
    const TemporaryDirectory tempDir;
    const auto testFile = tempDir.get() / "left" / "same.test";
    writeTextFile(testFile, "# groups:[Join]\n");

    SystestConfiguration config;
    config.testsDiscoverDir = tempDir.get().string();
    config.testFileExtension = ".test";

    const auto discoveredTestMap = loadTestFileMap(config);
    config.directlySpecifiedTestFiles = testFile.string();
    const auto directlySpecifiedTestMap = loadTestFileMap(config);
    config.testQueryNumbers.add(1);
    const auto queryFilteredTestMap = loadTestFileMap(config);

    const auto canonicalTestFile = std::filesystem::weakly_canonical(testFile);
    ASSERT_EQ(discoveredTestMap.size(), 1);
    ASSERT_EQ(directlySpecifiedTestMap.size(), 1);
    ASSERT_EQ(queryFilteredTestMap.size(), 1);
    const auto discoveredTestName = discoveredTestMap.at(canonicalTestFile).name();
    ASSERT_EQ(discoveredTestName, "left/same");
    EXPECT_EQ(directlySpecifiedTestMap.at(canonicalTestFile).name(), discoveredTestName);
    EXPECT_EQ(queryFilteredTestMap.at(canonicalTestFile).name(), discoveredTestName);
}

TEST_F(SystestStateTest, DirectlySpecifiedTestOutsideDiscoveryDirectoryUsesStem)
{
    const TemporaryDirectory tempDir;
    const auto discoveryDirectory = tempDir.get() / "discovery";
    const auto testFile = tempDir.get() / "outside" / "same.test";
    std::filesystem::create_directories(discoveryDirectory);
    writeTextFile(testFile, "# groups:[Join]\n");

    SystestConfiguration config;
    config.testsDiscoverDir = discoveryDirectory.string();
    config.directlySpecifiedTestFiles = testFile.string();

    const auto testMap = loadTestFileMap(config);

    ASSERT_EQ(testMap.size(), 1);
    EXPECT_EQ(testMap.at(std::filesystem::weakly_canonical(testFile)).name(), "same");
}

TEST_F(SystestStateTest, ResultFilesCreateDirectoriesForNestedTestNames)
{
    const TemporaryDirectory tempDir;

    const auto resultFile = SystestQuery::resultFile(tempDir.get(), "left/same", SystestQueryId(1));

    EXPECT_EQ(resultFile, tempDir.get() / "results" / "left" / "same_1.csv");
    EXPECT_TRUE(std::filesystem::is_directory(resultFile.parent_path()));
}
}
