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
#include <ErrorHandling.hpp>
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

TEST_F(SystestStateTest, TestWithUnsatisfiedRequiresIsSkippedDuringDiscovery)
{
    const TemporaryDirectory tempDir;
    const auto plainFile = tempDir.get() / "plain.test";
    const auto externalFile = tempDir.get() / "external.test";
    writeTextFile(plainFile, "# groups: [misc]\n");
    writeTextFile(externalFile, "# requires: mqtt\n");

    SystestConfiguration config;
    config.testsDiscoverDir = tempDir.get().string();
    config.testFileExtension = ".test";

    const auto testMap = loadTestFileMap(config);

    ASSERT_EQ(testMap.size(), 1);
    EXPECT_TRUE(testMap.contains(std::filesystem::weakly_canonical(plainFile)));
}

TEST_F(SystestStateTest, AcceptRequiresKeepsMatchingTestInDiscovery)
{
    const TemporaryDirectory tempDir;
    const auto externalFile = tempDir.get() / "external.test";
    writeTextFile(externalFile, "# requires: mqtt\n");

    SystestConfiguration config;
    config.testsDiscoverDir = tempDir.get().string();
    config.testFileExtension = ".test";
    config.acceptRequires.add("mqtt");

    const auto testMap = loadTestFileMap(config);

    ASSERT_EQ(testMap.size(), 1);
    EXPECT_TRUE(testMap.contains(std::filesystem::weakly_canonical(externalFile)));
}

TEST_F(SystestStateTest, AcceptRequiresIsCaseInsensitive)
{
    const TemporaryDirectory tempDir;
    const auto externalFile = tempDir.get() / "external.test";
    writeTextFile(externalFile, "# requires: MQTT\n");

    SystestConfiguration config;
    config.testsDiscoverDir = tempDir.get().string();
    config.testFileExtension = ".test";
    config.acceptRequires.add("mqtt");

    const auto testMap = loadTestFileMap(config);

    ASSERT_EQ(testMap.size(), 1);
}

TEST_F(SystestStateTest, RejectsMultipleRequiresDirectives)
{
    const TemporaryDirectory tempDir;
    const auto multiFile = tempDir.get() / "multi.test";
    writeTextFile(multiFile, "# requires: mqtt\n# requires: postgres\n");

    SystestConfiguration config;
    config.testsDiscoverDir = tempDir.get().string();
    config.testFileExtension = ".test";
    config.acceptRequires.add("mqtt");

    EXPECT_THROW(loadTestFileMap(config), NES::Exception);
}

TEST_F(SystestStateTest, RequiresHeaderRecognisedAmongOtherDirectives)
{
    const TemporaryDirectory tempDir;
    const auto testFile = tempDir.get() / "mixed.test";
    writeTextFile(
        testFile,
        "# name: mixed\n"
        "# description: example with both groups and requires\n"
        "# groups: [Misc]\n"
        "# requires: mqtt\n");

    SystestConfiguration config;
    config.testsDiscoverDir = tempDir.get().string();
    config.testFileExtension = ".test";
    config.acceptRequires.add("mqtt");

    const auto testMap = loadTestFileMap(config);

    ASSERT_EQ(testMap.size(), 1);
    const auto& parsed = testMap.at(std::filesystem::weakly_canonical(testFile));
    EXPECT_EQ(parsed.groups, std::vector<std::string>{"Misc"});
    ASSERT_TRUE(parsed.requirement.has_value());
    EXPECT_EQ(*parsed.requirement, "mqtt"); /// NOLINT(bugprone-unchecked-optional-access): guarded by the ASSERT_TRUE on the previous line.
}
}
