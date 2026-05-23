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
#include <filesystem>
#include <fstream>
#include <string>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <SpillConfig.hpp>

namespace NES::Testing
{

class SpillConfigTest : public BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("SpillConfigTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SpillConfigTest test class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }

    /// Writes `content` to a unique temp file and returns its path. Registered for
    /// removal at teardown so tests stay self-contained. The file name combines the
    /// test suite, test name, and a per-fixture counter, which is collision-free within
    /// the test binary.
    std::filesystem::path writeTempYaml(const std::string& content)
    {
        const auto* const info = ::testing::UnitTest::GetInstance()->current_test_info();
        const auto path = std::filesystem::temp_directory_path()
            / (std::string("spill-config-") + info->test_suite_name() + "-" + info->name() + "-" + std::to_string(tempFileCounter++)
               + ".yaml");
        std::ofstream out(path);
        EXPECT_TRUE(out.is_open()) << "failed to open temp YAML file: " << path;
        out << content;
        out.close();
        EXPECT_TRUE(out.good()) << "failed to write temp YAML file: " << path;
        tempFiles.push_back(path);
        return path;
    }

    void TearDown() override
    {
        for (const auto& path : tempFiles)
        {
            std::error_code ec;
            std::filesystem::remove(path, ec);
        }
        BaseUnitTest::TearDown();
    }

private:
    std::vector<std::filesystem::path> tempFiles;
    int tempFileCounter = 0;
};

/// Test A — defaults match the documented Phase 1 contract.
TEST_F(SpillConfigTest, defaults)
{
    const SpillConfig config;
    EXPECT_FALSE(config.enabled.getValue());
    EXPECT_EQ(config.rocksdbPath.getValue(), "/tmp/nes-spill");
    EXPECT_EQ(config.softThresholdMB.getValue(), 256U);
    EXPECT_EQ(config.hardThresholdMB.getValue(), 384U);
    EXPECT_EQ(config.compression.getValue(), "lz4");
    /// Increment C: emit lag defaults to 0 (no retention ⇒ byte-identical to pre-Increment-C behavior).
    EXPECT_EQ(config.emitLag.getValue(), 0U);
}

/// Test B — command-line overrides flow through every field.
TEST_F(SpillConfigTest, commandLineOverride)
{
    SpillConfig config;
    config.overwriteConfigWithCommandLineInput(
        {{"enabled", "true"},
         {"rocksdb_path", "/data/spill"},
         {"soft_threshold_mb", "100"},
         {"compression", "zstd"},
         {"emit_lag", "5000"}});

    EXPECT_TRUE(config.enabled.getValue());
    EXPECT_EQ(config.rocksdbPath.getValue(), "/data/spill");
    EXPECT_EQ(config.softThresholdMB.getValue(), 100U);
    EXPECT_EQ(config.compression.getValue(), "zstd");
    EXPECT_EQ(config.emitLag.getValue(), 5000U);
    /// Untouched field keeps its default.
    EXPECT_EQ(config.hardThresholdMB.getValue(), 384U);
}

/// Test C — YAML file round-trip restores every field.
TEST_F(SpillConfigTest, yamlRoundTrip)
{
    const auto yamlPath = writeTempYaml(
        "enabled: \"true\"\n"
        "rocksdb_path: \"/var/nes/spill\"\n"
        "soft_threshold_mb: \"128\"\n"
        "hard_threshold_mb: \"256\"\n"
        "compression: \"none\"\n"
        "emit_lag: \"5000\"\n");

    SpillConfig config;
    config.overwriteConfigWithYAMLFileInput(yamlPath.string());

    EXPECT_TRUE(config.enabled.getValue());
    EXPECT_EQ(config.rocksdbPath.getValue(), "/var/nes/spill");
    EXPECT_EQ(config.softThresholdMB.getValue(), 128U);
    EXPECT_EQ(config.hardThresholdMB.getValue(), 256U);
    EXPECT_EQ(config.compression.getValue(), "none");
    EXPECT_EQ(config.emitLag.getValue(), 5000U);
}

/// Test D — nested embedding in QueryExecutionConfiguration. This is the binding the
/// lowering guard actually reads (conf.spill.enabled), so it is the load-bearing test.
TEST_F(SpillConfigTest, embeddedDefaultsInQueryExecutionConfiguration)
{
    const QueryExecutionConfiguration qec;
    EXPECT_FALSE(qec.spill.enabled.getValue());
    EXPECT_EQ(qec.spill.rocksdbPath.getValue(), "/tmp/nes-spill");
    EXPECT_EQ(qec.spill.emitLag.getValue(), 0U);
}

/// Test E — nested command-line override reaches the embedded spill sub-config.
TEST_F(SpillConfigTest, embeddedCommandLineOverride)
{
    QueryExecutionConfiguration qec;
    qec.overwriteConfigWithCommandLineInput(
        {{"spill.enabled", "true"}, {"spill.soft_threshold_mb", "64"}, {"spill.emit_lag", "2000"}});

    EXPECT_TRUE(qec.spill.enabled.getValue());
    EXPECT_EQ(qec.spill.softThresholdMB.getValue(), 64U);
    /// Increment C: the auto-derived dotted key spill.emit_lag reaches the embedded sub-config.
    EXPECT_EQ(qec.spill.emitLag.getValue(), 2000U);
}

/// Test F — nested YAML override reaches the embedded spill sub-config.
TEST_F(SpillConfigTest, embeddedYamlOverride)
{
    const auto yamlPath = writeTempYaml(
        "spill:\n"
        "  enabled: \"true\"\n"
        "  compression: \"zstd\"\n"
        "  emit_lag: \"2000\"\n");

    QueryExecutionConfiguration qec;
    qec.overwriteConfigWithYAMLFileInput(yamlPath.string());

    EXPECT_TRUE(qec.spill.enabled.getValue());
    EXPECT_EQ(qec.spill.compression.getValue(), "zstd");
    EXPECT_EQ(qec.spill.emitLag.getValue(), 2000U);
}

}
