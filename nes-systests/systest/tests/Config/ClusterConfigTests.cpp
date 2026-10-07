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
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>

#include <cstdlib>
#include <Config/Config.hpp>
#include <Config/ConfigParser.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{

class ClusterConfigTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("ClusterConfigTest.log", LogLevel::LOG_DEBUG); }

    /// Writes a topology with the given placement lists and parses a command line that loads it.
    /// The file is named after the test, because the death tests run as separate processes at the same time.
    static SystestConfiguration parseWithPlacement(const std::string_view sourcePlacement, const std::string_view sinkPlacement)
    {
        const auto path = std::filesystem::temp_directory_path()
            / fmt::format("systest-cluster-config-{}.yaml", testing::UnitTest::GetInstance()->current_test_info()->name());
        {
            std::ofstream file{path};
            file << "workers:\n  - host: localhost:8080\n    data_address: localhost:9090\n";
            file << "allow_source_placement: " << sourcePlacement << "\n";
            file << "allow_sink_placement: " << sinkPlacement << "\n";
        }
        const auto pathArgument = path.string();
        std::array<const char*, 3> argv{"systest", "--clusterConfig", pathArgument.c_str()};
        return parseConfig(static_cast<int>(argv.size()), argv.data());
    }
};

TEST_F(ClusterConfigTest, AcceptsATopologyWithADefaultHostForSourcesAndSinks)
{
    const auto config = parseWithPlacement("[localhost:8080]", "[localhost:8080]");
    ASSERT_EQ(config.clusterConfig.allowSourcePlacement.size(), 1U);
    ASSERT_EQ(config.clusterConfig.allowSinkPlacement.size(), 1U);
}

/// The first entry of a placement list is the default host of a source or sink that names none, so an empty list is rejected
/// where the file is read, before any test file is loaded.
TEST_F(ClusterConfigTest, RejectsAnEmptySourcePlacement)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(parseWithPlacement("[]", "[localhost:8080]"), testing::ExitedWithCode(EXIT_FAILURE), "allow_source_placement");
}

TEST_F(ClusterConfigTest, RejectsAnEmptySinkPlacement)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(parseWithPlacement("[localhost:8080]", "[]"), testing::ExitedWithCode(EXIT_FAILURE), "allow_sink_placement");
}

}
