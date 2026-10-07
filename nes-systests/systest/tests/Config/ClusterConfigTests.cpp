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
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <Config/Config.hpp>
#include <Config/ConfigParser.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>

namespace NES
{

class ClusterConfigTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("ClusterConfigTest.log", LogLevel::LOG_DEBUG); }

    /// One file per test, because the death tests run as parallel processes.
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
