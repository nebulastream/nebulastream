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
#include <iterator>
#include <optional>
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
    static SystestConfiguration parseTopology(const std::string_view topology)
    {
        const auto path = std::filesystem::temp_directory_path()
            / fmt::format("systest-cluster-config-{}.yaml", testing::UnitTest::GetInstance()->current_test_info()->name());
        {
            std::ofstream file{path};
            file << topology;
        }
        const auto pathArgument = path.string();
        std::array<const char*, 3> argv{"systest", "--clusterConfig", pathArgument.c_str()};
        return parseConfig(static_cast<int>(argv.size()), argv.data());
    }

    static SystestConfiguration parseWithPlacement(const std::string_view sourcePlacement, const std::string_view sinkPlacement)
    {
        return parseTopology(
            fmt::format(
                "workers:\n  - host: localhost:8080\n    data_address: localhost:9090\n"
                "allow_source_placement: {}\nallow_sink_placement: {}\n",
                sourcePlacement,
                sinkPlacement));
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

TEST_F(ClusterConfigTest, ReadsEveryKeyOfAWorker)
{
    const auto config = parseTopology(
        "workers:\n"
        "  - host: sink:8080\n"
        "    data_address: sink:9090\n"
        "  - host: source:8080\n"
        "    data_address: source:9090\n"
        "    max_operators: 2\n"
        "    downstream: [sink:8080]\n"
        "    config:\n"
        "      worker:\n"
        "        query_engine:\n"
        "          number_of_worker_threads: 1\n"
        "allow_source_placement: [source:8080]\n"
        "allow_sink_placement: [sink:8080]\n");
    ASSERT_EQ(config.clusterConfig.workers.size(), 2U);
    const auto& sink = config.clusterConfig.workers.at(0);
    EXPECT_EQ(sink.maxOperators, std::nullopt);
    EXPECT_TRUE(sink.downstream.empty());
    const auto& source = config.clusterConfig.workers.at(1);
    EXPECT_EQ(source.host, Host{"source:8080"});
    EXPECT_EQ(source.dataAddress, "source:9090");
    EXPECT_EQ(source.maxOperators, 2U);
    EXPECT_EQ(source.downstream, std::vector{Host{"sink:8080"}});
    EXPECT_EQ(std::ranges::distance(source.config), 1);
}

TEST_F(ClusterConfigTest, RejectsAWorkerWithoutADataAddress)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(
        parseTopology("workers:\n  - host: a:8080\nallow_source_placement: [a:8080]\nallow_sink_placement: [a:8080]\n"),
        testing::ExitedWithCode(EXIT_FAILURE),
        "data_address");
}

TEST_F(ClusterConfigTest, RejectsATopologyWithoutWorkers)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(
        parseTopology("allow_source_placement: [a:8080]\nallow_sink_placement: [a:8080]\n"),
        testing::ExitedWithCode(EXIT_FAILURE),
        "workers");
}

TEST_F(ClusterConfigTest, RejectsAnUnknownKey)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(
        parseTopology(
            "workers:\n  - host: a:8080\n    data_address: a:9090\n    max_operator: 2\n"
            "allow_source_placement: [a:8080]\nallow_sink_placement: [a:8080]\n"),
        testing::ExitedWithCode(EXIT_FAILURE),
        "max_operator");
}

TEST_F(ClusterConfigTest, RejectsAPlacementOnAnUndeclaredWorker)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(parseWithPlacement("[other:8080]", "[localhost:8080]"), testing::ExitedWithCode(EXIT_FAILURE), "other:8080");
}

}
