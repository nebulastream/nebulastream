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

#include <chrono>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include <Config/Topology.hpp>
#include <gtest/gtest.h>
#include <nes-coordinator-bridge/coordinator.h>
#include <rust/cxx.h>

#include <Config/Config.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Model/Expectation.hpp>
#include <Model/RunnablePartition.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Model/Verdict.hpp>
#include <Parser/SystestParser.hpp>
#include <Parser/TestFileBuilder.hpp>
#include <Rewriter/NamePrefixer.hpp>
#include <Rewriter/RewriteContext.hpp>
#include <Rewriter/TestFilePreparer.hpp>
#include <Runner/Cluster.hpp>
#include <Runner/TestRunner.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <SingleWorkerTopology.hpp>
#include <TemporaryDirectory.hpp>

namespace NES
{

class RunnerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("Runner.log", LogLevel::LOG_DEBUG); }

    /// The mismatch text of a failed test case, and empty text otherwise, so an assertion can print why it failed.
    static std::string detailOf(const ReportEntry& entry)
    {
        const auto* verdict = std::get_if<Verdict>(&entry.outcome);
        return verdict != nullptr and not verdict->has_value() ? verdict->error().detail : std::string{};
    }

    static Cluster createCluster()
    {
        return Cluster{Cluster::Config{
            .mode = Bridge::WorkerMode::Embedded, .topology = Testing::singleWorkerTopology(), .optimizerConfig = {}, .workerConfig = {}}};
    }

    /// A run that keeps its files under the given directory.
    static SystestConfiguration configFor(const std::filesystem::path& workingDir)
    {
        SystestConfiguration config;
        config.workingDir = workingDir.string();
        return config;
    }

    /// A test file with one source row and one query reading it, rewritten under the given name for the runner's worker.
    static RunnableTestFile rewriteOneTuple(Cluster& cluster, const std::filesystem::path& workingDir, const std::string& name)
    {
        SystestParser parser;
        parser.loadString(
            "CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
            "CREATE PHYSICAL SOURCE FOR oneTuple TYPE File;\n"
            "ATTACH INLINE\n"
            "1\n"
            "\n"
            "CREATE SINK sinkOneTuple(field_1 UINT64 NOT NULL) TYPE File;\n"
            "\n"
            "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
            "----\n"
            "1\n");
        const auto placement = cluster.placementFor({});
        const auto testFile = workingDir / (name + ".test");
        const auto key = TestFileKeyFactory{workingDir}.deriveKeyOf(testFile, 0, 1);
        return TestFilePreparer::rewritePartition(
            buildTestFile(parser, testFile),
            RewriteContext{
                .testFileKey = key,
                .name = name,
                .workingDir = workingDir,
                .testDataDir = "/data",
                .sourceHost = placement.sources,
                .sinkHost = placement.sinks});
    }

    /// No test file below has configuration overrides.
    static std::vector<RunnablePartition> withoutSettings(std::vector<RunnableTestFile> runnables)
    {
        std::vector<RunnablePartition> partitions;
        partitions.reserve(runnables.size());
        for (auto& runnable : runnables)
        {
            partitions.push_back(RunnablePartition{.overrides = {}, .file = std::move(runnable)});
        }
        return partitions;
    }
};

/// The whole pipeline against a real coordinator: rewrite a test file, run it on an embedded worker, and check the result.
TEST_F(RunnerTest, RunsOneTupleEndToEnd)
{
    const Testing::TemporaryDirectory workingDir;
    auto cluster = createCluster();
    TestRunner runner{configFor(workingDir.get()), cluster, withoutSettings({rewriteOneTuple(cluster, workingDir.get(), "OneTuple")})};
    const auto checked = runner.submitAll(1);

    ASSERT_EQ(checked.size(), 1U);
    EXPECT_EQ(checked.at(0).id.originFile, "OneTuple");
    EXPECT_TRUE(hasPassed(checked.at(0).outcome)) << detailOf(checked.at(0));
}

/// Two test files run at once and both are checked.
/// The checks come back in test file order whichever query finishes first.
TEST_F(RunnerTest, RunsTwoTestFilesAtOnce)
{
    const Testing::TemporaryDirectory workingDir;
    auto cluster = createCluster();
    TestRunner runner{
        configFor(workingDir.get()),
        cluster,
        withoutSettings({rewriteOneTuple(cluster, workingDir.get(), "First"), rewriteOneTuple(cluster, workingDir.get(), "Second")})};
    const auto checked = runner.submitAll(2);

    ASSERT_EQ(checked.size(), 2U);
    EXPECT_EQ(checked.at(0).id.originFile, "First");
    EXPECT_EQ(checked.at(1).id.originFile, "Second");
    EXPECT_TRUE(hasPassed(checked.at(0).outcome)) << detailOf(checked.at(0));
    EXPECT_TRUE(hasPassed(checked.at(1).outcome)) << detailOf(checked.at(1));
}

/// No test expects its setup to fail, so a rejected setup statement fails the whole test file.
/// The runner reports it under that file's name and does not throw, so the other files still run.
TEST_F(RunnerTest, ReportsASetupStatementTheCoordinatorRejects)
{
    const Testing::TemporaryDirectory workingDir;
    RunnableTestFile runnable;
    runnable.name = "Rejected";
    runnable.setupStatements.emplace_back(PlainStatement{.sql = "CREATE PHYSICAL SOURCE FOR sourceThatWasNeverDeclared TYPE File"});

    auto cluster = createCluster();
    TestRunner runner{configFor(workingDir.get()), cluster, withoutSettings({std::move(runnable)})};
    const auto& rejected = runner.getRejected();

    ASSERT_EQ(rejected.size(), 1U);
    EXPECT_EQ(rejected.at(0).id.originFile, "Rejected");
    EXPECT_FALSE(hasPassed(rejected.at(0).outcome));
    EXPECT_TRUE(std::holds_alternative<Verdict>(rejected.at(0).outcome));
    EXPECT_TRUE(runner.submitAll(1).empty());
}

/// A query past the timeout would keep running on the workers of the later test cases, so the runner stops it.
TEST_F(RunnerTest, StopsAQueryThatRunsPastTheTimeout)
{
    const Testing::TemporaryDirectory workingDir;
    RunnableTestFile runnable;
    runnable.name = "Endless";
    runnable.setupStatements.emplace_back(PlainStatement{.sql = "CREATE LOGICAL SOURCE endless(id UINT64 NOT NULL)"});
    runnable.setupStatements.emplace_back(PlainStatement{.sql = R"(CREATE PHYSICAL SOURCE FOR endless TYPE Generator SET(
                'NONE' AS "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
                1 AS "SOURCE".SEED,
                'FIXED' AS "SOURCE".GENERATOR_RATE_TYPE,
                'emit_rate 10' AS "SOURCE".GENERATOR_RATE_CONFIG,
                'SEQUENCE UINT64 0 10 1' AS "SOURCE".GENERATOR_SCHEMA,
                'CSV' AS INPUT_FORMATTER."TYPE"))"});
    runnable.testCases.push_back(
        RewrittenTestCase{
            .action = RewrittenQuery{
                .sql = "SELECT id FROM endless INTO Void()",
                .id = SystestQueryId{1},
                .resultFile = std::nullopt,
                .declaredSinkSchema = std::nullopt,
                .inputFiles = {},
                .expected = ExpectedRows{}}});

    auto cluster = createCluster();
    auto config = configFor(workingDir.get());
    config.queryTimeoutSeconds = 1;
    TestRunner runner{config, cluster, withoutSettings({std::move(runnable)})};
    ASSERT_TRUE(runner.getRejected().empty()) << detailOf(runner.getRejected().front());
    const auto checked = runner.submitAll(1);

    ASSERT_EQ(checked.size(), 1U);
    ASSERT_TRUE(detailOf(checked.at(0)).contains("timed out")) << detailOf(checked.at(0));

    /// The stop is requested when the run returns and takes effect a moment later.
    const auto showQueries = [&cluster]
    {
        constexpr std::string_view sql = "SHOW QUERIES";
        return std::string{cluster.getCoordinator().submit(rust::Str{sql.data(), sql.size()}, Bridge::WaitMode::None, 0, true).result};
    };
    constexpr std::chrono::milliseconds pollInterval{100};
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{30};
    auto queries = showQueries();
    while (not queries.contains("Stopped") and std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(pollInterval);
        queries = showQueries();
    }
    EXPECT_TRUE(queries.contains("Stopped")) << queries;
}

/// A worker takes its settings at startup, so a test file with settings of its own gets a copy of the topology's workers.
TEST_F(RunnerTest, RegistersACopyOfTheTopologyForEachSetOfSettings)
{
    const Host source{"source-node:8080"};
    const Host sink{"sink-node:8080"};
    Cluster
        cluster{Cluster::
                    Config{
                        .mode = Bridge::WorkerMode::Embedded,
                        .topology = ClusterConfiguration{.workers = {TopologyWorker{.host = sink, .dataAddress = "sink-node:9090", .maxOperators = std::nullopt, .downstream = {}, .config = {}}, TopologyWorker{.host = source, .dataAddress = "source-node:9090", .maxOperators = std::nullopt, .downstream = {sink}, .config = {}}}, .allowSourcePlacement = {source}, .allowSinkPlacement = {sink}},
                        .optimizerConfig = {},
                        .workerConfig = {}}};

    const auto unchanged = cluster.placementFor({});
    EXPECT_EQ(unchanged.sources, source);
    EXPECT_EQ(unchanged.sinks, sink);

    const ConfigurationOverride settings{{"worker.query_engine.number_of_worker_threads", "1"}};
    const auto copy = cluster.placementFor(settings);
    EXPECT_EQ(copy.sources, Host{"source-node-1:8080"});
    EXPECT_EQ(copy.sinks, Host{"sink-node-1:8080"});
    EXPECT_EQ(cluster.placementFor(settings).sources, copy.sources);
}

}
