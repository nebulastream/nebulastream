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

#include <filesystem>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

#include <Config/Config.hpp>
#include <Discovery/TestDiscovery.hpp>
#include <Model/RunnablePartition.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Model/Verdict.hpp>
#include <Parser/SystestParser.hpp>
#include <Parser/TestFileBuilder.hpp>
#include <Rewriter/RewriteContext.hpp>
#include <Rewriter/TestFileRewriter.hpp>
#include <Runner/TestRunner.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
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

    /// A run over an embedded coordinator with the one worker it starts for itself, keeping its files under the given directory.
    static SystestConfiguration configFor(const std::filesystem::path& workingDir)
    {
        SystestConfiguration config;
        config.workingDir.setValue(workingDir.string());
        return config;
    }

    /// A test file with one source row and one query reading it, rewritten under the given name for the runner's worker.
    static RunnableTestFile rewriteOneTuple(TestRunner& runner, const std::filesystem::path& workingDir, const std::string& name)
    {
        SystestParser parser;
        parser.loadString("CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
                          "CREATE PHYSICAL SOURCE FOR oneTuple TYPE File;\n"
                          "ATTACH INLINE\n"
                          "1\n"
                          "\n"
                          "CREATE SINK sinkOneTuple(field_1 UINT64 NOT NULL) TYPE File;\n"
                          "\n"
                          "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                          "----\n"
                          "1\n");
        const auto placement = runner.placementFor({});
        const auto testFile = workingDir / (name + ".test");
        const auto key = DiscoveryRoot{workingDir}.keyOf(testFile, 0, 1);
        return TestFileRewriter::rewritePartition(
            buildTestFile(parser, testFile),
            RewriteContext{
                .testFileKey = key,
                .name = name,
                .workingDir = workingDir,
                .testDataDir = "/data",
                .sourceHost = placement->sources,
                .sinkHost = placement->sinks});
    }

    /// No test file below asks for worker settings of its own.
    static std::vector<RunnablePartition> withoutSettings(std::vector<RunnableTestFile> runnables)
    {
        std::vector<RunnablePartition> partitions;
        partitions.reserve(runnables.size());
        for (auto& runnable : runnables)
        {
            partitions.push_back(RunnablePartition{.overrides = {}, .test = std::move(runnable)});
        }
        return partitions;
    }
};

/// The whole pipeline against a real coordinator: rewrite a test file, run it on an embedded worker, and check the result.
TEST_F(RunnerTest, RunsOneTupleEndToEnd)
{
    const Testing::TemporaryDirectory workingDir;
    TestRunner runner{configFor(workingDir.get())};
    const auto partitions = withoutSettings({rewriteOneTuple(runner, workingDir.get(), "OneTuple")});
    const auto checked = runner.runAll(partitions, 1);

    ASSERT_EQ(checked.size(), 1U);
    EXPECT_EQ(checked.at(0).id.originFile, "OneTuple");
    EXPECT_TRUE(hasPassed(checked.at(0).outcome)) << detailOf(checked.at(0));
}

/// Two test files run at once and both are checked, which is the whole point of a pool.
/// The checks come back in test file order whichever query finishes first.
TEST_F(RunnerTest, RunsTwoTestFilesAtOnce)
{
    const Testing::TemporaryDirectory workingDir;
    TestRunner runner{configFor(workingDir.get())};
    const auto partitions
        = withoutSettings({rewriteOneTuple(runner, workingDir.get(), "First"), rewriteOneTuple(runner, workingDir.get(), "Second")});
    const auto checked = runner.runAll(partitions, 2);

    ASSERT_EQ(checked.size(), 2U);
    EXPECT_EQ(checked.at(0).id.originFile, "First");
    EXPECT_EQ(checked.at(1).id.originFile, "Second");
    EXPECT_TRUE(hasPassed(checked.at(0).outcome)) << detailOf(checked.at(0));
    EXPECT_TRUE(hasPassed(checked.at(1).outcome)) << detailOf(checked.at(1));
}

/// No test expects its setup to fail, so a rejected setup statement fails the whole test file.
/// The runner reports it under that file's name rather than throwing, so the other files still run.
TEST_F(RunnerTest, ReportsASetupStatementTheCoordinatorRejects)
{
    const Testing::TemporaryDirectory workingDir;
    RunnableTestFile runnable;
    runnable.name = "Rejected";
    runnable.setupStatements.emplace_back(PlainStatement{.sql = "CREATE PHYSICAL SOURCE FOR sourceThatWasNeverDeclared TYPE File"});

    TestRunner runner{configFor(workingDir.get())};
    const auto partitions = withoutSettings({std::move(runnable)});
    const auto checked = runner.runAll(partitions, 1);

    ASSERT_EQ(checked.size(), 1U);
    EXPECT_EQ(checked.at(0).id.originFile, "Rejected");
    EXPECT_FALSE(hasPassed(checked.at(0).outcome));
    EXPECT_TRUE(std::holds_alternative<Verdict>(checked.at(0).outcome));
}

}
