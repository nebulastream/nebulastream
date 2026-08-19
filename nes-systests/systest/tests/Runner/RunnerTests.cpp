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
#include <optional>
#include <string>
#include <tuple>
#include <variant>

#include <gtest/gtest.h>

#include <Model/RunnableTest.hpp>
#include <Model/Verdict.hpp>
#include <Parser/SystestParser.hpp>
#include <Parser/TestFileParser.hpp>
#include <Rewriter/SqlRewriter.hpp>
#include <Runner/TestRunner.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

class RunnerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("Runner.log", LogLevel::LOG_DEBUG); }

    /// The mismatch text of a failed case, and empty text otherwise, so an assertion can print why it failed.
    static std::string detailOf(const CheckedQuery& query)
    {
        const auto* mismatch = std::get_if<Mismatch>(&query.outcome);
        return mismatch != nullptr ? mismatch->detail : std::string{};
    }
};

/// The whole pipeline against a real coordinator: read a test file, rewrite it, run it on an embedded worker, and check the result.
TEST_F(RunnerTest, RunsOneTupleEndToEnd)
{
    const std::filesystem::path workingDir = "/tmp/systest_runner_test";
    std::filesystem::remove_all(workingDir);
    std::filesystem::create_directories(workingDir);

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

    const auto testFile = parseTestFile(parser, "OneTuple.test");

    TestRunner runner;
    SqlRewriter rewriter{RewriteTarget{
        .testFileKey = "ONETUPLE",
        .displayName = "OneTuple",
        .workingDir = workingDir,
        .testDataDir = "/data",
        .sourceHost = runner.placementFor({})->sources,
        .sinkHost = runner.placementFor({})->sinks}};
    const std::vector runnables{rewriter.rewrite(testFile)};
    const auto checked = runner.runAll(runnables, 1);

    ASSERT_EQ(checked.size(), 1U);
    EXPECT_EQ(checked.at(0).id.file, "OneTuple");
    EXPECT_TRUE(std::holds_alternative<Passed>(checked.at(0).outcome)) << detailOf(checked.at(0));
}

/// Two test files run at once and both are checked, which is the whole point of a pool.
/// The checks come back in test file order whichever query finishes first.
TEST_F(RunnerTest, RunsTwoTestFilesAtOnce)
{
    const std::filesystem::path workingDir = "/tmp/systest_runner_concurrent_test";
    std::filesystem::remove_all(workingDir);
    std::filesystem::create_directories(workingDir);

    TestRunner runner;
    const auto rewriteOneTuple = [&](const std::string& key)
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
        SqlRewriter rewriter{RewriteTarget{
            .testFileKey = key,
            .displayName = key,
            .workingDir = workingDir,
            .testDataDir = "/data",
            .sourceHost = runner.placementFor({})->sources,
            .sinkHost = runner.placementFor({})->sinks}};
        return rewriter.rewrite(parseTestFile(parser, key + ".test"));
    };

    const std::vector runnables{rewriteOneTuple("FIRST"), rewriteOneTuple("SECOND")};
    const auto checked = runner.runAll(runnables, 2);

    ASSERT_EQ(checked.size(), 2U);
    EXPECT_EQ(checked.at(0).id.file, "FIRST");
    EXPECT_EQ(checked.at(1).id.file, "SECOND");
    EXPECT_TRUE(std::holds_alternative<Passed>(checked.at(0).outcome)) << detailOf(checked.at(0));
    EXPECT_TRUE(std::holds_alternative<Passed>(checked.at(1).outcome)) << detailOf(checked.at(1));
}

/// No test expects its setup to fail, so a rejected setup statement fails the whole test file.
/// The runner reports it under that file's name rather than throwing, so the other files still run.
TEST_F(RunnerTest, ReportsASetupStatementTheCoordinatorRejects)
{
    RunnableTest runnable;
    runnable.name = "Rejected.test";
    runnable.createStmts.push_back(
        RunnableCreateStatement{.sql = "CREATE PHYSICAL SOURCE FOR sourceThatWasNeverDeclared TYPE File", .staged = std::nullopt});

    TestRunner runner;
    const std::vector runnables{std::move(runnable)};
    const auto checked = runner.runAll(runnables, 1);

    ASSERT_EQ(checked.size(), 1U);
    EXPECT_EQ(checked.at(0).id.file, "Rejected.test");
    EXPECT_TRUE(std::holds_alternative<Mismatch>(checked.at(0).outcome));
}

}
