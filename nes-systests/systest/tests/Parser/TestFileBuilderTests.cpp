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

#include <cstddef>
#include <filesystem>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

#include <Identifiers/Identifiers.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Model/Expectation.hpp>
#include <Model/ParsedTestFile.hpp>
#include <Parser/SystestParser.hpp>
#include <Parser/TestFileBuilder.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

class TestFileParserTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("TestFileBuilder.log", LogLevel::LOG_DEBUG); }

    static ParsedTestFile readSlt(const std::string& slt)
    {
        SystestParser parser;
        parser.loadString(slt);
        return buildTestFile(parser, "/tests/OneTuple.test");
    }
};

/// A configuration line applies to the queries below it, not to the whole file.
/// TupleLargerThanBuffer relies on this: it sets one buffer size, checks that a tuple does not fit, then sets a larger
/// one and checks that it does.
/// Reading the file as a whole would give both queries the same setting.
TEST_F(TestFileParserTest, ReadsAConfigurationAsApplyingToTheQueriesBelowIt)
{
    const auto [path, statements] = readSlt(
        "GlobalConfiguration worker.default_query_execution.operator_buffer_size: [1567]\n"
        "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
        "----\n"
        "1\n"
        "\n"
        "GlobalConfiguration worker.default_query_execution.operator_buffer_size: [1568]\n"
        "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
        "----\n"
        "1\n");

    ASSERT_EQ(statements.size(), 2U);
    const auto* first = std::get_if<SelectStatement>(&statements.at(0));
    const auto* second = std::get_if<SelectStatement>(&statements.at(1));
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(first->overrides.at("worker.default_query_execution.operator_buffer_size"), "1567");
    EXPECT_EQ(second->overrides.at("worker.default_query_execution.operator_buffer_size"), "1568");
}

/// A global line holds for what follows it, so a query below one inherits it without restating it.
TEST_F(TestFileParserTest, CarriesAGlobalConfigurationToEveryQueryBelowIt)
{
    const auto [path, statements] = readSlt(
        "GlobalConfiguration worker.query_engine.number_of_worker_threads: [1]\n"
        "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
        "----\n"
        "1\n"
        "\n"
        "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
        "----\n"
        "1\n");

    ASSERT_EQ(statements.size(), 2U);
    for (const auto& statement : statements)
    {
        const auto* query = std::get_if<SelectStatement>(&statement);
        ASSERT_NE(query, nullptr);
        EXPECT_EQ(query->overrides.at("worker.query_engine.number_of_worker_threads"), "1");
    }
}

/// A configuration line may list values rather than one, and each is a setting the query runs under.
/// JoinNull relies on this: it asserts the join returns the same rows with the bloom filter on and off.
TEST_F(TestFileParserTest, RunsAQueryOncePerListedAlternative)
{
    const auto [path, statements] = readSlt(
        "GlobalConfiguration worker.default_query_execution.bloom_filter.enable_bloom_filter: [true, false]\n"
        "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
        "----\n"
        "1\n");

    ASSERT_EQ(statements.size(), 2U);
    const auto* first = std::get_if<SelectStatement>(&statements.at(0));
    const auto* second = std::get_if<SelectStatement>(&statements.at(1));
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);

    static constexpr auto Key = "worker.default_query_execution.bloom_filter.enable_bloom_filter";
    EXPECT_EQ(first->overrides.at(Key), "true");
    EXPECT_EQ(second->overrides.at(Key), "false");
    /// The alternatives are the same query asserting the same answer, so everything but the overrides agrees.
    EXPECT_EQ(first->sql, second->sql);
    EXPECT_EQ(first->id, second->id);
    EXPECT_EQ(std::get<ExpectedRows>(first->expected).rows, std::get<ExpectedRows>(second->expected).rows);
}

/// Two lines that each list values combine into every pairing of them, so the query runs once per pairing.
TEST_F(TestFileParserTest, CombinesTwoListedConfigurationsIntoEveryPairing)
{
    const auto [path, statements] = readSlt(
        "GlobalConfiguration worker.query_engine.number_of_worker_threads: [1, 2]\n"
        "Configuration worker.default_query_execution.operator_buffer_size: [1567, 1568]\n"
        "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
        "----\n"
        "1\n");

    ASSERT_EQ(statements.size(), 4U);
    std::set<std::pair<std::string, std::string>> pairings;
    for (const auto& statement : statements)
    {
        const auto* query = std::get_if<SelectStatement>(&statement);
        ASSERT_NE(query, nullptr);
        pairings.emplace(
            query->overrides.at("worker.query_engine.number_of_worker_threads"),
            query->overrides.at("worker.default_query_execution.operator_buffer_size"));
    }
    const std::set<std::pair<std::string, std::string>> expected{{"1", "1567"}, {"1", "1568"}, {"2", "1567"}, {"2", "1568"}};
    EXPECT_EQ(pairings, expected);
}

/// A later global line replaces the key of an earlier one in every pairing, so pairings can end up equal.
/// Equal pairings would run the same query twice with the same configuration, so only one of them is kept.
TEST_F(TestFileParserTest, RunsEqualPairingsOnce)
{
    const auto [path, statements] = readSlt(
        "GlobalConfiguration worker.query_engine.number_of_worker_threads: [1, 2]\n"
        "GlobalConfiguration worker.query_engine.number_of_worker_threads: [1]\n"
        "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
        "----\n"
        "1\n");

    ASSERT_EQ(statements.size(), 1U);
    const auto* query = std::get_if<SelectStatement>(&statements.at(0));
    ASSERT_NE(query, nullptr);
    EXPECT_EQ(query->overrides.at("worker.query_engine.number_of_worker_threads"), "1");
}

/// A key set for the whole file and again for one query is ambiguous, so the parser rejects it.
TEST_F(TestFileParserTest, RejectsAKeySetBothGloballyAndForOneQuery)
{
    EXPECT_THROW(
        readSlt(
            "GlobalConfiguration worker.query_engine.number_of_worker_threads: [1]\n"
            "Configuration worker.query_engine.number_of_worker_threads: [2]\n"
            "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
            "----\n"
            "1\n"),
        Exception);
}

/// Two lines that set the same key for the same query contradict each other, so the parser rejects them.
TEST_F(TestFileParserTest, RejectsAKeySetTwiceForOneQuery)
{
    EXPECT_THROW(
        readSlt(
            "Configuration worker.query_engine.number_of_worker_threads: [1]\n"
            "Configuration worker.query_engine.number_of_worker_threads: [2]\n"
            "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
            "----\n"
            "1\n"),
        Exception);
}

/// A query with no configuration runs exactly once, so listing no configuration is not an empty set of alternatives.
TEST_F(TestFileParserTest, RunsAQueryWithoutOverridesExactlyOnce)
{
    const auto [path, statements] = readSlt(
        "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
        "----\n"
        "1\n");

    ASSERT_EQ(statements.size(), 1U);
    const auto* query = std::get_if<SelectStatement>(&statements.at(0));
    ASSERT_NE(query, nullptr);
    EXPECT_TRUE(query->overrides.empty());
}

/// A result block that follows no query is a malformed test file.
/// Reading it reports that file, and the run goes on to the next one.
TEST_F(TestFileParserTest, RejectsAResultThatFollowsNoQuery)
{
    EXPECT_THROW(
        readSlt(
            "CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
            "----\n"
            "1\n"),
        Exception);
}

/// A query with no result block behind it would otherwise be dropped without an error, and the file would pass with one
/// query fewer than written.
TEST_F(TestFileParserTest, RejectsAQueryWithoutAResult)
{
    EXPECT_THROW(
        readSlt(
            "CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
            "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"),
        Exception);
}

/// An EXPLAIN returns the plan it prints rather than rows, so it is read as its own statement and the block
/// below it is the plan it expects.
TEST_F(TestFileParserTest, ReadsAnExplainWithThePlanItExpects)
{
    const auto [path, statements] = readSlt(
        "EXPLAIN (LOGICAL) FORMAT TEXT SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
        "----\n"
        "== Logical Plan ==\n"
        "SINK(SINKONETUPLE)\n"
        "==END==\n");

    ASSERT_EQ(statements.size(), 1U);
    const auto* explain = std::get_if<ExplainStatement>(&statements.at(0));
    ASSERT_NE(explain, nullptr);
    EXPECT_EQ(explain->sql, "EXPLAIN (LOGICAL) FORMAT TEXT SELECT field_1 FROM oneTuple INTO sinkOneTuple;");
    /// The terminator closes the block and is not part of the plan.
    EXPECT_EQ(explain->expected.lines, (std::vector<std::string>{"== Logical Plan ==", "SINK(SINKONETUPLE)"}));
}

/// An EXPLAIN is not executed, so a configuration line above it has no effect. The local line is dropped rather than
/// kept for the query below, while a global line stays in scope, as in the runner before the typed model.
TEST_F(TestFileParserTest, DropsALocalConfigurationAboveAnExplainAndKeepsAGlobalOne)
{
    const auto [path, statements] = readSlt(
        "GlobalConfiguration worker.query_engine.number_of_worker_threads: [2]\n"
        "Configuration worker.default_query_execution.operator_buffer_size: [1567]\n"
        "EXPLAIN (LOGICAL) FORMAT TEXT SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
        "----\n"
        "== Logical Plan ==\n"
        "SINK(SINKONETUPLE)\n"
        "==END==\n"
        "\n"
        "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
        "----\n"
        "1\n");

    ASSERT_EQ(statements.size(), 2U);
    ASSERT_NE(std::get_if<ExplainStatement>(&statements.at(0)), nullptr);
    const auto* query = std::get_if<SelectStatement>(&statements.at(1));
    ASSERT_NE(query, nullptr);
    EXPECT_EQ(query->overrides.at("worker.query_engine.number_of_worker_threads"), "2");
    EXPECT_FALSE(query->overrides.contains("worker.default_query_execution.operator_buffer_size"));
}

/// The reader captures each statement in file order, typed: creates with their attach data, the query with its expected rows.
TEST_F(TestFileParserTest, ReadsCreatesAndQueryWithExpectedRows)
{
    const auto [path, statements] = readSlt(
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

    EXPECT_EQ(path, "/tests/OneTuple.test");
    ASSERT_EQ(statements.size(), 4U);

    const auto* logicalSource = std::get_if<CreateStatement>(&statements.at(0));
    ASSERT_NE(logicalSource, nullptr);
    EXPECT_EQ(logicalSource->sql, "CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);");
    EXPECT_FALSE(logicalSource->attach.has_value());

    const auto* physicalSource = std::get_if<CreateStatement>(&statements.at(1));
    ASSERT_NE(physicalSource, nullptr);
    EXPECT_EQ(physicalSource->sql, "CREATE PHYSICAL SOURCE FOR oneTuple TYPE File;");
    const auto* inlineRows = physicalSource->attach.has_value() ? std::get_if<InlineRows>(&*physicalSource->attach) : nullptr;
    ASSERT_NE(inlineRows, nullptr);
    EXPECT_EQ(inlineRows->rows, (std::vector<std::string>{"1"}));

    const auto* sink = std::get_if<CreateStatement>(&statements.at(2));
    ASSERT_NE(sink, nullptr);
    EXPECT_EQ(sink->sql, "CREATE SINK sinkOneTuple(field_1 UINT64 NOT NULL) TYPE File;");
    EXPECT_FALSE(sink->attach.has_value());

    const auto* query = std::get_if<SelectStatement>(&statements.at(3));
    ASSERT_NE(query, nullptr);
    EXPECT_EQ(query->sql, "SELECT field_1 FROM oneTuple INTO sinkOneTuple;");
    EXPECT_EQ(query->id.getRawValue(), 1U);
    const auto* expectedRows = std::get_if<ExpectedRows>(&query->expected);
    ASSERT_NE(expectedRows, nullptr);
    EXPECT_EQ(expectedRows->rows, std::vector<std::string>{"1"});
}

/// An ATTACH FILE source keeps the referenced path rather than materializing anything.
TEST_F(TestFileParserTest, ReadsFileAttach)
{
    const auto [path, statements] = readSlt(
        "CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
        "CREATE PHYSICAL SOURCE FOR stream TYPE File;\n"
        "ATTACH FILE small/stream8.csv\n"
        "\n"
        "CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
        "\n"
        "SELECT id FROM stream INTO out;\n"
        "----\n"
        "1\n");

    ASSERT_EQ(statements.size(), 4U);
    const auto* physicalSource = std::get_if<CreateStatement>(&statements.at(1));
    ASSERT_NE(physicalSource, nullptr);
    const auto* attachedFile = physicalSource->attach.has_value() ? std::get_if<AttachedFile>(&*physicalSource->attach) : nullptr;
    ASSERT_NE(attachedFile, nullptr);
    EXPECT_EQ(attachedFile->path, std::filesystem::path{"small/stream8.csv"});
}

TEST_F(TestFileParserTest, ReadsAnExpectedErrorWithItsCodeAndMessage)
{
    const std::vector<std::tuple<std::string, ErrorCode, std::optional<std::string>>> forms{
        {"ERROR 1003 \"expected message\"", ErrorCode::SLTWrongSchema, "expected message"},
        {"ERROR SLTUnexpectedToken", ErrorCode::SLTUnexpectedToken, std::nullopt},
        {"ERROR 1003 unquoted message text", ErrorCode::SLTWrongSchema, "unquoted message text"},
        {"error 01003 \t\"quoted\"", ErrorCode::SLTWrongSchema, "quoted"},
        {"ERROR 1003 \"\"", ErrorCode::SLTWrongSchema, ""},
        {"ERROR 1003 ", ErrorCode::SLTWrongSchema, std::nullopt}};
    for (const auto& [errorLine, code, message] : forms)
    {
        SCOPED_TRACE(errorLine);
        const auto [path, statements] = readSlt("SELECT a FROM s INTO k;\n----\n" + errorLine + "\n");
        ASSERT_EQ(statements.size(), 1U);
        const auto* query = std::get_if<SelectStatement>(&statements.at(0));
        ASSERT_NE(query, nullptr);
        EXPECT_EQ(query->sql, "SELECT a FROM s INTO k;");
        EXPECT_EQ(query->id, SystestQueryId{1});
        const auto* error = std::get_if<ExpectedError>(&query->expected);
        ASSERT_NE(error, nullptr);
        EXPECT_EQ(error->code, code);
        EXPECT_EQ(error->message, message);
    }
}

TEST_F(TestFileParserTest, RejectsAnExplainThatExpectsAnError){ASSERT_EXCEPTION_ERRORCODE(
    { std::ignore = readSlt("EXPLAIN SELECT a FROM s INTO k;\n----\nERROR 1003\n"); }, ErrorCode::SLTUnexpectedToken)}

TEST_F(TestFileParserTest, AppliesALocalConfigurationToTheNextQueryOnly)
{
    const auto [path, statements] = readSlt(
        "Configuration worker.default_query_execution.operator_buffer_size: 1567\n"
        "SELECT a FROM s INTO k;\n"
        "----\n"
        "1\n"
        "\n"
        "SELECT b FROM s INTO k;\n"
        "----\n"
        "2\n");

    ASSERT_EQ(statements.size(), 2U);
    const auto* first = std::get_if<SelectStatement>(&statements.at(0));
    const auto* second = std::get_if<SelectStatement>(&statements.at(1));
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(first->overrides, (ConfigurationOverride{{"worker.default_query_execution.operator_buffer_size", "1567"}}));
    EXPECT_TRUE(second->overrides.empty());
}

TEST_F(TestFileParserTest, NumbersEveryKindOfQueryInFileOrder)
{
    const auto [path, statements] = readSlt(
        "Configuration c: [1, 2]\n"
        "SELECT a FROM s INTO k;\n"
        "----\n"
        "1\n"
        "\n"
        "SELECT b FROM s INTO k;\n"
        "----\n"
        "ERROR 1003\n"
        "\n"
        "EXPLAIN SELECT c FROM s INTO k;\n"
        "----\n"
        "plan\n"
        "==END==\n"
        "\n"
        "SELECT d FROM s INTO k;\n"
        "====\n"
        "SELECT e FROM s INTO k;\n"
        "\n"
        "SELECT f FROM s INTO k;\n"
        "----\n"
        "6\n");

    ASSERT_EQ(statements.size(), 6U);
    EXPECT_EQ(getQueryNumbersOf(statements.at(0)), std::vector{SystestQueryId{1}});
    EXPECT_EQ(getQueryNumbersOf(statements.at(1)), std::vector{SystestQueryId{1}});
    EXPECT_EQ(getQueryNumbersOf(statements.at(2)), std::vector{SystestQueryId{2}});
    EXPECT_EQ(getQueryNumbersOf(statements.at(3)), std::vector{SystestQueryId{3}});
    EXPECT_TRUE(std::holds_alternative<SelectStatement>(statements.at(2)));
    EXPECT_TRUE(std::holds_alternative<ExplainStatement>(statements.at(3)));
    const auto* differential = std::get_if<DifferentialStatement>(&statements.at(4));
    ASSERT_NE(differential, nullptr);
    EXPECT_EQ(differential->firstId, SystestQueryId{4});
    EXPECT_EQ(differential->secondId, SystestQueryId{4});
    const auto* last = std::get_if<SelectStatement>(&statements.at(5));
    ASSERT_NE(last, nullptr);
    EXPECT_EQ(last->sql, "SELECT f FROM s INTO k;");
    EXPECT_EQ(last->id, SystestQueryId{5});
}

TEST_F(TestFileParserTest, RunsADifferentialBlockOncePerPairingInScopeAboveIt)
{
    const auto [path, statements] = readSlt(
        "GlobalConfiguration g: [1, 2]\n"
        "Configuration c: 3\n"
        "SELECT a FROM s INTO k;\n"
        "====\n"
        "SELECT b FROM s\n"
        "INTO k;\n"
        "Configuration c: 4\n"
        "SELECT x FROM s INTO k;\n"
        "----\n"
        "1\n");

    ASSERT_EQ(statements.size(), 4U);
    for (const auto& [index, global] : std::vector<std::pair<size_t, std::string>>{{0, "1"}, {1, "2"}})
    {
        const auto* differential = std::get_if<DifferentialStatement>(&statements.at(index));
        ASSERT_NE(differential, nullptr);
        EXPECT_EQ(differential->firstSql, "SELECT a FROM s INTO k;");
        EXPECT_EQ(differential->secondSql, "SELECT b FROM s\nINTO k;");
        EXPECT_EQ(differential->firstId, SystestQueryId{1});
        EXPECT_EQ(differential->secondId, SystestQueryId{1});
        EXPECT_EQ(differential->overrides, (ConfigurationOverride{{"g", global}, {"c", "3"}}));
    }
    for (const auto& [index, global] : std::vector<std::pair<size_t, std::string>>{{2, "1"}, {3, "2"}})
    {
        const auto* query = std::get_if<SelectStatement>(&statements.at(index));
        ASSERT_NE(query, nullptr);
        EXPECT_EQ(query->sql, "SELECT x FROM s INTO k;");
        EXPECT_EQ(query->id, SystestQueryId{2});
        EXPECT_EQ(query->overrides, (ConfigurationOverride{{"g", global}, {"c", "4"}}));
    }
}

TEST_F(TestFileParserTest, DropsAStatementLineDirectlyBelowResultRows)
{
    const auto [path, statements] = readSlt(
        "SELECT a FROM s INTO k;\n"
        "----\n"
        "1\n"
        "Configuration c: 1\n"
        "SELECT b FROM s INTO k;\n"
        "----\n"
        "2\n"
        "selected,3\n"
        "\n"
        "SELECT c FROM s INTO k;\n"
        "----\n"
        "3\n"
        "CREATE SINK x(a UINT64) TYPE File;\n");

    ASSERT_EQ(statements.size(), 3U);
    const auto* first = std::get_if<SelectStatement>(&statements.at(0));
    const auto* second = std::get_if<SelectStatement>(&statements.at(1));
    const auto* third = std::get_if<SelectStatement>(&statements.at(2));
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    ASSERT_NE(third, nullptr);
    EXPECT_EQ(std::get<ExpectedRows>(first->expected).rows, std::vector<std::string>{"1"});
    EXPECT_TRUE(second->overrides.empty());
    EXPECT_EQ(std::get<ExpectedRows>(second->expected).rows, std::vector<std::string>{"2"});
    EXPECT_EQ(std::get<ExpectedRows>(third->expected).rows, std::vector<std::string>{"3"});
}

TEST_F(TestFileParserTest, KeepsWhitespaceRowsAndTextBeforeACommentInExpectedRows)
{
    const auto [path, statements] = readSlt(
        "SELECT a FROM s INTO k;\n"
        "----\n"
        "1,a # note\n"
        "   \n"
        "# full line comment\n"
        "2\n");

    ASSERT_EQ(statements.size(), 1U);
    const auto* query = std::get_if<SelectStatement>(&statements.at(0));
    ASSERT_NE(query, nullptr);
    EXPECT_EQ(std::get<ExpectedRows>(query->expected).rows, (std::vector<std::string>{"1,a ", "   ", "2"}));
}

TEST_F(TestFileParserTest, ReadsAResultBlockWithoutRowsAsExpectingNoRows)
{
    const auto [path, statements] = readSlt(
        "SELECT a FROM s INTO k;\n"
        "----\n"
        "\n"
        "SELECT b FROM s INTO k;\n"
        "----\n");

    ASSERT_EQ(statements.size(), 2U);
    for (const auto& statement : statements)
    {
        const auto* query = std::get_if<SelectStatement>(&statement);
        ASSERT_NE(query, nullptr);
        const auto* rows = std::get_if<ExpectedRows>(&query->expected);
        ASSERT_NE(rows, nullptr);
        EXPECT_TRUE(rows->rows.empty());
    }
}

TEST_F(TestFileParserTest, ReadsAnExplainPlanVerbatimUntilItsTerminator)
{
    const auto [path, statements] = readSlt(
        "EXPLAIN (ALL, FORMAT TEXT)\n"
        "SELECT a FROM s\n"
        "INTO k;\n"
        "----\n"
        "SELECT x\n"
        "----\n"
        "\n"
        "CREATE y\n"
        "==END== trailing\n"
        "SELECT b FROM s INTO k;\n"
        "----\n"
        "1\n");

    ASSERT_EQ(statements.size(), 2U);
    const auto* explain = std::get_if<ExplainStatement>(&statements.at(0));
    ASSERT_NE(explain, nullptr);
    EXPECT_EQ(explain->sql, "EXPLAIN (ALL, FORMAT TEXT)\nSELECT a FROM s\nINTO k;");
    EXPECT_EQ(explain->id, SystestQueryId{1});
    EXPECT_EQ(explain->expected.lines, (std::vector<std::string>{"SELECT x", "----", "", "CREATE y"}));
    const auto* query = std::get_if<SelectStatement>(&statements.at(1));
    ASSERT_NE(query, nullptr);
    EXPECT_EQ(query->id, SystestQueryId{2});
}

TEST_F(TestFileParserTest, ReadsTheRestOfTheFileIntoAnExplainPlanWithoutTerminator)
{
    const auto [path, statements] = readSlt(
        "EXPLAIN SELECT a FROM s INTO k;\n"
        "----\n"
        "plan\n"
        "\n"
        "SELECT b FROM s INTO k;\n"
        "----\n"
        "1\n");

    ASSERT_EQ(statements.size(), 1U);
    const auto* explain = std::get_if<ExplainStatement>(&statements.at(0));
    ASSERT_NE(explain, nullptr);
    EXPECT_EQ(explain->expected.lines, (std::vector<std::string>{"plan", "", "SELECT b FROM s INTO k;", "----", "1"}));
}

TEST_F(TestFileParserTest, ReadsAnExplainOnceUnderAListedGlobalConfiguration)
{
    const auto [path, statements] = readSlt(
        "GlobalConfiguration g: [1, 2]\n"
        "EXPLAIN SELECT a FROM s INTO k;\n"
        "----\n"
        "plan\n"
        "==END==\n");

    ASSERT_EQ(statements.size(), 1U);
    EXPECT_TRUE(std::holds_alternative<ExplainStatement>(statements.at(0)));
}

TEST_F(TestFileParserTest, ReadsAttachedDataUpToTheNextBlankLine)
{
    const auto [path, statements] = readSlt(
        "CREATE PHYSICAL SOURCE FOR s TYPE File;\n"
        "ATTACH INLINE\n"
        "1\n"
        "   \n"
        "2 # trailing\n"
        "CREATE SINK x(a UINT64) TYPE File;\n"
        "\n"
        "CREATE PHYSICAL SOURCE FOR t TYPE File;\n"
        "ATTACH INLINE\n"
        "\n"
        "CREATE PHYSICAL SOURCE FOR u TYPE File;\n"
        "\n"
        "ATTACH FILE  data/u.csv # note\n"
        "SELECT a FROM s INTO k;\n"
        "----\n"
        "1\n");

    ASSERT_EQ(statements.size(), 4U);
    const auto* withRows = std::get_if<CreateStatement>(&statements.at(0));
    ASSERT_NE(withRows, nullptr);
    ASSERT_TRUE(withRows->attach.has_value());
    EXPECT_EQ(
        std::get<InlineRows>(*withRows->attach).rows, (std::vector<std::string>{"1", "   ", "2 ", "CREATE SINK x(a UINT64) TYPE File;"}));

    const auto* withoutRows = std::get_if<CreateStatement>(&statements.at(1));
    ASSERT_NE(withoutRows, nullptr);
    EXPECT_EQ(withoutRows->sql, "CREATE PHYSICAL SOURCE FOR t TYPE File;");
    ASSERT_TRUE(withoutRows->attach.has_value());
    EXPECT_TRUE(std::get<InlineRows>(*withoutRows->attach).rows.empty());

    const auto* withFile = std::get_if<CreateStatement>(&statements.at(2));
    ASSERT_NE(withFile, nullptr);
    ASSERT_TRUE(withFile->attach.has_value());
    EXPECT_EQ(std::get<AttachedFile>(*withFile->attach).path, std::filesystem::path{" data/u.csv "});

    EXPECT_TRUE(std::holds_alternative<SelectStatement>(statements.at(3)));
}

TEST_F(TestFileParserTest, ReadsStatementTextAcrossLinesAndComments)
{
    const auto [path, statements] = readSlt(
        "CREATE LOGICAL SOURCE s(\n"
        "  # column\n"
        "  a UINT64\n"
        ");\n"
        "CREATE SINK x(a UINT64) TYPE File; # note\n"
        "CREATE SINK y(a UINT64) TYPE File;\n"
        "SELECT a\n"
        "# note\n"
        "FROM s INTO k; # trailing\n"
        "----\n"
        "1\n"
        "\n"
        "SELECT b FROM s INTO k;\n"
        "CREATE SINK z(a UINT64) TYPE File;\n"
        "----\n"
        "2\n");

    ASSERT_EQ(statements.size(), 4U);
    const auto* source = std::get_if<CreateStatement>(&statements.at(0));
    ASSERT_NE(source, nullptr);
    EXPECT_EQ(source->sql, "CREATE LOGICAL SOURCE s(\n  a UINT64\n);");
    const auto* sinks = std::get_if<CreateStatement>(&statements.at(1));
    ASSERT_NE(sinks, nullptr);
    EXPECT_EQ(sinks->sql, "CREATE SINK x(a UINT64) TYPE File; \nCREATE SINK y(a UINT64) TYPE File;");
    const auto* first = std::get_if<SelectStatement>(&statements.at(2));
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(first->sql, "SELECT a\nFROM s INTO k; ");
    const auto* second = std::get_if<SelectStatement>(&statements.at(3));
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(second->sql, "SELECT b FROM s INTO k;\nCREATE SINK z(a UINT64) TYPE File;");
}

TEST_F(TestFileParserTest, ReadsKeywordsRegardlessOfCase)
{
    const auto [path, statements] = readSlt(
        "create sink x(a UINT64) type File;\n"
        "configuration c: 1\n"
        "select a from s into k;\n"
        "----\n"
        "1\n"
        "\n"
        "explain select a from s into k;\n"
        "----\n"
        "plan\n"
        "==END==\n");

    ASSERT_EQ(statements.size(), 3U);
    EXPECT_TRUE(std::holds_alternative<CreateStatement>(statements.at(0)));
    const auto* query = std::get_if<SelectStatement>(&statements.at(1));
    ASSERT_NE(query, nullptr);
    EXPECT_EQ(query->overrides, (ConfigurationOverride{{"c", "1"}}));
    EXPECT_TRUE(std::holds_alternative<ExplainStatement>(statements.at(2)));
}

TEST_F(TestFileParserTest, ReadsAFileWithoutStatementsAsEmpty)
{
    EXPECT_TRUE(readSlt("").statements.empty());
    EXPECT_TRUE(readSlt("# only a comment\n\n   \n\r\n").statements.empty());
}

TEST_F(TestFileParserTest, RejectsAMalformedFileWithItsErrorCode)
{
    const std::vector<std::pair<std::string, ErrorCode>> malformed{
        {"----\n1\n", ErrorCode::SLTUnexpectedToken},
        {"CREATE SINK x(a UINT64) TYPE File\nSELECT a FROM s INTO k;\n----\n1\n", ErrorCode::SLTUnexpectedToken},
        {"SELECT a FROM s INTO k;\n", ErrorCode::SLTUnexpectedToken},
        {"SELECT a FROM s INTO k\n", ErrorCode::SLTUnexpectedToken},
        {"SELECT a FROM s INTO k;\n-----\n1\n", ErrorCode::SLTUnexpectedToken},
        {"SELECT a FROM s INTO k\n----\n1\n", ErrorCode::InvalidQuerySyntax},
        {"SELECT a FROM s INTO k;\nConfiguration c: 1\n----\n1\n", ErrorCode::InvalidQuerySyntax},
        {"SELECT a FROM s INTO k;\n\nSELECT b FROM s INTO k;\n----\n1\n", ErrorCode::SLTUnexpectedToken},
        {"SELECT a FROM s INTO k;\n---- # rows\n1\n", ErrorCode::SLTUnexpectedToken},
        {"SELECT a FROM s INTO k;\r\n----\r\n1\r\n", ErrorCode::SLTUnexpectedToken},
        {"  SELECTa;\n----\n1\n", ErrorCode::SLTUnexpectedToken},
        {"SELECT a FROM s INTO k;\n====\nSELECT b FROM s INTO k;\n----\n1\n", ErrorCode::SLTUnexpectedToken},
        {"SELECT a FROM s INTO k;\n====\nSELECT b FROM s INTO k;\n====\nSELECT c FROM s INTO k;\n", ErrorCode::SLTUnexpectedToken},
        {"SELECT a FROM s INTO k;\n----\n1\n\n====\nSELECT b FROM s INTO k;\n", ErrorCode::SLTUnexpectedToken},
        {"SELECT a FROM s INTO k;\n====\n----\n1\n", ErrorCode::InvalidQuerySyntax},
        {"SELECT a FROM s INTO k;\n====\n\n", ErrorCode::SLTUnexpectedToken},
        {"SELECT a FROM s INTO k\nDIFFERENTIAL\nSELECT b FROM s INTO k;\n", ErrorCode::SLTUnexpectedToken},
        {"SELECT a FROM s INTO k;\n----\nERROR 9999000\n", ErrorCode::SLTUnexpectedToken},
        {"SELECT a FROM s INTO k;\n----\nERROR sltunexpectedtoken\n", ErrorCode::SLTUnexpectedToken},
        {"SELECT a FROM s INTO k;\n----\nERROR 1003 \"open\n", ErrorCode::SLTUnexpectedToken},
        {"SELECT a FROM s INTO k;\n----\nERROR 1003 \"issue #42\"\n", ErrorCode::SLTUnexpectedToken},
        {"ERROR 1003\n", ErrorCode::TestException},
        {"Attach File x.csv\n", ErrorCode::SLTUnexpectedToken},
        {"CREATE PHYSICAL SOURCE FOR s TYPE File;\nattach inline\n1\n", ErrorCode::SLTUnexpectedToken},
        {"Configuration k:8\n", ErrorCode::SLTUnexpectedToken},
        {"Configuration k: 8, 16\n", ErrorCode::SLTUnexpectedToken},
        {"Configuration k: []\n", ErrorCode::SLTUnexpectedToken},
        {"Configuration k: 2\nGlobalConfiguration k: 1\nSELECT a FROM s INTO k;\n----\n1\n", ErrorCode::SLTUnexpectedToken},
        {"Configuration k: 1\nConfiguration k: 2\nSELECT a FROM s INTO k;\n----\n1\n", ErrorCode::SLTUnexpectedToken},
        {"GlobalConfiguration k: 1\nConfiguration k: 2\nSELECT a FROM s INTO k;\n----\n1\n", ErrorCode::SLTUnexpectedToken}};
    for (const auto& [content, code] : malformed)
    {
        SCOPED_TRACE(content);
        ASSERT_EXCEPTION_ERRORCODE({ std::ignore = readSlt(content); }, code)
    }
}

}
