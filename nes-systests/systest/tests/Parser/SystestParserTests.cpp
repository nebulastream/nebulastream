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
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <ranges>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <gtest/gtest.h>

#include <Discovery/TestFileReader.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Model/Expectation.hpp>
#include <Model/ParsedTestFile.hpp>
#include <Parser/SystestParser.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
std::string escape(const std::string_view text)
{
    std::string escaped;
    for (const auto character : text)
    {
        switch (character)
        {
            case '\\':
                escaped += "\\\\";
                break;
            case '\n':
                escaped += "\\n";
                break;
            case '\r':
                escaped += "\\r";
                break;
            case '\t':
                escaped += "\\t";
                break;
            default:
                escaped += character;
        }
    }
    return escaped;
}

std::string describeLines(const std::vector<std::string>& lines)
{
    const auto quoted = lines | std::views::transform([](const std::string& line) { return fmt::format("'{}'", escape(line)); });
    return fmt::format("[{}]", fmt::join(quoted, ", "));
}

std::string describeOverrides(const ConfigurationOverride& overrides)
{
    std::vector<std::string> pairs;
    for (const auto& [key, value] : overrides)
    {
        pairs.push_back(fmt::format("{}={}", escape(key), escape(value)));
    }
    return fmt::format("{{{}}}", fmt::join(pairs, ", "));
}

std::string describeExpectation(const Expectation& expectation)
{
    if (const auto* rows = std::get_if<ExpectedRows>(&expectation))
    {
        return "ROWS " + describeLines(rows->rows);
    }
    if (const auto* plan = std::get_if<ExpectedPlan>(&expectation))
    {
        return "PLAN " + describeLines(plan->lines);
    }
    const auto& error = std::get<ExpectedError>(expectation);
    const auto code = static_cast<uint64_t>(error.code);
    return error.message.has_value() ? fmt::format("ERROR {} '{}'", code, escape(*error.message)) : fmt::format("ERROR {}", code);
}

std::string describe(const TestStatement& statement)
{
    if (const auto* create = std::get_if<CreateStatement>(&statement))
    {
        if (!create->attach.has_value())
        {
            return fmt::format("CREATE '{}'", escape(create->sql));
        }
        if (const auto* rows = std::get_if<InlineRows>(&*create->attach))
        {
            return fmt::format("CREATE '{}' INLINE {}", escape(create->sql), describeLines(rows->rows));
        }
        return fmt::format("CREATE '{}' FILE '{}'", escape(create->sql), escape(std::get<AttachedFile>(*create->attach).path.string()));
    }
    if (const auto* select = std::get_if<SelectStatement>(&statement))
    {
        return fmt::format(
            "SELECT {} '{}' {} {}",
            select->id.getRawValue(),
            escape(select->sql),
            describeExpectation(select->expected),
            describeOverrides(select->overrides));
    }
    if (const auto* differential = std::get_if<DifferentialStatement>(&statement))
    {
        return fmt::format(
            "DIFFERENTIAL {} '{}' '{}' {}",
            differential->firstId.getRawValue(),
            escape(differential->firstSql),
            escape(differential->secondSql),
            describeOverrides(differential->overrides));
    }
    const auto& explain = std::get<ExplainStatement>(statement);
    return fmt::format("EXPLAIN {} '{}' {}", explain.id.getRawValue(), escape(explain.sql), describeLines(explain.expected.lines));
}

std::vector<std::string> describeParsed(const std::string& content)
{
    const auto substitutions = std::array{
        Substitution{.keyword = "TESTDATA", .replacement = "/data"}, Substitution{.keyword = "CONFIG/", .replacement = "/cfg/"}};
    try
    {
        std::vector<std::string> described;
        for (const auto& statement : parseTestFile(content, substitutions))
        {
            described.push_back(describe(statement));
        }
        return described;
    }
    catch (const Exception& exception)
    {
        return {fmt::format("EXCEPTION {}", exception.code())};
    }
}

std::vector<std::string> splitLines(const std::string_view text)
{
    std::vector<std::string> lines;
    std::istringstream stream{std::string{text}};
    for (std::string line; std::getline(stream, line);)
    {
        lines.push_back(line);
    }
    return lines;
}

class SystestParserTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("SystestParser.log", LogLevel::LOG_DEBUG); }

    static std::vector<TestStatement> parse(const std::string& content) { return parseTestFile(content, {}); }
};
}

/// A configuration line applies to the queries below it, not to the whole file.
/// TupleLargerThanBuffer relies on this: it sets one buffer size, checks that a tuple does not fit, then sets a larger
/// one and checks that it does.
/// Reading the file as a whole would give both queries the same setting.
TEST_F(SystestParserTest, ReadsAConfigurationAsApplyingToTheQueriesBelowIt)
{
    const auto statements = parse(
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
TEST_F(SystestParserTest, CarriesAGlobalConfigurationToEveryQueryBelowIt)
{
    const auto statements = parse(
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
TEST_F(SystestParserTest, RunsAQueryOncePerListedAlternative)
{
    const auto statements = parse(
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
TEST_F(SystestParserTest, CombinesTwoListedConfigurationsIntoEveryPairing)
{
    const auto statements = parse(
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
    const auto expected = std::set<std::pair<std::string, std::string>>{{"1", "1567"}, {"1", "1568"}, {"2", "1567"}, {"2", "1568"}};
    EXPECT_EQ(pairings, expected);
}

/// A later global line replaces the key of an earlier one in every pairing, so pairings can end up equal.
/// Equal pairings would run the same query twice with the same configuration, so only one of them is kept.
TEST_F(SystestParserTest, RunsEqualPairingsOnce)
{
    const auto statements = parse(
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
TEST_F(SystestParserTest, RejectsAKeySetBothGloballyAndForOneQuery){ASSERT_EXCEPTION_ERRORCODE(
    {
        std::ignore = parse(
            "GlobalConfiguration worker.query_engine.number_of_worker_threads: [1]\n"
            "Configuration worker.query_engine.number_of_worker_threads: [2]\n"
            "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
            "----\n"
            "1\n");
    },
    ErrorCode::SLTUnexpectedToken)}

/// Two lines that set the same key for the same query contradict each other, so the parser rejects them.
TEST_F(SystestParserTest, RejectsAKeySetTwiceForOneQuery){ASSERT_EXCEPTION_ERRORCODE(
    {
        std::ignore = parse(
            "Configuration worker.query_engine.number_of_worker_threads: [1]\n"
            "Configuration worker.query_engine.number_of_worker_threads: [2]\n"
            "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
            "----\n"
            "1\n");
    },
    ErrorCode::SLTUnexpectedToken)}

/// A query with no configuration runs exactly once, so listing no configuration is not an empty set of alternatives.
TEST_F(SystestParserTest, RunsAQueryWithoutOverridesExactlyOnce)
{
    const auto statements = parse(
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
TEST_F(SystestParserTest, RejectsAResultThatFollowsNoQuery){ASSERT_EXCEPTION_ERRORCODE(
    {
        std::ignore = parse(
            "CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
            "----\n"
            "1\n");
    },
    ErrorCode::SLTUnexpectedToken)}

/// A query with no result block behind it would otherwise be dropped without an error, and the file would pass with one
/// query fewer than written.
TEST_F(SystestParserTest, RejectsAQueryWithoutAResult){ASSERT_EXCEPTION_ERRORCODE(
    {
        std::ignore = parse(
            "CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
            "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n");
    },
    ErrorCode::SLTUnexpectedToken)}

/// An EXPLAIN returns the plan it prints rather than rows, so it is read as its own statement and the block
/// below it is the plan it expects.
TEST_F(SystestParserTest, ReadsAnExplainWithThePlanItExpects)
{
    const auto statements = parse(
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

/// An EXPLAIN is not executed, so a configuration line above it has no effect.
/// The parser drops the local line instead of keeping it for the query below, and a global line stays in scope.
TEST_F(SystestParserTest, DropsALocalConfigurationAboveAnExplainAndKeepsAGlobalOne)
{
    const auto statements = parse(
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
TEST_F(SystestParserTest, ReadsCreatesAndQueryWithExpectedRows)
{
    const auto statements = parse(
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
TEST_F(SystestParserTest, ReadsFileAttach)
{
    const auto statements = parse(
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

TEST_F(SystestParserTest, ReadsAnExpectedErrorWithItsCodeAndMessage)
{
    const auto forms = std::vector<std::tuple<std::string, ErrorCode, std::optional<std::string>>>{
        {"ERROR 1003 \"expected message\"", ErrorCode::SLTWrongSchema, "expected message"},
        {"ERROR SLTUnexpectedToken", ErrorCode::SLTUnexpectedToken, std::nullopt},
        {"ERROR 1003 unquoted message text", ErrorCode::SLTWrongSchema, "unquoted message text"},
        {"error 01003 \t\"quoted\"", ErrorCode::SLTWrongSchema, "quoted"},
        {"ERROR 1003 \"\"", ErrorCode::SLTWrongSchema, ""},
        {"ERROR 1003 ", ErrorCode::SLTWrongSchema, std::nullopt},
        {"ERROR 1003 \"", ErrorCode::SLTWrongSchema, ""}};
    for (const auto& [errorLine, code, message] : forms)
    {
        SCOPED_TRACE(errorLine);
        const auto statements = parse("SELECT a FROM s INTO k;\n----\n" + errorLine + "\n");
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

TEST_F(SystestParserTest, RejectsAnExplainThatExpectsAnError){ASSERT_EXCEPTION_ERRORCODE(
    { std::ignore = parse("EXPLAIN SELECT a FROM s INTO k;\n----\nERROR 1003\n"); }, ErrorCode::SLTUnexpectedToken)}

TEST_F(SystestParserTest, AppliesALocalConfigurationToTheNextQueryOnly)
{
    const auto statements = parse(
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

TEST_F(SystestParserTest, NumbersEveryKindOfQueryInFileOrder)
{
    const auto statements = parse(
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
    const auto* last = std::get_if<SelectStatement>(&statements.back());
    ASSERT_NE(last, nullptr);
    EXPECT_EQ(last->sql, "SELECT f FROM s INTO k;");
    EXPECT_EQ(last->id, SystestQueryId{5});
}

TEST_F(SystestParserTest, RunsADifferentialBlockOncePerPairingInScopeAboveIt)
{
    const auto statements = parse(
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

TEST_F(SystestParserTest, DropsAStatementLineDirectlyBelowResultRows)
{
    const auto statements = parse(
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

TEST_F(SystestParserTest, KeepsWhitespaceRowsAndTextBeforeACommentInExpectedRows)
{
    const auto statements = parse(
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

TEST_F(SystestParserTest, ReadsAResultBlockWithoutRowsAsExpectingNoRows)
{
    const auto statements = parse(
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

TEST_F(SystestParserTest, ReadsAnExplainPlanVerbatimUntilItsTerminator)
{
    const auto statements = parse(
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

TEST_F(SystestParserTest, ReadsTheRestOfTheFileIntoAnExplainPlanWithoutTerminator)
{
    const auto statements = parse(
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

TEST_F(SystestParserTest, ReadsAnExplainOnceUnderAListedGlobalConfiguration)
{
    const auto statements = parse(
        "GlobalConfiguration g: [1, 2]\n"
        "EXPLAIN SELECT a FROM s INTO k;\n"
        "----\n"
        "plan\n"
        "==END==\n");

    ASSERT_EQ(statements.size(), 1U);
    EXPECT_TRUE(std::holds_alternative<ExplainStatement>(statements.at(0)));
}

TEST_F(SystestParserTest, ReadsAttachedDataUpToTheNextBlankLine)
{
    const auto statements = parse(
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
    const auto* rows = withRows->attach.has_value() ? std::get_if<InlineRows>(&*withRows->attach) : nullptr;
    ASSERT_NE(rows, nullptr);
    EXPECT_EQ(rows->rows, (std::vector<std::string>{"1", "   ", "2 ", "CREATE SINK x(a UINT64) TYPE File;"}));

    const auto* withoutRows = std::get_if<CreateStatement>(&statements.at(1));
    ASSERT_NE(withoutRows, nullptr);
    EXPECT_EQ(withoutRows->sql, "CREATE PHYSICAL SOURCE FOR t TYPE File;");
    const auto* noRows = withoutRows->attach.has_value() ? std::get_if<InlineRows>(&*withoutRows->attach) : nullptr;
    ASSERT_NE(noRows, nullptr);
    EXPECT_TRUE(noRows->rows.empty());

    const auto* withFile = std::get_if<CreateStatement>(&statements.at(2));
    ASSERT_NE(withFile, nullptr);
    const auto* file = withFile->attach.has_value() ? std::get_if<AttachedFile>(&*withFile->attach) : nullptr;
    ASSERT_NE(file, nullptr);
    EXPECT_EQ(file->path, std::filesystem::path{" data/u.csv "});

    EXPECT_TRUE(std::holds_alternative<SelectStatement>(statements.at(3)));
}

TEST_F(SystestParserTest, ReadsStatementTextAcrossLinesAndComments)
{
    const auto statements = parse(
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

TEST_F(SystestParserTest, ReadsKeywordsRegardlessOfCase)
{
    const auto statements = parse(
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

TEST_F(SystestParserTest, ReadsAFileWithoutStatementsAsEmpty)
{
    EXPECT_TRUE(parse("").empty());
    EXPECT_TRUE(parse("# only a comment\n\n   \n\r\n").empty());
}

TEST_F(SystestParserTest, RejectsAMalformedFileWithItsErrorCode)
{
    const auto malformed = std::vector<std::pair<std::string, ErrorCode>>{
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
        {"GlobalConfiguration k: 1\nConfiguration k: 2\nSELECT a FROM s INTO k;\n----\n1\n", ErrorCode::SLTUnexpectedToken},
        {"SELECT a FROM s INTO k;\n----\nERROR\n", ErrorCode::SLTUnexpectedToken},
        {"SELECT a FROM s INTO k;\n----\nERROR 1003 close\"\n", ErrorCode::SLTUnexpectedToken},
        {"SELECT a FROM s INTO k;\n----\nERROR 1003 \"message\" # note\n", ErrorCode::SLTUnexpectedToken},
        {"SELECT a FROM s INTO k\n====\nSELECT b FROM s INTO k;\n", ErrorCode::InvalidQuerySyntax},
        {"SELECT a FROM s INTO k;\n====\nSELECT b FROM s INTO k;\nERROR 1003\n", ErrorCode::TestException},
        {"SELECT a FROM s INTO k;\n====\n", ErrorCode::SLTUnexpectedToken},
        {"Configuration\n", ErrorCode::SLTUnexpectedToken},
        {"Configuration k [8]\n", ErrorCode::SLTUnexpectedToken},
        {"Configuration : 8\n", ErrorCode::SLTUnexpectedToken},
        {"Configuration k:\n", ErrorCode::SLTUnexpectedToken},
        {"Configuration k: [8\n", ErrorCode::SLTUnexpectedToken},
        {"Configuration k: 8]\n", ErrorCode::SLTUnexpectedToken},
        {"Configuration k: [8, ,16]\n", ErrorCode::SLTUnexpectedToken},
        {"Configuration k: [8] # note\n", ErrorCode::SLTUnexpectedToken},
        {"GlobalConfiguration g:1\n", ErrorCode::SLTUnexpectedToken}};
    for (const auto& [content, code] : malformed)
    {
        SCOPED_TRACE(content);
        ASSERT_EXCEPTION_ERRORCODE({ std::ignore = parse(content); }, code)
    }
}

TEST_F(SystestParserTest, ReportsMalformedBlocksAsUnexpectedTokens)
{
    const auto malformed = std::vector<std::string>{
        "SELECT a FROM s INTO k;\n----\n\nERROR 1003\n",
        "EXPLAIN SELECT a FROM s INTO k;\n====\nSELECT b FROM s INTO k;\n",
        "====\nSELECT b FROM s INTO k;\n",
        "CREATE PHYSICAL SOURCE FOR s TYPE File;\nATTACH FILE\n",
        "SELECT a FROM s INTO k;\n----\nERROR 99999999999999999999999\n"};
    for (const auto& content : malformed)
    {
        SCOPED_TRACE(content);
        ASSERT_EXCEPTION_ERRORCODE({ std::ignore = parse(content); }, ErrorCode::SLTUnexpectedToken)
    }
}

TEST_F(SystestParserTest, ReadsConfigurationValues)
{
    const auto accepted = std::vector<std::pair<std::string, std::vector<ConfigurationOverride>>>{
        {"Configuration k: 8", {ConfigurationOverride{{"k", "8"}}}},
        {"Configuration   k:   8 # note", {ConfigurationOverride{{"k", "8"}}}},
        {"Configuration k: some value", {ConfigurationOverride{{"k", "some value"}}}},
        {"Configuration k: [8, 1024]", {ConfigurationOverride{{"k", "8"}}, ConfigurationOverride{{"k", "1024"}}}},
        {"Configuration k: [ 8 ]", {ConfigurationOverride{{"k", "8"}}}},
        {"Configuration k: [8,,16]", {ConfigurationOverride{{"k", "8"}}, ConfigurationOverride{{"k", "16"}}}},
        {"Configuration k: [,]", {ConfigurationOverride{}}},
        {"GlobalConfiguration g: [1, 2]", {ConfigurationOverride{{"g", "1"}}, ConfigurationOverride{{"g", "2"}}}}};
    for (const auto& [line, expected] : accepted)
    {
        SCOPED_TRACE(line);
        std::vector<ConfigurationOverride> overrides;
        for (const auto& statement : parse(line + "\nSELECT a FROM s INTO k;\n----\n1\n"))
        {
            overrides.push_back(std::get<SelectStatement>(statement).overrides);
        }
        EXPECT_EQ(overrides, expected);
    }
}

TEST_F(SystestParserTest, EndsTheSecondQueryOfADifferentialBlockAtTheNextStatement)
{
    const auto statements = parse("SELECT a FROM s INTO k;\n====\nSELECT b FROM s INTO k;\nCREATE SINK x(a UINT64) TYPE File;\n");

    ASSERT_EQ(statements.size(), 2U);
    const auto* differential = std::get_if<DifferentialStatement>(&statements.at(0));
    ASSERT_NE(differential, nullptr);
    EXPECT_EQ(differential->secondSql, "SELECT b FROM s INTO k;");
    const auto* create = std::get_if<CreateStatement>(&statements.at(1));
    ASSERT_NE(create, nullptr);
    EXPECT_EQ(create->sql, "CREATE SINK x(a UINT64) TYPE File;");
}

TEST_F(SystestParserTest, EndsAnExplainPlanOnlyAtATerminatorInTheFirstColumn)
{
    const auto statements = parse("EXPLAIN SELECT a FROM s INTO k;\n---- \nplan\n  ==END==\n==END==\n");

    ASSERT_EQ(statements.size(), 1U);
    const auto* explain = std::get_if<ExplainStatement>(&statements.at(0));
    ASSERT_NE(explain, nullptr);
    EXPECT_EQ(explain->expected.lines, (std::vector<std::string>{"---- ", "plan", "  ==END=="}));
}

TEST_F(SystestParserTest, KeepsTheTrailingNewlineOfAnUnterminatedCreateAtTheEndOfTheFile)
{
    const auto statements = parse("CREATE SINK x(a UINT64) TYPE File\n");

    ASSERT_EQ(statements.size(), 1U);
    EXPECT_EQ(std::get<CreateStatement>(statements.at(0)).sql, "CREATE SINK x(a UINT64) TYPE File\n");
}

TEST_F(SystestParserTest, ReadsSelectAsAPrefixOfTheLine)
{
    for (const auto& sql : {std::string{"  SELECT a FROM s INTO k;"}, std::string{"SELECTa;"}})
    {
        SCOPED_TRACE(sql);
        const auto statements = parse(sql + "\n----\n1\n");
        ASSERT_EQ(statements.size(), 1U);
        EXPECT_EQ(std::get<SelectStatement>(statements.at(0)).sql, sql);
    }
}

TEST_F(SystestParserTest, SubstitutesKeywordsThatStandAsWholeWords)
{
    const auto substitutions = std::array{
        Substitution{.keyword = "we", .replacement = "R"},
        Substitution{.keyword = "CONFIG/", .replacement = "/cfg/"},
        Substitution{.keyword = "alpha", .replacement = "beta alpha"},
        Substitution{.keyword = "beta", .replacement = "gamma"}};

    const auto statements = parseTestFile(
        "SELECT producedPower, my_we, we_x, wex, WE, we, (we), CONFIG/a, CONFIG/ , alpha FROM s INTO k; # we\n"
        "----\n"
        "we,CONFIG/\n",
        substitutions);

    ASSERT_EQ(statements.size(), 1U);
    const auto& query = std::get<SelectStatement>(statements.at(0));
    EXPECT_EQ(query.sql, "SELECT producedPower, my_we, we_x, wex, WE, R, (R), CONFIG/a, /cfg/ , gamma alpha FROM s INTO k; ");
    EXPECT_EQ(std::get<ExpectedRows>(query.expected).rows, std::vector<std::string>{"R,/cfg/"});
}

TEST_F(SystestParserTest, ParsesEdgeCasesIntoTheRecordedStatements)
{
    constexpr std::string_view inputMarker = "@@@ input\n";
    constexpr std::string_view expectedMarker = "@@@ expected\n";
    const auto cases = readTestFile(SYSTEST_DATA_DIR "parser_edge_cases.dummy");
    size_t checked = 0;
    for (auto start = cases.find(inputMarker); start != std::string::npos; ++checked)
    {
        const auto inputStart = start + inputMarker.size();
        const auto expectedStart = cases.find(expectedMarker, inputStart);
        ASSERT_NE(expectedStart, std::string::npos);
        start = cases.find(inputMarker, expectedStart);
        const auto input = cases.substr(inputStart, expectedStart - inputStart);
        const auto expected
            = std::string_view{cases}.substr(expectedStart + expectedMarker.size(), start - expectedStart - expectedMarker.size());
        SCOPED_TRACE(input);
        EXPECT_EQ(describeParsed(input), splitLines(expected));
    }
    EXPECT_EQ(checked, 93U);
}

}
