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

#include <set>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

#include <Model/TestFile.hpp>
#include <Parser/SystestParser.hpp>
#include <Parser/TestFileParser.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES::Systest
{

class TestFileParserTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("TestFileParser.log", LogLevel::LOG_DEBUG); }

    static TestFile readSlt(const std::string& slt)
    {
        SystestParser parser;
        parser.loadString(slt);
        return parseTestFile(parser, "/tests/OneTuple.test");
    }
};

/// A configuration line applies to the queries below it, not to the whole file.
/// TupleLargerThanBuffer relies on this: it sets one buffer size, checks that a tuple does not fit, then sets a larger
/// one and checks that it does.
/// Reading the file as a whole would give both queries the same setting.
TEST_F(TestFileParserTest, ReadsAConfigurationAsApplyingToTheQueriesBelowIt)
{
    const auto testFile = readSlt("GlobalConfiguration worker.default_query_execution.operator_buffer_size: [1567]\n"
                                  "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                                  "----\n"
                                  "1\n"
                                  "\n"
                                  "GlobalConfiguration worker.default_query_execution.operator_buffer_size: [1568]\n"
                                  "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                                  "----\n"
                                  "1\n");

    ASSERT_EQ(testFile.statements.size(), 2U);
    const auto* first = std::get_if<Systest::QueryStatement>(&testFile.statements.at(0));
    const auto* second = std::get_if<Systest::QueryStatement>(&testFile.statements.at(1));
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(first->settings.at("worker.default_query_execution.operator_buffer_size"), "1567");
    EXPECT_EQ(second->settings.at("worker.default_query_execution.operator_buffer_size"), "1568");
}

/// A global line holds for what follows it, so a query below one inherits it without restating it.
TEST_F(TestFileParserTest, CarriesAGlobalConfigurationToEveryQueryBelowIt)
{
    const auto testFile = readSlt("GlobalConfiguration worker.query_engine.number_of_worker_threads: [1]\n"
                                  "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                                  "----\n"
                                  "1\n"
                                  "\n"
                                  "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                                  "----\n"
                                  "1\n");

    ASSERT_EQ(testFile.statements.size(), 2U);
    for (const auto& statement : testFile.statements)
    {
        const auto* query = std::get_if<Systest::QueryStatement>(&statement);
        ASSERT_NE(query, nullptr);
        EXPECT_EQ(query->settings.at("worker.query_engine.number_of_worker_threads"), "1");
    }
}

/// A configuration line may list values rather than one, and each is a setting the query runs under.
/// JoinNull relies on this: it asserts the join answers the same with the bloom filter on and off.
TEST_F(TestFileParserTest, RunsAQueryOncePerListedAlternative)
{
    const auto testFile = readSlt("GlobalConfiguration worker.default_query_execution.bloom_filter.enable_bloom_filter: [true, false]\n"
                                  "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                                  "----\n"
                                  "1\n");

    ASSERT_EQ(testFile.statements.size(), 2U);
    const auto* first = std::get_if<Systest::QueryStatement>(&testFile.statements.at(0));
    const auto* second = std::get_if<Systest::QueryStatement>(&testFile.statements.at(1));
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);

    static constexpr auto Key = "worker.default_query_execution.bloom_filter.enable_bloom_filter";
    EXPECT_EQ(first->settings.at(Key), "true");
    EXPECT_EQ(second->settings.at(Key), "false");
    /// The alternatives are the same query asserting the same answer, so everything but the settings agrees.
    EXPECT_EQ(first->sql, second->sql);
    EXPECT_EQ(first->id, second->id);
    EXPECT_EQ(std::get<ExpectedRows>(first->expected).rows, std::get<ExpectedRows>(second->expected).rows);
}

/// Two lines that each list values combine into every pairing of them, so the query runs once per pairing.
TEST_F(TestFileParserTest, CombinesTwoListedConfigurationsIntoEveryPairing)
{
    const auto testFile = readSlt("GlobalConfiguration worker.query_engine.number_of_worker_threads: [1, 2]\n"
                                  "Configuration worker.default_query_execution.operator_buffer_size: [1567, 1568]\n"
                                  "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                                  "----\n"
                                  "1\n");

    ASSERT_EQ(testFile.statements.size(), 4U);
    std::set<std::pair<std::string, std::string>> pairings;
    for (const auto& statement : testFile.statements)
    {
        const auto* query = std::get_if<Systest::QueryStatement>(&statement);
        ASSERT_NE(query, nullptr);
        pairings.emplace(
            query->settings.at("worker.query_engine.number_of_worker_threads"),
            query->settings.at("worker.default_query_execution.operator_buffer_size"));
    }
    const std::set<std::pair<std::string, std::string>> expected{{"1", "1567"}, {"1", "1568"}, {"2", "1567"}, {"2", "1568"}};
    EXPECT_EQ(pairings, expected);
}

/// A key set for the whole file and again for one query is ambiguous, so the parser rejects it.
TEST_F(TestFileParserTest, RejectsAKeySetBothGloballyAndForOneQuery)
{
    EXPECT_THROW(
        readSlt("GlobalConfiguration worker.query_engine.number_of_worker_threads: [1]\n"
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
        readSlt("Configuration worker.query_engine.number_of_worker_threads: [1]\n"
                "Configuration worker.query_engine.number_of_worker_threads: [2]\n"
                "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                "----\n"
                "1\n"),
        Exception);
}

/// A query with no configuration runs exactly once, so listing no configuration is not an empty set of alternatives.
TEST_F(TestFileParserTest, RunsAQueryWithoutSettingsExactlyOnce)
{
    const auto testFile = readSlt("SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                                  "----\n"
                                  "1\n");

    ASSERT_EQ(testFile.statements.size(), 1U);
    const auto* query = std::get_if<Systest::QueryStatement>(&testFile.statements.at(0));
    ASSERT_NE(query, nullptr);
    EXPECT_TRUE(query->settings.empty());
}

/// A result block that follows no query is a malformed test file.
/// Reading it reports that file, and the run goes on to the next one.
TEST_F(TestFileParserTest, RejectsAResultThatFollowsNoQuery)
{
    EXPECT_THROW(
        readSlt("CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
                "----\n"
                "1\n"),
        Exception);
}

/// An EXPLAIN answers with the plan it prints rather than with rows, so it is read as its own statement and the block
/// below it is the plan it expects.
TEST_F(TestFileParserTest, ReadsAnExplainWithThePlanItExpects)
{
    const auto testFile = readSlt("EXPLAIN (LOGICAL) FORMAT TEXT SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                                  "----\n"
                                  "== Logical Plan ==\n"
                                  "SINK(SINKONETUPLE)\n"
                                  "==END==\n");

    ASSERT_EQ(testFile.statements.size(), 1U);
    const auto* explain = std::get_if<Systest::ExplainStatement>(&testFile.statements.at(0));
    ASSERT_NE(explain, nullptr);
    EXPECT_EQ(explain->sql, "EXPLAIN (LOGICAL) FORMAT TEXT SELECT field_1 FROM oneTuple INTO sinkOneTuple;");
    /// The terminator closes the block and is not part of the plan.
    EXPECT_EQ(explain->expected, (ExpectedResult{"== Logical Plan ==", "SINK(SINKONETUPLE)"}));
}

/// The reader captures each statement in file order, typed: creates with their attach data, the query with its expected rows.
TEST_F(TestFileParserTest, ReadsCreatesAndQueryWithExpectedRows)
{
    const auto [path, statements] = readSlt("CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
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

    const auto* query = std::get_if<QueryStatement>(&statements.at(3));
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
    const auto [path, statements] = readSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
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

}
