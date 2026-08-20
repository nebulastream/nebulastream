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
#include <initializer_list>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

#include <Identifiers/Identifiers.hpp>
#include <Model/Expectation.hpp>
#include <Model/RewrittenTest.hpp>
#include <Parser/SystestParser.hpp>
#include <Parser/TestFileBuilder.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Rewriter/SourceRewriting.hpp>
#include <Rewriter/SqlRewriter.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

class RewriteIdentifiersTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("RewriteIdentifiers.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup RewriteIdentifiers test class.");
    }

    static void TearDownTestSuite() { NES_INFO("Tear down RewriteIdentifiers test class."); }

    /// Registers the given catalog names under one key and seals them, mirroring the declaring pass that precedes a rewrite.
    static QualifiedNames namesWith(const std::initializer_list<std::string_view> declared)
    {
        NameRegistry registry{"BENCHMARK_NEXMARK"};
        for (const auto name : declared)
        {
            registry.declare(name);
        }
        return std::move(registry).seal();
    }
};

/// The rewrite substitutes a registered name and preserves the surrounding keywords, punctuation and whitespace exactly.
TEST_F(RewriteIdentifiersTest, SubstitutesRegisteredNames)
{
    const auto names = namesWith({"stream", "bid"});
    EXPECT_EQ(rewriteIdentifiers("SELECT bid FROM stream", names), "SELECT BENCHMARK_NEXMARK_BID FROM BENCHMARK_NEXMARK_STREAM");
}

/// The catalog compares an unquoted name case-insensitively, so the token matches its registration however it is cased.
TEST_F(RewriteIdentifiersTest, MatchesRegisteredNameRegardlessOfCase)
{
    const auto names = namesWith({"stream"});
    EXPECT_EQ(rewriteIdentifiers("SELECT x FROM STREAM", names), "SELECT x FROM BENCHMARK_NEXMARK_STREAM");
}

/// The rewrite leaves an identifier that the qualifier never registered unchanged, so column names and aliases pass through.
TEST_F(RewriteIdentifiersTest, LeavesUnregisteredIdentifiersAlone)
{
    const auto names = namesWith({"stream"});
    EXPECT_EQ(rewriteIdentifiers("SELECT unknown FROM stream", names), "SELECT unknown FROM BENCHMARK_NEXMARK_STREAM");
}

/// An identifier that merely contains a registered name is a distinct token, and the rewrite leaves it unchanged.
TEST_F(RewriteIdentifiersTest, DoesNotRewriteIdentifierContainingRegisteredName)
{
    const auto names = namesWith({"stream"});
    EXPECT_EQ(rewriteIdentifiers("SELECT x FROM streamSink", names), "SELECT x FROM streamSink");
    EXPECT_EQ(rewriteIdentifiers("SELECT x FROM sinkStream", names), "SELECT x FROM sinkStream");
}

/// A registered name that appears inside a string literal is not an identifier token, so the literal stays byte for byte.
TEST_F(RewriteIdentifiersTest, DoesNotRewriteInsideStringLiterals)
{
    const auto names = namesWith({"stream"});
    EXPECT_EQ(rewriteIdentifiers("INTO File(file_path='/data/stream/x.csv')", names), "INTO File(file_path='/data/stream/x.csv')");
}

/// Rewriting is a fixed point: the qualified spelling is not itself registered, so a second pass changes nothing.
TEST_F(RewriteIdentifiersTest, IsIdempotent)
{
    const auto names = namesWith({"stream", "bid"});
    const auto once = rewriteIdentifiers("SELECT bid FROM stream", names);
    EXPECT_EQ(rewriteIdentifiers(once, names), once);
}

class SqlRewriterTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("SqlRewriter.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup SqlRewriter test class.");
    }

    static void TearDownTestSuite() { NES_INFO("Tear down SqlRewriter test class."); }

    static RewrittenTest rewriteSlt(const std::string& slt)
    {
        SystestParser parser;
        parser.loadString(slt);
        const auto testFile = buildTestFile(parser, "/tests/OneTuple.test");
        return rewriteTestFile(
            testFile,
            RewriteTarget{
                .testFileKey = "TESTKEY",
                .displayName = "testkey",
                .workingDir = "/work",
                .testDataDir = "/data",
                .sourceHost = Host{"localhost:8080"},
                .sinkHost = Host{"localhost:8080"}});
    }

    /// Unwraps the single-query action of a case, throwing when the case holds a differential block instead.
    static const RewrittenQuery& queryOf(const RewrittenCase& testCase) { return std::get<RewrittenQuery>(testCase.action); }
};

/// A file marked `SEQUENTIAL_EXECUTION` runs its queries in order, so every query of it waits for the one above.
/// The first query has nothing above it, and the runner starts it whatever the flag says.
TEST_F(SqlRewriterTest, MarksEveryQueryOfASequentialFileAsFollowingTheOneAbove)
{
    const auto [name, qualifyingPrefix, setup, queries] = rewriteSlt("SEQUENTIAL_EXECUTION\n"
                                                                     "CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
                                                                     "CREATE SINK sinkOneTuple(field_1 UINT64 NOT NULL) TYPE File;\n"
                                                                     "\n"
                                                                     "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                                                                     "----\n"
                                                                     "1\n"
                                                                     "\n"
                                                                     "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                                                                     "----\n"
                                                                     "1\n");

    ASSERT_EQ(queries.size(), 2U);
    EXPECT_TRUE(queries.at(0).runsAfterPrevious);
    EXPECT_TRUE(queries.at(1).runsAfterPrevious);
}

/// A differential block asserts one thing, that its two queries agree, so it becomes one case with one verdict.
/// The runner submits the halves back to back itself, so the case does not wait for anything in a file without order.
TEST_F(SqlRewriterTest, MergesADifferentialBlockIntoOneCase)
{
    const auto [name, qualifyingPrefix, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
                                                                     "CREATE SINK sinkOneTuple(field_1 UINT64 NOT NULL) TYPE File;\n"
                                                                     "\n"
                                                                     "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                                                                     "====\n"
                                                                     "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n");

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_FALSE(queries.at(0).runsAfterPrevious);
    EXPECT_TRUE(std::holds_alternative<RewrittenDifferential>(queries.at(0).action));
}

/// The plain path: an inline source, a declared File sink, and one query rewrite to qualified DDL, a configured physical source, and an
/// inlined per-query sink.
TEST_F(SqlRewriterTest, RewritesInlineSourceNamedSinkAndQuery)
{
    const auto [name, qualifyingPrefix, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
                                                                     "CREATE PHYSICAL SOURCE FOR oneTuple TYPE File;\n"
                                                                     "ATTACH INLINE\n"
                                                                     "1\n"
                                                                     "\n"
                                                                     "CREATE SINK sinkOneTuple(field_1 UINT64 NOT NULL) TYPE File;\n"
                                                                     "\n"
                                                                     "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                                                                     "----\n"
                                                                     "1\n");

    ASSERT_EQ(setup.size(), 2U);
    EXPECT_EQ(sqlOf(setup.at(0)), "CREATE LOGICAL SOURCE TESTKEY_ONETUPLE(field_1 UINT64 NOT NULL);");
    EXPECT_TRUE(std::holds_alternative<PlainStatement>(setup.at(0)));

    EXPECT_EQ(
        sqlOf(setup.at(1)),
        R"(CREATE PHYSICAL SOURCE FOR TESTKEY_ONETUPLE TYPE File SET ('/work/sources/TESTKEY_0.csv' AS "SOURCE"."FILE_PATH", )"
        R"('localhost:8080' AS "SOURCE"."HOST", 'CSV' AS "INPUT_FORMATTER"."TYPE");)");
    const auto* withInline = std::get_if<StatementWithInlineData>(&setup.at(1));
    ASSERT_NE(withInline, nullptr);
    EXPECT_EQ(withInline->data.path, "/work/sources/TESTKEY_0.csv");
    EXPECT_EQ(withInline->data.rows, (std::vector<std::string>{"1"}));

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_EQ(
        queryOf(queries.at(0)).sql,
        R"(SELECT field_1 FROM TESTKEY_ONETUPLE INTO File('localhost:8080' AS "SINK"."HOST", '/work/TESTKEY_1.csv' AS "SINK"."FILE_PATH", )"
        R"('CSV' AS "SINK"."OUTPUT_FORMAT", SCHEMA(field_1 UINT64 NOT NULL) AS "SINK"."SCHEMA");)");
    EXPECT_EQ(queryOf(queries.at(0)).resultFile, "/work/TESTKEY_1.csv");
    const auto* expectedRows = std::get_if<ExpectedRows>(&queryOf(queries.at(0)).expectation);
    ASSERT_NE(expectedRows, nullptr);
    EXPECT_EQ(expectedRows->rows, (std::vector<std::string>{"1"}));
}

/// The two halves of a differential block share one query number, and one result file between them would compare a
/// result against itself and pass whatever either query returned.
TEST_F(SqlRewriterTest, GivesEachHalfOfADifferentialBlockItsOwnResultFile)
{
    const auto [name, qualifyingPrefix, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
                                                                     "CREATE SINK sinkOneTuple(field_1 UINT64 NOT NULL) TYPE File;\n"
                                                                     "\n"
                                                                     "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                                                                     "====\n"
                                                                     "SELECT field_1 FROM oneTuple WHERE field_1 > 0 INTO sinkOneTuple;\n");

    ASSERT_EQ(queries.size(), 1U);
    const auto* differential = std::get_if<RewrittenDifferential>(&queries.at(0).action);
    ASSERT_NE(differential, nullptr);
    EXPECT_NE(differential->firstResultFile, differential->secondResultFile);
    EXPECT_NE(differential->firstSql, differential->secondSql);
}

/// Every name is registered before any statement is rewritten, so a source declared below the query that reads it still qualifies.
/// Registering as each statement was rewritten would leave this reference raw and the query would bind against nothing.
TEST_F(SqlRewriterTest, QualifiesSourceDeclaredBelowTheQuery)
{
    const auto [name, qualifyingPrefix, setup, queries] = rewriteSlt("CREATE SINK sinkOneTuple(field_1 UINT64 NOT NULL) TYPE File;\n"
                                                                     "\n"
                                                                     "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                                                                     "----\n"
                                                                     "1\n"
                                                                     "\n"
                                                                     "CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n");

    ASSERT_EQ(setup.size(), 1U);
    EXPECT_EQ(sqlOf(setup.at(0)), "CREATE LOGICAL SOURCE TESTKEY_ONETUPLE(field_1 UINT64 NOT NULL);");

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_EQ(
        queryOf(queries.at(0)).sql,
        R"(SELECT field_1 FROM TESTKEY_ONETUPLE INTO File('localhost:8080' AS "SINK"."HOST", '/work/TESTKEY_1.csv' AS "SINK"."FILE_PATH", )"
        R"('CSV' AS "SINK"."OUTPUT_FORMAT", SCHEMA(field_1 UINT64 NOT NULL) AS "SINK"."SCHEMA");)");
}

/// The declaring pass stores sinks alongside the names it registers, so a sink declared below the query that writes to it is still
/// inlined with the schema that it declares.
TEST_F(SqlRewriterTest, InlinesSinkDeclaredBelowTheQuery)
{
    const auto [name, qualifyingPrefix, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
                                                                     "\n"
                                                                     "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                                                                     "----\n"
                                                                     "1\n"
                                                                     "\n"
                                                                     "CREATE SINK sinkOneTuple(field_1 UINT64 NOT NULL) TYPE File;\n");

    ASSERT_EQ(setup.size(), 1U);
    ASSERT_EQ(queries.size(), 1U);
    EXPECT_EQ(
        queryOf(queries.at(0)).sql,
        R"(SELECT field_1 FROM TESTKEY_ONETUPLE INTO File('localhost:8080' AS "SINK"."HOST", '/work/TESTKEY_1.csv' AS "SINK"."FILE_PATH", )"
        R"('CSV' AS "SINK"."OUTPUT_FORMAT", SCHEMA(field_1 UINT64 NOT NULL) AS "SINK"."SCHEMA");)");
    EXPECT_EQ(queryOf(queries.at(0)).resultFile, "/work/TESTKEY_1.csv");
}

/// A test that asserts a syntax error writes a query the parser rejects, and nothing can be inlined into a statement with no parse tree.
/// It goes to the coordinator as it stands, so the coordinator reports the error against that query rather than the rewrite failing
/// every query of the file.
/// The rewrite still qualifies its source references, because substituting names reads tokens.
TEST_F(SqlRewriterTest, PassesAQueryThatDoesNotParseThrough)
{
    const auto [name, qualifyingPrefix, setup, queries]
        = rewriteSlt("CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
                     "CREATE PHYSICAL SOURCE FOR oneTuple TYPE File;\n"
                     "ATTACH INLINE\n"
                     "1\n"
                     "\n"
                     "CREATE SINK sinkOneTuple(field_1 UINT64 NOT NULL) TYPE File;\n"
                     "\n"
                     "SELECT field_1 FROM oneTuple GROUP BY field_1 WHERE field_1 > UINT64(1) INTO sinkOneTuple;\n"
                     "----\n"
                     "ERROR 2000\n");

    ASSERT_EQ(queries.size(), 1U);
    /// The sink keeps the name that the test wrote, because the declaring pass stores a declared sink for inlining rather than
    /// registering it as a name.
    /// It resolves to nothing in the catalog, which does not matter: the statement fails to parse before any name lookup.
    EXPECT_EQ(
        queryOf(queries.at(0)).sql, "SELECT field_1 FROM TESTKEY_ONETUPLE GROUP BY field_1 WHERE field_1 > UINT64(1) INTO sinkOneTuple;");
    EXPECT_FALSE(queryOf(queries.at(0)).resultFile.has_value());
}

/// A query that infers with a model refers to it, so a model qualifies like a source.
/// The path to that file is relative to the test-data directory, and the worker loading it resolves a relative path against
/// its own working directory instead.
TEST_F(SqlRewriterTest, RewritesModelAndTheQueryThatInfersWithIt)
{
    const auto [name, qualifyingPrefix, setup, queries]
        = rewriteSlt("CREATE LOGICAL SOURCE stream(p1 FLOAT32 NOT NULL);\n"
                     "CREATE PHYSICAL SOURCE FOR stream TYPE File;\n"
                     "ATTACH FILE small/iris.csv\n"
                     "\n"
                     "CREATE MODEL iris ('model/iris.onnx')\n"
                     "INPUT (p1 FLOAT32)\n"
                     "OUTPUT (setosa FLOAT32);\n"
                     "\n"
                     "CREATE SINK result(p1 FLOAT32 NOT NULL, setosa FLOAT32 NOT NULL) TYPE File;\n"
                     "\n"
                     "SELECT * FROM MODEL_INFERENCE(iris, stream) INTO result;\n"
                     "----\n"
                     "1,1\n");

    ASSERT_EQ(setup.size(), 3U);
    EXPECT_EQ(
        sqlOf(setup.at(2)),
        "CREATE MODEL TESTKEY_IRIS ('/data/model/iris.onnx')\n"
        "INPUT (p1 FLOAT32)\n"
        "OUTPUT (setosa FLOAT32);");
    EXPECT_TRUE(std::holds_alternative<PlainStatement>(setup.at(2)));

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_TRUE(queryOf(queries.at(0)).sql.contains("MODEL_INFERENCE(TESTKEY_IRIS, TESTKEY_STREAM)"));
}

/// An ATTACH FILE source references an existing file under the test-data directory rather than materializing a new one.
TEST_F(SqlRewriterTest, RewritesFileSourceWithoutMaterializing)
{
    const auto [name, qualifyingPrefix, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                                                                     "CREATE PHYSICAL SOURCE FOR stream TYPE File;\n"
                                                                     "ATTACH FILE small/stream8.csv\n"
                                                                     "\n"
                                                                     "CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
                                                                     "\n"
                                                                     "SELECT id FROM stream INTO out;\n"
                                                                     "----\n"
                                                                     "1\n");

    ASSERT_EQ(setup.size(), 2U);
    EXPECT_EQ(
        sqlOf(setup.at(1)),
        R"(CREATE PHYSICAL SOURCE FOR TESTKEY_STREAM TYPE File SET ('/data/small/stream8.csv' AS "SOURCE"."FILE_PATH", )"
        R"('localhost:8080' AS "SOURCE"."HOST", 'CSV' AS "INPUT_FORMATTER"."TYPE");)");
    /// An ATTACH FILE source points at an existing file, so no statement holds inline data to write.
    EXPECT_TRUE(std::holds_alternative<PlainStatement>(setup.at(1)));
}

/// A source that reads from a socket gets no file path: a server sends it the attached data, and the endpoint is known only once that
/// server binds, so the rewriter leaves the statement without one and stages the rows for a server instead.
/// The options that the test set stay as the test wrote them.
TEST_F(SqlRewriterTest, StagesTheDataOfASourceThatReadsFromASocket)
{
    const auto [name, qualifyingPrefix, setup, queries]
        = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                     R"(CREATE PHYSICAL SOURCE FOR stream TYPE TCP SET('|' AS INPUT_FORMATTER.FIELD_DELIMITER);)"
                     "\n"
                     "ATTACH INLINE\n"
                     "1|19\n"
                     "\n"
                     "CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
                     "\n"
                     "SELECT id FROM stream INTO out;\n"
                     "----\n"
                     "1\n");

    ASSERT_EQ(setup.size(), 2U);
    EXPECT_EQ(
        sqlOf(setup.at(1)),
        R"(CREATE PHYSICAL SOURCE FOR TESTKEY_STREAM TYPE TCP SET ('localhost:8080' AS "SOURCE"."HOST", )"
        R"('CSV' AS "INPUT_FORMATTER"."TYPE", '|' AS INPUT_FORMATTER.FIELD_DELIMITER);)");
    const auto* served = std::get_if<StatementWithServedData>(&setup.at(1));
    ASSERT_NE(served, nullptr);
    EXPECT_EQ(std::get<std::vector<std::string>>(served->data.content), (std::vector<std::string>{"1|19"}));
}

/// A source that reads from a socket pointed at a file rather than declaring rows, so the server sends that file.
TEST_F(SqlRewriterTest, ServesTheFileOfASourceThatReadsFromASocket)
{
    const auto [name, qualifyingPrefix, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                                                                     "CREATE PHYSICAL SOURCE FOR stream TYPE TCP;\n"
                                                                     "ATTACH FILE small/stream8.csv\n"
                                                                     "\n"
                                                                     "CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
                                                                     "\n"
                                                                     "SELECT id FROM stream INTO out;\n"
                                                                     "----\n"
                                                                     "1\n");

    ASSERT_EQ(setup.size(), 2U);
    const auto* served = std::get_if<StatementWithServedData>(&setup.at(1));
    ASSERT_NE(served, nullptr);
    EXPECT_EQ(std::get<std::filesystem::path>(served->data.content), "/data/small/stream8.csv");
}

/// The endpoint that a data server bound is known only once the run is under way, so the runner merges it into the statement then,
/// alongside whatever the rewriter already put there.
TEST_F(SqlRewriterTest, AddsSourceOptionsToAnAlreadyRewrittenStatement)
{
    EXPECT_EQ(
        addSourceOptions(
            R"(CREATE PHYSICAL SOURCE FOR TESTKEY_STREAM TYPE TCP SET ('localhost:8080' AS "SOURCE"."HOST");)",
            {R"('4242' AS "SOURCE"."SOCKET_PORT")"}),
        R"(CREATE PHYSICAL SOURCE FOR TESTKEY_STREAM TYPE TCP SET ('4242' AS "SOURCE"."SOCKET_PORT", 'localhost:8080' AS "SOURCE"."HOST");)");
}

/// A sink that discards its input writes no file, so it gets neither a path nor an output format, and the query has no result file
/// to read back.
/// The rewrite lowers both sink forms that way.
TEST_F(SqlRewriterTest, GivesASinkThatDiscardsItsInputNoResultFile)
{
    const auto declared = rewriteSlt("CREATE SINK discard(id UINT64 NOT NULL) TYPE Void;\n"
                                     "\n"
                                     R"(SELECT id FROM File('small/stream8.csv' AS "SOURCE".FILE_PATH) INTO discard;)"
                                     "\n----\n");
    ASSERT_EQ(declared.cases.size(), 1U);
    EXPECT_FALSE(queryOf(declared.cases.at(0)).resultFile.has_value());
    EXPECT_TRUE(queryOf(declared.cases.at(0))
                    .sql.contains(R"(INTO Void('localhost:8080' AS "SINK"."HOST", SCHEMA(id UINT64 NOT NULL) AS "SINK"."SCHEMA"))"));

    const auto written = rewriteSlt(R"(SELECT id FROM File('small/stream8.csv' AS "SOURCE".FILE_PATH) INTO Void();)"
                                    "\n----\n");
    ASSERT_EQ(written.cases.size(), 1U);
    EXPECT_FALSE(queryOf(written.cases.at(0)).resultFile.has_value());
    EXPECT_TRUE(queryOf(written.cases.at(0)).sql.contains(R"(INTO Void('localhost:8080' AS "SINK"."HOST"))"));
}

/// Only a physical source reads attached data.
/// The test file format admits an ATTACH after any CREATE, since it matches on line prefixes, so one placed elsewhere is a mistake to
/// report rather than something to drop.
TEST_F(SqlRewriterTest, RejectsDataAttachedToAnythingButAPhysicalSource)
{
    EXPECT_THROW(
        rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                   "ATTACH FILE small/stream8.csv\n"
                   "\n"
                   "CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
                   "\n"
                   "SELECT id FROM stream INTO out;\n"
                   "----\n"
                   "1\n"),
        Exception);
}

/// A source written into the query gives its path relative to the test-data directory, so the rewrite resolves the path against it
/// and handles the rest of the source options and the sink as usual.
TEST_F(SqlRewriterTest, ResolvesTheFileOfASourceWrittenIntoTheQuery)
{
    const auto [name, qualifyingPrefix, setup, queries]
        = rewriteSlt("CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
                     "\n"
                     R"(SELECT id FROM File('small/stream8.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE") INTO out;)"
                     "\n----\n"
                     "1\n");

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_EQ(
        queryOf(queries.at(0)).sql,
        R"(SELECT id FROM File('/data/small/stream8.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE", )"
        R"('localhost:8080' AS "SOURCE"."HOST") )"
        R"(INTO File('localhost:8080' AS "SINK"."HOST", '/work/TESTKEY_1.csv' AS "SINK"."FILE_PATH", 'CSV' AS "SINK"."OUTPUT_FORMAT", )"
        R"(SCHEMA(id UINT64 NOT NULL) AS "SINK"."SCHEMA");)");
}

/// A source that is written into the query and holds its own data has no file to resolve, so it keeps the options that the test wrote.
/// It still gets the host, because the coordinator has to place it like any other source.
TEST_F(SqlRewriterTest, KeepsTheOptionsOfASelfContainedSourceWrittenIntoTheQuery)
{
    const auto [name, qualifyingPrefix, setup, queries]
        = rewriteSlt("CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
                     "\n"
                     R"(SELECT id FROM Generator('SEQUENCE UINT64 0 10 1' AS "SOURCE".GENERATOR_SCHEMA) INTO out;)"
                     "\n----\n"
                     "1\n");

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_TRUE(queryOf(queries.at(0))
                    .sql.starts_with(R"(SELECT id FROM Generator('SEQUENCE UINT64 0 10 1' AS "SOURCE".GENERATOR_SCHEMA, )"
                                     R"('localhost:8080' AS "SOURCE"."HOST", 'CSV' AS "INPUT_FORMATTER"."TYPE") INTO )"));
}

/// An absolute path already resolves the same way wherever it is read, so the rewrite leaves it unchanged.
TEST_F(SqlRewriterTest, KeepsAnAbsoluteFileOfASourceWrittenIntoTheQuery)
{
    const auto [name, qualifyingPrefix, setup, queries]
        = rewriteSlt("CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
                     "\n"
                     R"(SELECT id FROM File('/elsewhere/stream8.csv' AS "SOURCE".FILE_PATH) INTO out;)"
                     "\n----\n"
                     "1\n");

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_TRUE(queryOf(queries.at(0))
                    .sql.starts_with(R"(SELECT id FROM File('/elsewhere/stream8.csv' AS "SOURCE".FILE_PATH, )"
                                     R"('localhost:8080' AS "SOURCE"."HOST", 'CSV' AS "INPUT_FORMATTER"."TYPE") INTO )"));
}

/// A self-contained physical source draws its data from its own options rather than an attached data set.
/// The rewrite pins it to the worker, keeps its options as the test wrote them, and stages no inline data to write.
TEST_F(SqlRewriterTest, RewritesSelfContainedSourceKeepingItsOptions)
{
    const auto [name, qualifyingPrefix, setup, queries]
        = rewriteSlt("CREATE LOGICAL SOURCE gen(id UINT64 NOT NULL);\n"
                     R"(CREATE PHYSICAL SOURCE FOR gen TYPE Generator SET('SEQUENCE UINT64 0 10 1' AS "SOURCE".GENERATOR_SCHEMA);)"
                     "\n\n"
                     "CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
                     "\n"
                     "SELECT id FROM gen INTO out;\n"
                     "----\n"
                     "1\n");

    ASSERT_EQ(setup.size(), 2U);
    EXPECT_EQ(
        sqlOf(setup.at(1)),
        R"(CREATE PHYSICAL SOURCE FOR TESTKEY_GEN TYPE Generator SET ('localhost:8080' AS "SOURCE"."HOST", )"
        R"('CSV' AS "INPUT_FORMATTER"."TYPE", 'SEQUENCE UINT64 0 10 1' AS "SOURCE".GENERATOR_SCHEMA);)");
    EXPECT_TRUE(std::holds_alternative<PlainStatement>(setup.at(1)));
}

/// A physical source that chose its own host keeps it, and the rewriter injects no second one.
TEST_F(SqlRewriterTest, KeepsTheHostAPhysicalSourceChose)
{
    const auto [name, qualifyingPrefix, setup, queries]
        = rewriteSlt("CREATE LOGICAL SOURCE gen(id UINT64 NOT NULL);\n"
                     R"(CREATE PHYSICAL SOURCE FOR gen TYPE Generator SET('SEQUENCE UINT64 0 10 1' AS "SOURCE".GENERATOR_SCHEMA, )"
                     R"('elsewhere:9999' AS "SOURCE"."HOST");)"
                     "\n\n"
                     "CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
                     "\n"
                     "SELECT id FROM gen INTO out;\n"
                     "----\n"
                     "1\n");

    ASSERT_EQ(setup.size(), 2U);
    EXPECT_TRUE(sqlOf(setup.at(1)).contains(R"('elsewhere:9999' AS "SOURCE"."HOST")"));
    EXPECT_FALSE(sqlOf(setup.at(1)).contains(R"('localhost:8080' AS "SOURCE"."HOST")"));
}

/// A sink written into the query that chose its own host keeps it, and the rewriter injects no second one.
TEST_F(SqlRewriterTest, KeepsTheHostASinkWrittenIntoTheQueryChose)
{
    const auto [name, qualifyingPrefix, setup, queries]
        = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                     "\n"
                     R"(SELECT id FROM stream INTO Void('elsewhere:9999' AS "SINK"."HOST");)"
                     "\n----\n"
                     "1\n");

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_TRUE(queryOf(queries.at(0)).sql.contains(R"('elsewhere:9999' AS "SINK"."HOST")"));
    EXPECT_FALSE(queryOf(queries.at(0)).sql.contains(R"('localhost:8080' AS "SINK"."HOST")"));
}

/// The checker reads the result file the rewriter chose, so a sink picking its own would succeed or fail against a file nobody reads.
TEST_F(SqlRewriterTest, RejectsASinkThatChoosesItsResultFile)
{
    EXPECT_THROW(
        rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                   "\n"
                   R"(SELECT id FROM stream INTO File('/elsewhere/out.csv' AS "SINK"."FILE_PATH");)"
                   "\n----\n"
                   "1\n"),
        Exception);
}

/// A checksum sink quotes its strings, because the expected checksums were computed over quoted strings.
TEST_F(SqlRewriterTest, ChecksumSinkGetsQuotedStrings)
{
    const auto [name, qualifyingPrefix, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                                                                     "\n"
                                                                     "SELECT id FROM stream INTO Checksum();\n"
                                                                     "----\n"
                                                                     "1\n");

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_TRUE(queryOf(queries.at(0)).sql.contains(R"('true' AS "OUTPUT_FORMATTER"."QUOTE_STRINGS")"));
}

/// A checksum sink that chose its own quoting keeps it, so the rewriter injects no second value.
TEST_F(SqlRewriterTest, ChecksumSinkKeepsItsOwnQuotingChoice)
{
    const auto [name, qualifyingPrefix, setup, queries]
        = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                     "\n"
                     "SELECT id FROM stream INTO Checksum('false' AS \"OUTPUT_FORMATTER\".\"QUOTE_STRINGS\");\n"
                     "----\n"
                     "1\n");

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_TRUE(queryOf(queries.at(0)).sql.contains(R"('false' AS "OUTPUT_FORMATTER"."QUOTE_STRINGS")"));
    EXPECT_FALSE(queryOf(queries.at(0)).sql.contains(R"('true' AS "OUTPUT_FORMATTER"."QUOTE_STRINGS")"));
}

/// The explained query binds and optimizes as a regular query does, so a source written into it gets the same completion:
/// an absolute data path, the run's source worker, and the CSV input format default.
TEST_F(SqlRewriterTest, ExplainCompletesASourceWrittenIntoTheQuery)
{
    const auto [name, qualifyingPrefix, setup, queries]
        = rewriteSlt("EXPLAIN (OPTIMIZED) SELECT id FROM File('small/stream8.csv' AS \"SOURCE\".FILE_PATH) "
                     "INTO Void('true' AS \"SINK\".\"NOOP\");\n"
                     "----\n"
                     "== Optimized Plan ==\n");

    ASSERT_EQ(queries.size(), 1U);
    const auto* explain = std::get_if<RewrittenExplain>(&queries.at(0).action);
    ASSERT_NE(explain, nullptr);
    EXPECT_TRUE(explain->sql.contains(R"('/data/small/stream8.csv' AS "SOURCE".FILE_PATH)"));
    EXPECT_TRUE(explain->sql.contains(R"('localhost:8080' AS "SOURCE"."HOST")"));
    EXPECT_TRUE(explain->sql.contains(R"('CSV' AS "INPUT_FORMATTER"."TYPE")"));
}

/// The query of an EXPLAIN can refer to a declared sink, and the plan prints that sink's name, so the sink has to exist in the catalog.
/// The rewriter submits the declaration with its mandatory options spliced in, and the reference keeps the qualified name rather
/// than an inlined sink.
TEST_F(SqlRewriterTest, ExplainKeepsTheDeclaredSinkAndSubmitsItsDeclaration)
{
    const auto [name, qualifyingPrefix, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                                                                     "CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
                                                                     "\n"
                                                                     "EXPLAIN (OPTIMIZED) SELECT id FROM stream INTO out;\n"
                                                                     "----\n"
                                                                     "== Optimized Plan ==\n");

    ASSERT_EQ(setup.size(), 2U);
    EXPECT_EQ(
        sqlOf(setup.at(1)),
        R"(CREATE SINK TESTKEY_OUT(id UINT64 NOT NULL) TYPE File )"
        R"(SET ('localhost:8080' AS "SINK"."HOST", '/work/TESTKEY_out.csv' AS "SINK"."FILE_PATH", 'CSV' AS "SINK"."OUTPUT_FORMAT");)");

    ASSERT_EQ(queries.size(), 1U);
    const auto* explain = std::get_if<RewrittenExplain>(&queries.at(0).action);
    ASSERT_NE(explain, nullptr);
    EXPECT_EQ(explain->sql, "EXPLAIN (OPTIMIZED) SELECT id FROM TESTKEY_STREAM INTO TESTKEY_OUT;");
    EXPECT_EQ(qualifyingPrefix, "TESTKEY_");
}

/// An EXPLAIN whose query writes its sink inline needs no declaration: the rewriter inlines the sink as it does for a query
/// and submits nothing for it.
TEST_F(SqlRewriterTest, ExplainInlinesTheSinkThatItsQueryWrites)
{
    const auto [name, qualifyingPrefix, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                                                                     "\n"
                                                                     "EXPLAIN (OPTIMIZED) SELECT id FROM stream "
                                                                     "INTO Void('true' AS \"SINK\".\"NOOP\");\n"
                                                                     "----\n"
                                                                     "== Optimized Plan ==\n");

    ASSERT_EQ(setup.size(), 1U);
    ASSERT_EQ(queries.size(), 1U);
    const auto* explain = std::get_if<RewrittenExplain>(&queries.at(0).action);
    ASSERT_NE(explain, nullptr);
    EXPECT_TRUE(explain->sql.contains("INTO Void("));
    EXPECT_TRUE(explain->sql.contains(R"('localhost:8080' AS "SINK"."HOST")"));
}

/// Pins a limitation of the file-global sink mode: an EXPLAIN registers the declared sink names, so the rewrite qualifies the
/// reference in an executed query before the sink lookup, whose key is the written name.
/// The rewriter rejects such a file rather than rewriting it wrongly.
/// Lifting this needs the lookup to resolve the qualified spelling back to the written one.
TEST_F(SqlRewriterTest, RejectsAFileThatMixesAnExplainWithAQueryIntoADeclaredSink)
{
    EXPECT_THROW(
        rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                   "CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
                   "\n"
                   "EXPLAIN (OPTIMIZED) SELECT id FROM stream INTO out;\n"
                   "----\n"
                   "== Optimized Plan ==\n"
                   "==END==\n"
                   "\n"
                   "SELECT id FROM stream INTO out;\n"
                   "----\n"
                   "1\n"),
        Exception);
}

}
