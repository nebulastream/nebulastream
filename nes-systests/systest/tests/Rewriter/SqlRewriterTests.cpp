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

#include <TokenStreamRewriter.h>
#include <gtest/gtest.h>

#include <Identifiers/Identifiers.hpp>
#include <Model/Expectation.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Parser/SystestParser.hpp>
#include <Parser/TestFileBuilder.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Rewriter/RewriteTarget.hpp>
#include <Rewriter/SourceRewriting.hpp>
#include <Rewriter/SqlParse.hpp>
#include <Rewriter/SqlRewriter.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

class QualifyNamesTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("QualifyNames.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup QualifyNames test class.");
    }

    static void TearDownTestSuite() { NES_INFO("Tear down QualifyNames test class."); }

    /// Registers the given catalog names under one key and seals them, mirroring the declaring pass that precedes a rewrite.
    static QualifiedNames declareAll(const std::initializer_list<std::string_view> declared)
    {
        NameRegistry registry{DiscoveryRoot{"/tests"}.keyOf("/tests/benchmark/Nexmark.test", 0, 1)};
        for (const auto name : declared)
        {
            registry.declare(name);
        }
        return std::move(registry).seal();
    }

    /// Parses a statement and qualifies it on its own, which is what every rewriting pass does alongside its own edits.
    static std::string qualify(const std::string& sql, const QualifiedNames& names)
    {
        SqlParse parse{sql};
        antlr4::TokenStreamRewriter rewriter{&parse.tokenStream()};
        qualifyNames(parse, rewriter, names);
        return rewriter.getText();
    }
};

/// The rewrite substitutes a registered name and preserves the surrounding keywords, punctuation and whitespace exactly.
TEST_F(QualifyNamesTest, SubstitutesRegisteredNames)
{
    const auto names = declareAll({"stream", "bid"});
    EXPECT_EQ(qualify("SELECT bid FROM stream", names), "SELECT BENCHMARK_D_NEXMARK_BID FROM BENCHMARK_D_NEXMARK_STREAM");
}

/// Unquoted names are compared case-insensitively, so the token matches its registration either way.
TEST_F(QualifyNamesTest, MatchesRegisteredNameRegardlessOfCase)
{
    const auto names = declareAll({"stream"});
    EXPECT_EQ(qualify("SELECT x FROM STREAM", names), "SELECT x FROM BENCHMARK_D_NEXMARK_STREAM");
}

/// The rewrite leaves an identifier that the qualifier never registered unchanged.
TEST_F(QualifyNamesTest, LeavesUnregisteredIdentifiersAlone)
{
    const auto names = declareAll({"stream"});
    EXPECT_EQ(qualify("SELECT unknown FROM stream", names), "SELECT unknown FROM BENCHMARK_D_NEXMARK_STREAM");
}

/// An identifier that only contains a registered name as a substring is a distinct token, rewrite leaves it unchanged.
TEST_F(QualifyNamesTest, DoesNotRewriteIdentifierContainingRegisteredName)
{
    const auto names = declareAll({"stream"});
    EXPECT_EQ(qualify("SELECT x FROM streamSink", names), "SELECT x FROM streamSink");
    EXPECT_EQ(qualify("SELECT x FROM sinkStream", names), "SELECT x FROM sinkStream");
}

/// A registered name that appears inside a string literal is not an identifier token, rewrite leaves it unchanged.
TEST_F(QualifyNamesTest, DoesNotRewriteInsideStringLiterals)
{
    const auto names = declareAll({"stream"});
    EXPECT_EQ(
        qualify(R"(SELECT x FROM stream INTO File('/data/stream/x.csv' AS "SINK".FILE_PATH))", names),
        R"(SELECT x FROM BENCHMARK_D_NEXMARK_STREAM INTO File('/data/stream/x.csv' AS "SINK".FILE_PATH))");
}

/// Rewriting is a fixed point: the qualified spelling is not itself registered, so a second pass is idempotent.
TEST_F(QualifyNamesTest, IsIdempotent)
{
    const auto names = declareAll({"stream", "bid"});
    const auto once = qualify("SELECT bid FROM stream", names);
    EXPECT_EQ(qualify(once, names), once);
}

/// The grammar spells a plugin type as a plain identifier, so a file declaring a source named after a plugin would
/// otherwise have its types substituted too, and the statement would refer to a source or sink type that does not exist.
TEST_F(QualifyNamesTest, PreservesAPluginTypeThatMatchesADeclaredName)
{
    const auto names = declareAll({"File"});
    EXPECT_EQ(qualify("CREATE PHYSICAL SOURCE FOR File TYPE File", names), "CREATE PHYSICAL SOURCE FOR BENCHMARK_D_NEXMARK_FILE TYPE File");
    EXPECT_EQ(
        qualify("CREATE SINK File(field_1 UINT64 NOT NULL) TYPE File", names),
        "CREATE SINK BENCHMARK_D_NEXMARK_FILE(field_1 UINT64 NOT NULL) TYPE File");
}

/// A source or sink written into a query spells its type as a plain identifier too, and no keyword precedes it there.
TEST_F(QualifyNamesTest, PreservesThePluginTypeOfASourceOrSinkWrittenIntoAQuery)
{
    const auto names = declareAll({"File"});
    EXPECT_EQ(
        qualify(R"(SELECT x FROM File INTO File('out.csv' AS "SINK".FILE_PATH))", names),
        R"(SELECT x FROM BENCHMARK_D_NEXMARK_FILE INTO File('out.csv' AS "SINK".FILE_PATH))");
}

/// The grammar admits any text between quotes, so a query can hold a token that no identifier can hold.
/// Such a token was never registered, so the rewrite copies it and leaves the query for the parser to reject.
TEST_F(QualifyNamesTest, LeavesATokenThatIsNotALegalIdentifierAlone)
{
    const auto names = declareAll({"stream"});
    EXPECT_EQ(qualify(R"(SELECT "" FROM stream)", names), R"(SELECT "" FROM BENCHMARK_D_NEXMARK_STREAM)");
    EXPECT_EQ(qualify(R"(SELECT "a.b" FROM stream)", names), R"(SELECT "a.b" FROM BENCHMARK_D_NEXMARK_STREAM)");
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

    static RunnableTestFile rewriteSlt(const std::string& slt)
    {
        SystestParser parser;
        parser.loadString(slt);
        const std::filesystem::path testFilePath{"/tests/testkey.test"};
        return rewriteTestFile(
            buildTestFile(parser, testFilePath),
            RewriteTarget{
                .testFileKey = DiscoveryRoot{"/tests"}.keyOf(testFilePath, 0, 1),
                .displayName = "testkey",
                .workingDir = "/work",
                .testDataDir = "/data",
                .sourceHost = Host{"localhost:8080"},
                .sinkHost = Host{"localhost:8080"}});
    }
};

/// A differential block asserts that its two queries agree, so it becomes one case with a verdict.
/// The two queries may be submitted concurrently.
/// The physical source declaration is missing from a working version, but the rewriter does not care at this point.
TEST_F(SqlRewriterTest, MergesADifferentialBlockIntoOneCase)
{
    const auto [name, qualifyingPrefix, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
                                                                     "CREATE SINK sinkOneTuple(field_1 UINT64 NOT NULL) TYPE File;\n"
                                                                     "\n"
                                                                     "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                                                                     "====\n"
                                                                     "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n");

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_TRUE(std::holds_alternative<RewrittenDifferential>(queries.at(0).action));
}

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
        std::get<RewrittenQuery>(queries.at(0).action).sql,
        R"(SELECT field_1 FROM TESTKEY_ONETUPLE INTO File('localhost:8080' AS "SINK"."HOST", '/work/TESTKEY_1.csv' AS "SINK"."FILE_PATH", )"
        R"('CSV' AS "SINK"."OUTPUT_FORMAT", SCHEMA(field_1 UINT64 NOT NULL) AS "SINK"."SCHEMA");)");
    EXPECT_EQ(std::get<RewrittenQuery>(queries.at(0).action).resultFile, "/work/TESTKEY_1.csv");
    const auto* expectedRows = std::get_if<ExpectedRows>(&std::get<RewrittenQuery>(queries.at(0).action).expectation);
    ASSERT_NE(expectedRows, nullptr);
    EXPECT_EQ(expectedRows->rows, (std::vector<std::string>{"1"}));

    EXPECT_EQ(qualifyingPrefix, "TESTKEY_");
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
    ASSERT_EQ(queries.size(), 1U);
    EXPECT_TRUE(std::get<RewrittenQuery>(queries.at(0).action).sql.contains("FROM TESTKEY_ONETUPLE"));
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
    EXPECT_TRUE(std::get<RewrittenQuery>(queries.at(0).action).sql.contains(R"(SCHEMA(field_1 UINT64 NOT NULL) AS "SINK"."SCHEMA")"));
}

/// A test that asserts a syntax error writes a query the parser rejects, and nothing can be inlined into a statement with no parse tree.
/// It is submitted as it stands, so the error is reported against that query rather than the rewrite failing
/// every query of the file.
/// Its names stay as the test wrote them, because qualifying reads the parse tree that this statement does not have.
/// The statement fails to parse before any name is looked up, so the unqualified names change nothing.
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
    EXPECT_EQ(
        std::get<RewrittenQuery>(queries.at(0).action).sql,
        "SELECT field_1 FROM oneTuple GROUP BY field_1 WHERE field_1 > UINT64(1) INTO sinkOneTuple;");
    EXPECT_FALSE(std::get<RewrittenQuery>(queries.at(0).action).resultFile.has_value());
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
    EXPECT_TRUE(std::get<RewrittenQuery>(queries.at(0).action).sql.contains("MODEL_INFERENCE(TESTKEY_IRIS, TESTKEY_STREAM)"));
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
    EXPECT_FALSE(std::get<RewrittenQuery>(declared.cases.at(0).action).resultFile.has_value());
    EXPECT_TRUE(std::get<RewrittenQuery>(declared.cases.at(0).action)
                    .sql.contains(R"(INTO Void('localhost:8080' AS "SINK"."HOST", SCHEMA(id UINT64 NOT NULL) AS "SINK"."SCHEMA"))"));

    const auto written = rewriteSlt(R"(SELECT id FROM File('small/stream8.csv' AS "SOURCE".FILE_PATH) INTO Void();)"
                                    "\n----\n");
    ASSERT_EQ(written.cases.size(), 1U);
    EXPECT_FALSE(std::get<RewrittenQuery>(written.cases.at(0).action).resultFile.has_value());
    EXPECT_TRUE(std::get<RewrittenQuery>(written.cases.at(0).action).sql.contains(R"(INTO Void('localhost:8080' AS "SINK"."HOST"))"));
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
        std::get<RewrittenQuery>(queries.at(0).action).sql,
        R"(SELECT id FROM File('/data/small/stream8.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE", )"
        R"('localhost:8080' AS "SOURCE"."HOST") )"
        R"(INTO File('localhost:8080' AS "SINK"."HOST", '/work/TESTKEY_1.csv' AS "SINK"."FILE_PATH", 'CSV' AS "SINK"."OUTPUT_FORMAT", )"
        R"(SCHEMA(id UINT64 NOT NULL) AS "SINK"."SCHEMA");)");
}

/// A source that is written into the query and holds its own data has no file to resolve, so it keeps the options that the test wrote.
/// It still gets the host, because placing it needs one like it does for any other source.
TEST_F(SqlRewriterTest, KeepsTheOptionsOfASelfContainedSourceWrittenIntoTheQuery)
{
    const auto [name, qualifyingPrefix, setup, queries]
        = rewriteSlt("CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
                     "\n"
                     R"(SELECT id FROM Generator('SEQUENCE UINT64 0 10 1' AS "SOURCE".GENERATOR_SCHEMA) INTO out;)"
                     "\n----\n"
                     "1\n");

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_TRUE(std::get<RewrittenQuery>(queries.at(0).action).sql.contains(R"('SEQUENCE UINT64 0 10 1' AS "SOURCE".GENERATOR_SCHEMA)"));
    EXPECT_TRUE(std::get<RewrittenQuery>(queries.at(0).action).sql.contains(R"('localhost:8080' AS "SOURCE"."HOST")"));
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
    EXPECT_TRUE(std::get<RewrittenQuery>(queries.at(0).action).sql.contains(R"('/elsewhere/stream8.csv' AS "SOURCE".FILE_PATH)"));
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
    EXPECT_TRUE(std::get<RewrittenQuery>(queries.at(0).action).sql.contains(R"('elsewhere:9999' AS "SINK"."HOST")"));
    EXPECT_FALSE(std::get<RewrittenQuery>(queries.at(0).action).sql.contains(R"('localhost:8080' AS "SINK"."HOST")"));
}

/// A physical source reads the data that its ATTACH names, and the rewrite resolves that path against the test data directory.
/// A path written into the SET clause would pass through as the test wrote it, so a relative one would miss the file, and one
/// written alongside an ATTACH would set the option twice.
TEST_F(SqlRewriterTest, RejectsAPhysicalSourceThatChoosesItsDataFile)
{
    EXPECT_THROW(
        rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                   R"(CREATE PHYSICAL SOURCE FOR stream TYPE File SET('small/stream8.csv' AS "SOURCE"."FILE_PATH");)"
                   "\n\n"
                   "CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
                   "\n"
                   "SELECT id FROM stream INTO out;\n"
                   "----\n"
                   "1\n"),
        Exception);
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
    EXPECT_TRUE(std::get<RewrittenQuery>(queries.at(0).action).sql.contains(R"('true' AS "OUTPUT_FORMATTER"."QUOTE_STRINGS")"));
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
    EXPECT_TRUE(std::get<RewrittenQuery>(queries.at(0).action).sql.contains(R"('false' AS "OUTPUT_FORMATTER"."QUOTE_STRINGS")"));
    EXPECT_FALSE(std::get<RewrittenQuery>(queries.at(0).action).sql.contains(R"('true' AS "OUTPUT_FORMATTER"."QUOTE_STRINGS")"));
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
}

/// Only File and Checksum take a file path, so only they can be pointed at the file that the checker reads.
/// A Print or MQTT sink would reject that path and report a parameter the test never wrote, so the rewriter says so instead.
TEST_F(SqlRewriterTest, RejectsASinkWhoseResultNoTestCanCheck)
{
    EXPECT_THROW(
        rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                   "\n"
                   "SELECT id FROM stream INTO Print();\n"
                   "----\n"
                   "1\n"),
        Exception);
}

/// A sink type this rewriter does not know passes through, so the engine rejects it and a test can expect that error,
/// rather than the whole file failing to load.
TEST_F(SqlRewriterTest, PassesAnUnknownSinkTypeThroughForTheEngineToReject)
{
    const auto [name, qualifyingPrefix, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                                                                     "\n"
                                                                     "SELECT id FROM stream INTO FileSink();\n"
                                                                     "----\n"
                                                                     "ERROR 1000\n");

    ASSERT_EQ(queries.size(), 1U);
    const auto& query = std::get<RewrittenQuery>(queries.at(0).action);
    EXPECT_FALSE(query.resultFile.has_value());
    EXPECT_TRUE(query.sql.contains("INTO FileSink("));
}

/// A test picks a Void sink deliberately, to run a query whose output it does not check, so it stays legal and writes no
/// result file.
TEST_F(SqlRewriterTest, KeepsAVoidSinkThatWritesNoResult)
{
    const auto [name, qualifyingPrefix, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                                                                     "\n"
                                                                     "SELECT id FROM stream INTO Void();\n"
                                                                     "----\n");

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_FALSE(std::get<RewrittenQuery>(queries.at(0).action).resultFile.has_value());
}

/// The grammar allows one SET clause per sink, so the options that the rewriter adds merge into the clause that the test
/// wrote instead of following it as a second clause, which would not parse.
TEST_F(SqlRewriterTest, MergesTheRewrittenSinkOptionsIntoTheOnesTheTestWrote)
{
    const auto [name, qualifyingPrefix, setup, queries]
        = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                     "CREATE SINK out(id UINT64 NOT NULL) TYPE File SET ('JSON' AS \"SINK\".\"OUTPUT_FORMAT\");\n"
                     "\n"
                     "EXPLAIN (OPTIMIZED) SELECT id FROM stream INTO out;\n"
                     "----\n"
                     "== Optimized Plan ==\n");

    ASSERT_EQ(setup.size(), 2U);
    /// The format that the test chose survives, and the rewriter adds no second one.
    EXPECT_EQ(
        sqlOf(setup.at(1)),
        R"(CREATE SINK TESTKEY_OUT(id UINT64 NOT NULL) TYPE File )"
        R"(SET ('localhost:8080' AS "SINK"."HOST", '/work/TESTKEY_out.csv' AS "SINK"."FILE_PATH", 'JSON' AS "SINK"."OUTPUT_FORMAT");)");
}

/// The checker reads the file that the rewriter chose, so a sink naming its own result file would write where nothing looks.
/// A sink written into a query is rejected for the same reason.
TEST_F(SqlRewriterTest, RejectsADeclaredSinkThatChoosesItsResultFile)
{
    EXPECT_THROW(
        rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                   "CREATE SINK out(id UINT64 NOT NULL) TYPE File SET ('/tmp/mine.csv' AS \"SINK\".\"FILE_PATH\");\n"
                   "\n"
                   "EXPLAIN (OPTIMIZED) SELECT id FROM stream INTO out;\n"
                   "----\n"
                   "== Optimized Plan ==\n"),
        Exception);
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

/// An EXPLAIN registers the declared sink names, so a file may hold both an EXPLAIN and a query writing into the same
/// declared sink.
/// The EXPLAIN keeps the sink by its qualified name, so the printed plan names it, while the query inlines it, and the
/// lookup finds it because qualifying reads the parse of the test's own text and leaves the written name in the tree.
TEST_F(SqlRewriterTest, MixesAnExplainWithAQueryIntoTheSameDeclaredSink)
{
    const auto [name, qualifyingPrefix, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                                                                     "CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
                                                                     "\n"
                                                                     "EXPLAIN (OPTIMIZED) SELECT id FROM stream INTO out;\n"
                                                                     "----\n"
                                                                     "== Optimized Plan ==\n"
                                                                     "==END==\n"
                                                                     "\n"
                                                                     "SELECT id FROM stream INTO out;\n"
                                                                     "----\n"
                                                                     "1\n");

    ASSERT_EQ(queries.size(), 2U);
    const auto* explain = std::get_if<RewrittenExplain>(&queries.at(0).action);
    ASSERT_NE(explain, nullptr);
    EXPECT_EQ(explain->sql, "EXPLAIN (OPTIMIZED) SELECT id FROM TESTKEY_STREAM INTO TESTKEY_OUT;");

    const auto* query = std::get_if<RewrittenQuery>(&queries.at(1).action);
    ASSERT_NE(query, nullptr);
    EXPECT_TRUE(query->sql.starts_with("SELECT id FROM TESTKEY_STREAM INTO File("));
}

}
