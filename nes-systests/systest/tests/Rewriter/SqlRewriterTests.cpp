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
#include <Model/RunnableTest.hpp>
#include <Parser/SystestParser.hpp>
#include <Parser/TestFileParser.hpp>
#include <Rewriter/NameQualifier.hpp>
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

/// A registered name is substituted and the surrounding keywords, punctuation and whitespace are preserved exactly.
TEST_F(RewriteIdentifiersTest, SubstitutesRegisteredNames)
{
    const auto names = namesWith({"stream", "bid"});
    EXPECT_EQ(rewriteIdentifiers("SELECT bid FROM stream", names), "SELECT BENCHMARK_NEXMARK_BID FROM BENCHMARK_NEXMARK_STREAM");
}

/// The catalog folds an unquoted name, so the token matches its registration however it is cased.
TEST_F(RewriteIdentifiersTest, MatchesRegisteredNameRegardlessOfCase)
{
    const auto names = namesWith({"stream"});
    EXPECT_EQ(rewriteIdentifiers("SELECT x FROM STREAM", names), "SELECT x FROM BENCHMARK_NEXMARK_STREAM");
}

/// An identifier the qualifier never registered is left untouched, so column names and aliases pass through.
TEST_F(RewriteIdentifiersTest, LeavesUnregisteredIdentifiersAlone)
{
    const auto names = namesWith({"stream"});
    EXPECT_EQ(rewriteIdentifiers("SELECT unknown FROM stream", names), "SELECT unknown FROM BENCHMARK_NEXMARK_STREAM");
}

/// An identifier that merely contains a registered name is a distinct token, so a substring must not be rewritten.
TEST_F(RewriteIdentifiersTest, DoesNotRewriteIdentifierContainingRegisteredName)
{
    const auto names = namesWith({"stream"});
    EXPECT_EQ(rewriteIdentifiers("SELECT x FROM streamSink", names), "SELECT x FROM streamSink");
    EXPECT_EQ(rewriteIdentifiers("SELECT x FROM sinkStream", names), "SELECT x FROM sinkStream");
}

/// A registered name that appears inside a string literal is not an identifier token, so the literal is left byte for byte.
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

    static RunnableTest rewriteSlt(const std::string& slt)
    {
        SystestParser parser;
        parser.loadString(slt);
        const auto testFile = parseTestFile(parser, "/tests/OneTuple.test");
        SqlRewriter rewriter{RewriteTarget{
            .testFileKey = "TESTKEY",
            .displayName = "testkey",
            .workingDir = "/work",
            .testDataDir = "/data",
            .sourceHost = Host{"localhost:8080"},
            .sinkHost = Host{"localhost:8080"}}};
        return rewriter.rewrite(testFile);
    }

    /// Unwraps the single-query action of a case, throwing when the case holds a differential block instead.
    static const RunnableQuery& queryOf(const RunnableCase& testCase) { return std::get<RunnableQuery>(testCase.action); }
};

/// A file marked `SEQUENTIAL_EXECUTION` runs its queries in order, so every query of it waits for the one above.
/// The first query has nothing above it, and the runner starts it whatever the flag says.
TEST_F(SqlRewriterTest, MarksEveryQueryOfASequentialFileAsFollowingTheOneAbove)
{
    const auto [name, variant, setup, queries] = rewriteSlt("SEQUENTIAL_EXECUTION\n"
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
    const auto [name, variant, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
                                                            "CREATE SINK sinkOneTuple(field_1 UINT64 NOT NULL) TYPE File;\n"
                                                            "\n"
                                                            "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                                                            "====\n"
                                                            "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n");

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_FALSE(queries.at(0).runsAfterPrevious);
    EXPECT_TRUE(std::holds_alternative<RunnableDifferential>(queries.at(0).action));
}

/// The plain path: an inline source, a declared File sink, and one query rewrite to qualified DDL, a configured physical source, and an
/// inlined per-query sink with a stamped query name.
TEST_F(SqlRewriterTest, RewritesInlineSourceNamedSinkAndQuery)
{
    const auto [name, variant, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
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
    EXPECT_EQ(setup.at(0).sql, "CREATE LOGICAL SOURCE TESTKEY_ONETUPLE(field_1 UINT64 NOT NULL);");
    EXPECT_FALSE(setup.at(0).staged.has_value());

    EXPECT_EQ(
        setup.at(1).sql,
        R"(CREATE PHYSICAL SOURCE FOR TESTKEY_ONETUPLE TYPE File SET ('/work/sources/TESTKEY_0.csv' AS "SOURCE"."FILE_PATH", )"
        R"('localhost:8080' AS "SOURCE"."HOST", 'CSV' AS "INPUT_FORMATTER"."TYPE");)");
    ASSERT_TRUE(setup.at(1).staged.has_value());
    const auto* inlineData = std::get_if<InlineData>(&setup.at(1).staged.value());
    ASSERT_NE(inlineData, nullptr);
    EXPECT_EQ(inlineData->path, "/work/sources/TESTKEY_0.csv");
    EXPECT_EQ(inlineData->rows, (std::vector<std::string>{"1"}));

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_EQ(
        queryOf(queries.at(0)).sql,
        R"(SELECT field_1 FROM TESTKEY_ONETUPLE INTO File('localhost:8080' AS "SINK"."HOST", '/work/TESTKEY_1.csv' AS "SINK"."FILE_PATH", )"
        R"('CSV' AS "SINK"."OUTPUT_FORMAT", SCHEMA(field_1 UINT64 NOT NULL) AS "SINK"."SCHEMA") SET ('TESTKEY:1' AS "QUERY"."NAME");)");
    EXPECT_EQ(queryOf(queries.at(0)).resultFile, "/work/TESTKEY_1.csv");
    const auto* expectedRows = std::get_if<ExpectedRows>(&queryOf(queries.at(0)).expectation);
    ASSERT_NE(expectedRows, nullptr);
    EXPECT_EQ(expectedRows->rows, ExpectedResult{"1"});
}

/// The two halves of a differential block share one query number, and one result file between them would compare a
/// result against itself and pass whatever either query returned.
TEST_F(SqlRewriterTest, GivesEachHalfOfADifferentialBlockItsOwnResultFile)
{
    const auto [name, variant, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
                                                            "CREATE SINK sinkOneTuple(field_1 UINT64 NOT NULL) TYPE File;\n"
                                                            "\n"
                                                            "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                                                            "====\n"
                                                            "SELECT field_1 FROM oneTuple WHERE field_1 > 0 INTO sinkOneTuple;\n");

    ASSERT_EQ(queries.size(), 1U);
    const auto* differential = std::get_if<RunnableDifferential>(&queries.at(0).action);
    ASSERT_NE(differential, nullptr);
    EXPECT_NE(differential->firstResultFile, differential->secondResultFile);
    EXPECT_NE(differential->firstSql, differential->secondSql);
}

/// Every name is registered before any statement is rewritten, so a source declared below the query that reads it still qualifies.
/// Registering as each statement was rewritten would leave this reference raw and the query would bind against nothing.
TEST_F(SqlRewriterTest, QualifiesSourceDeclaredBelowTheQuery)
{
    const auto [name, variant, setup, queries] = rewriteSlt("CREATE SINK sinkOneTuple(field_1 UINT64 NOT NULL) TYPE File;\n"
                                                            "\n"
                                                            "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                                                            "----\n"
                                                            "1\n"
                                                            "\n"
                                                            "CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n");

    ASSERT_EQ(setup.size(), 1U);
    EXPECT_EQ(setup.at(0).sql, "CREATE LOGICAL SOURCE TESTKEY_ONETUPLE(field_1 UINT64 NOT NULL);");

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_EQ(
        queryOf(queries.at(0)).sql,
        R"(SELECT field_1 FROM TESTKEY_ONETUPLE INTO File('localhost:8080' AS "SINK"."HOST", '/work/TESTKEY_1.csv' AS "SINK"."FILE_PATH", )"
        R"('CSV' AS "SINK"."OUTPUT_FORMAT", SCHEMA(field_1 UINT64 NOT NULL) AS "SINK"."SCHEMA") SET ('TESTKEY:1' AS "QUERY"."NAME");)");
}

/// Sinks are captured in the same pass that registers names, so a sink declared below the query that writes to it is still inlined
/// with the schema it declares.
TEST_F(SqlRewriterTest, InlinesSinkDeclaredBelowTheQuery)
{
    const auto [name, variant, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
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
        R"('CSV' AS "SINK"."OUTPUT_FORMAT", SCHEMA(field_1 UINT64 NOT NULL) AS "SINK"."SCHEMA") SET ('TESTKEY:1' AS "QUERY"."NAME");)");
    EXPECT_EQ(queryOf(queries.at(0)).resultFile, "/work/TESTKEY_1.csv");
}

/// A test that asserts a syntax error writes a query the parser rejects, and nothing can be inlined into a statement with no parse tree.
/// It is submitted as it stands, so the coordinator reports the error against that query rather than the rewrite failing every query of the
/// file.
/// Its source references are still qualified, because substituting names reads tokens.
TEST_F(SqlRewriterTest, PassesAQueryThatDoesNotParseThrough)
{
    const auto [name, variant, setup, queries]
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
    /// The sink keeps the name the test wrote, because a declared sink is captured for inlining rather than registered as a name.
    /// It resolves to nothing in the catalog, which does not matter: the statement fails to parse before any name is looked up.
    EXPECT_EQ(
        queryOf(queries.at(0)).sql, "SELECT field_1 FROM TESTKEY_ONETUPLE GROUP BY field_1 WHERE field_1 > UINT64(1) INTO sinkOneTuple;");
    EXPECT_FALSE(queryOf(queries.at(0)).resultFile.has_value());
}

/// A query that infers with a model refers to it, so a model is qualified like a source.
/// The path to that file is relative to the test-data directory, and the worker loading it resolves a relative path against
/// its own working directory instead.
TEST_F(SqlRewriterTest, RewritesModelAndTheQueryThatInfersWithIt)
{
    const auto [name, variant, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE stream(p1 FLOAT32 NOT NULL);\n"
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
        setup.at(2).sql,
        "CREATE MODEL TESTKEY_IRIS ('/data/model/iris.onnx')\n"
        "INPUT (p1 FLOAT32)\n"
        "OUTPUT (setosa FLOAT32);");
    EXPECT_FALSE(setup.at(2).staged.has_value());

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_TRUE(queryOf(queries.at(0)).sql.contains("MODEL_INFERENCE(TESTKEY_IRIS, TESTKEY_STREAM)"));
}

/// An ATTACH FILE source references an existing file under the test-data directory rather than materializing a new one.
TEST_F(SqlRewriterTest, RewritesFileSourceWithoutMaterializing)
{
    const auto [name, variant, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
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
        setup.at(1).sql,
        R"(CREATE PHYSICAL SOURCE FOR TESTKEY_STREAM TYPE File SET ('/data/small/stream8.csv' AS "SOURCE"."FILE_PATH", )"
        R"('localhost:8080' AS "SOURCE"."HOST", 'CSV' AS "INPUT_FORMATTER"."TYPE");)");
    /// An ATTACH FILE source points at an existing file, so no statement carries inline data to write.
    EXPECT_FALSE(setup.at(1).staged.has_value());
}

/// A source that reads from a socket gets no file path: its attached data is served to it, and the endpoint is only settled once the server
/// binds, so the statement is left without one and the rows are staged for a server instead.
/// Options the test set are still kept in their original spelling.
TEST_F(SqlRewriterTest, StagesTheDataOfASourceThatReadsFromASocket)
{
    const auto [name, variant, setup, queries]
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
        setup.at(1).sql,
        R"(CREATE PHYSICAL SOURCE FOR TESTKEY_STREAM TYPE TCP SET ('localhost:8080' AS "SOURCE"."HOST", )"
        R"('CSV' AS "INPUT_FORMATTER"."TYPE", '|' AS INPUT_FORMATTER.FIELD_DELIMITER);)");
    ASSERT_TRUE(setup.at(1).staged.has_value());
    const auto* served = std::get_if<ServedData>(&setup.at(1).staged.value());
    ASSERT_NE(served, nullptr);
    EXPECT_EQ(std::get<std::vector<std::string>>(served->content), (std::vector<std::string>{"1|19"}));
}

/// A source that reads from a socket pointed at a file rather than declaring rows, so the file is what gets served.
TEST_F(SqlRewriterTest, ServesTheFileOfASourceThatReadsFromASocket)
{
    const auto [name, variant, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                                                            "CREATE PHYSICAL SOURCE FOR stream TYPE TCP;\n"
                                                            "ATTACH FILE small/stream8.csv\n"
                                                            "\n"
                                                            "CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
                                                            "\n"
                                                            "SELECT id FROM stream INTO out;\n"
                                                            "----\n"
                                                            "1\n");

    ASSERT_EQ(setup.size(), 2U);
    ASSERT_TRUE(setup.at(1).staged.has_value());
    const auto* served = std::get_if<ServedData>(&setup.at(1).staged.value());
    ASSERT_NE(served, nullptr);
    EXPECT_EQ(std::get<std::filesystem::path>(served->content), "/data/small/stream8.csv");
}

/// The endpoint a data server bound is only known once the run is under way, so it is merged into the statement then, alongside
/// whatever the rewriter already put there.
TEST_F(SqlRewriterTest, AddsSourceOptionsToAnAlreadyRewrittenStatement)
{
    EXPECT_EQ(
        addSourceOptions(
            R"(CREATE PHYSICAL SOURCE FOR TESTKEY_STREAM TYPE TCP SET ('localhost:8080' AS "SOURCE"."HOST");)",
            {R"('4242' AS "SOURCE"."SOCKET_PORT")"}),
        R"(CREATE PHYSICAL SOURCE FOR TESTKEY_STREAM TYPE TCP SET ('4242' AS "SOURCE"."SOCKET_PORT", 'localhost:8080' AS "SOURCE"."HOST");)");
}

/// A sink that discards its input writes no file, so it is given neither a path nor an output format and the query is left with no result
/// file to read back.
/// Both sink forms are lowered that way.
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

/// A source written into the query gives its path relative to the test-data directory, so the path is resolved against it while the
/// rest of the source options, the sink and the query name are rewritten as usual.
TEST_F(SqlRewriterTest, ResolvesTheFileOfASourceWrittenIntoTheQuery)
{
    const auto [name, variant, setup, queries]
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
        R"(SCHEMA(id UINT64 NOT NULL) AS "SINK"."SCHEMA") SET ('TESTKEY:1' AS "QUERY"."NAME");)");
}

/// A source written into the query that carries its own data has no file to resolve, so it keeps the options the test wrote.
/// It still gets the host, because the coordinator has to place it like any other source.
TEST_F(SqlRewriterTest, KeepsTheOptionsOfASelfContainedSourceWrittenIntoTheQuery)
{
    const auto [name, variant, setup, queries]
        = rewriteSlt("CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
                     "\n"
                     R"(SELECT id FROM Generator('SEQUENCE UINT64 0 10 1' AS "SOURCE".GENERATOR_SCHEMA) INTO out;)"
                     "\n----\n"
                     "1\n");

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_TRUE(
        queryOf(queries.at(0))
            .sql.starts_with(
                R"(SELECT id FROM Generator('SEQUENCE UINT64 0 10 1' AS "SOURCE".GENERATOR_SCHEMA, 'localhost:8080' AS "SOURCE"."HOST") INTO )"));
}

/// An absolute path already resolves the same way wherever it is read, so it is left untouched.
TEST_F(SqlRewriterTest, KeepsAnAbsoluteFileOfASourceWrittenIntoTheQuery)
{
    const auto [name, variant, setup, queries]
        = rewriteSlt("CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
                     "\n"
                     R"(SELECT id FROM File('/elsewhere/stream8.csv' AS "SOURCE".FILE_PATH) INTO out;)"
                     "\n----\n"
                     "1\n");

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_TRUE(
        queryOf(queries.at(0))
            .sql.starts_with(
                R"(SELECT id FROM File('/elsewhere/stream8.csv' AS "SOURCE".FILE_PATH, 'localhost:8080' AS "SOURCE"."HOST") INTO )"));
}

/// A self-contained physical source draws its data from its own options rather than an attached data set.
/// It is pinned to the worker, its options are kept in their original spelling, and no statement carries inline data to write.
TEST_F(SqlRewriterTest, RewritesSelfContainedSourceKeepingItsOptions)
{
    const auto [name, variant, setup, queries]
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
        setup.at(1).sql,
        R"(CREATE PHYSICAL SOURCE FOR TESTKEY_GEN TYPE Generator SET ('localhost:8080' AS "SOURCE"."HOST", )"
        R"('CSV' AS "INPUT_FORMATTER"."TYPE", 'SEQUENCE UINT64 0 10 1' AS "SOURCE".GENERATOR_SCHEMA);)");
    EXPECT_FALSE(setup.at(1).staged.has_value());
}

}
