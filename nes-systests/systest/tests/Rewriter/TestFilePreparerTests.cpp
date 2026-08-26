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
#include <fstream>
#include <initializer_list>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <TokenStreamRewriter.h>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <Config/Config.hpp>
#include <Discovery/TestDiscovery.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Model/Expectation.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Parser/SystestParser.hpp>
#include <Parser/TestFileBuilder.hpp>
#include <Rewriter/NamePrefixer.hpp>
#include <Rewriter/RewriteContext.hpp>
#include <Rewriter/SourceRewriting.hpp>
#include <Rewriter/SqlParse.hpp>
#include <Rewriter/TestFilePreparer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <TemporaryDirectory.hpp>

namespace NES
{

class PrefixNamesTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("PrefixNames.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup PrefixNames test class.");
    }

    static void TearDownTestSuite() { NES_INFO("Tear down PrefixNames test class."); }

    /// Registers and seals the names, as the declaring pass does before a rewrite.
    static PrefixedNames declareAll(const std::initializer_list<std::string_view> declared)
    {
        NameRegistry registry{TestFileKeyFactory{"/tests"}.deriveKeyOf("/tests/benchmark/Nexmark.test", 0, 1)};
        for (const auto name : declared)
        {
            registry.declare(name);
        }
        return std::move(registry).seal();
    }

    /// Runs only the prefixing, without the other edits of a rewrite.
    static std::string prefix(const std::string& sql, const PrefixedNames& names)
    {
        SqlParse parse{sql};
        antlr4::TokenStreamRewriter rewriter{&parse.tokenStream()};
        prefixNames(parse, rewriter, names);
        return rewriter.getText();
    }
};

/// Keywords, punctuation and whitespace stay byte for byte.
TEST_F(PrefixNamesTest, SubstitutesRegisteredNames)
{
    const auto names = declareAll({"stream", "sink"});
    EXPECT_EQ(prefix("SELECT x  FROM stream INTO sink", names), "SELECT x  FROM BENCHMARK_D_NEXMARK_STREAM INTO BENCHMARK_D_NEXMARK_SINK");
}

/// Unquoted names compare case-insensitively.
TEST_F(PrefixNamesTest, MatchesRegisteredNameRegardlessOfCase)
{
    const auto names = declareAll({"stream"});
    EXPECT_EQ(prefix("SELECT x FROM STREAM", names), "SELECT x FROM BENCHMARK_D_NEXMARK_STREAM");
}

TEST_F(PrefixNamesTest, LeavesUnregisteredIdentifiersAlone)
{
    const auto names = declareAll({"stream"});
    EXPECT_EQ(prefix("SELECT unknown FROM stream", names), "SELECT unknown FROM BENCHMARK_D_NEXMARK_STREAM");
}

TEST_F(PrefixNamesTest, DoesNotRewriteIdentifierContainingRegisteredName)
{
    const auto names = declareAll({"stream"});
    EXPECT_EQ(prefix("SELECT x FROM streamSink", names), "SELECT x FROM streamSink");
    EXPECT_EQ(prefix("SELECT x FROM sinkStream", names), "SELECT x FROM sinkStream");
}

TEST_F(PrefixNamesTest, DoesNotRewriteInsideStringLiterals)
{
    const auto names = declareAll({"stream"});
    EXPECT_EQ(
        prefix(R"(SELECT x FROM stream INTO File('/data/stream/x.csv' AS "SINK".FILE_PATH))", names),
        R"(SELECT x FROM BENCHMARK_D_NEXMARK_STREAM INTO File('/data/stream/x.csv' AS "SINK".FILE_PATH))");
}

/// The prefixed spelling is not itself registered.
TEST_F(PrefixNamesTest, IsIdempotent)
{
    const auto names = declareAll({"stream", "sink"});
    const auto once = prefix("SELECT x FROM stream INTO sink", names);
    EXPECT_EQ(prefix(once, names), once);
}

/// The grammar parses a plugin type as a plain identifier, so a source called `File` keeps `TYPE File`.
TEST_F(PrefixNamesTest, PreservesAPluginTypeThatMatchesADeclaredName)
{
    const auto names = declareAll({"File"});
    EXPECT_EQ(prefix("CREATE PHYSICAL SOURCE FOR File TYPE File", names), "CREATE PHYSICAL SOURCE FOR BENCHMARK_D_NEXMARK_FILE TYPE File");
    EXPECT_EQ(
        prefix("CREATE SINK File(field_1 UINT64 NOT NULL) TYPE File", names),
        "CREATE SINK BENCHMARK_D_NEXMARK_FILE(field_1 UINT64 NOT NULL) TYPE File");
}

/// Inline sources and sinks also have a plain-identifier type, with no keyword before it.
TEST_F(PrefixNamesTest, PreservesThePluginTypeOfASourceOrSinkWrittenIntoAQuery)
{
    const auto names = declareAll({"File"});
    EXPECT_EQ(
        prefix(R"(SELECT x FROM File INTO File('out.csv' AS "SINK".FILE_PATH))", names),
        R"(SELECT x FROM BENCHMARK_D_NEXMARK_FILE INTO File('out.csv' AS "SINK".FILE_PATH))");
}

/// The grammar parses a function name as a plain identifier too.
TEST_F(PrefixNamesTest, PreservesAFunctionNamedLikeADeclaredName)
{
    const auto names = declareAll({"abs"});
    EXPECT_EQ(prefix("SELECT ABS(x) FROM abs", names), "SELECT ABS(x) FROM BENCHMARK_D_NEXMARK_ABS");
}

/// A renamed column would break the match between the query output and the declared sink schema.
TEST_F(PrefixNamesTest, PreservesAColumnNamedLikeADeclaredName)
{
    const auto names = declareAll({"s", "out"});
    EXPECT_EQ(
        prefix("CREATE LOGICAL SOURCE s(s UINT64 NOT NULL)", names), "CREATE LOGICAL SOURCE BENCHMARK_D_NEXMARK_S(s UINT64 NOT NULL)");
    EXPECT_EQ(
        prefix("SELECT s FROM s WHERE s > 1 INTO out", names),
        "SELECT s FROM BENCHMARK_D_NEXMARK_S WHERE s > 1 INTO BENCHMARK_D_NEXMARK_OUT");
    EXPECT_EQ(prefix("SELECT s AS s FROM s", names), "SELECT s AS s FROM BENCHMARK_D_NEXMARK_S");
}

/// The parser rejects a qualifier that does not match the source.
TEST_F(PrefixNamesTest, PrefixesTheSourceQualifierOfAFieldReference)
{
    const auto names = declareAll({"s"});
    EXPECT_EQ(
        prefix("SELECT s.s, s.* FROM s", names), "SELECT BENCHMARK_D_NEXMARK_S.s, BENCHMARK_D_NEXMARK_S.* FROM BENCHMARK_D_NEXMARK_S");
    EXPECT_EQ(prefix("SELECT x.id FROM s AS x", names), "SELECT x.id FROM BENCHMARK_D_NEXMARK_S AS x");
}

/// The grammar admits any quoted text, so the parser rejects such a query later.
TEST_F(PrefixNamesTest, LeavesATokenThatIsNotALegalIdentifierAlone)
{
    const auto names = declareAll({"stream"});
    EXPECT_EQ(prefix(R"(SELECT "" FROM stream)", names), R"(SELECT "" FROM BENCHMARK_D_NEXMARK_STREAM)");
    EXPECT_EQ(prefix(R"(SELECT "a.b" FROM stream)", names), R"(SELECT "a.b" FROM BENCHMARK_D_NEXMARK_STREAM)");
}

/// The timestamp is a field, so it stays unprefixed even when a source shares its name.
TEST_F(PrefixNamesTest, PrefixesEverySourceAndSinkOfAJoinButNotItsTimestampField)
{
    const auto names = declareAll({"s", "t", "ts", "out", "out2"});
    EXPECT_EQ(
        prefix("SELECT * FROM (SELECT * FROM s) INNER JOIN t ON s.id = t.id WINDOW TUMBLING(ts, SIZE 1 MINUTES) INTO out, out2", names),
        "SELECT * FROM (SELECT * FROM BENCHMARK_D_NEXMARK_S) INNER JOIN BENCHMARK_D_NEXMARK_T ON BENCHMARK_D_NEXMARK_S.id = "
        "BENCHMARK_D_NEXMARK_T.id WINDOW TUMBLING(ts, SIZE 1 MINUTES) INTO BENCHMARK_D_NEXMARK_OUT, BENCHMARK_D_NEXMARK_OUT2");
}

class TestFilePreparerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("TestFileRewriter.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup TestFileRewriter test class.");
    }

    static void TearDownTestSuite() { NES_INFO("Tear down TestFileRewriter test class."); }

    static RunnableTestFile rewriteSlt(const std::string& slt)
    {
        SystestParser parser;
        parser.loadString(slt);
        const std::filesystem::path testFilePath{"/tests/testkey.test"};
        return TestFilePreparer::rewritePartition(
            buildTestFile(parser, testFilePath),
            RewriteContext{
                .testFileKey = TestFileKeyFactory{"/tests"}.deriveKeyOf(testFilePath, 0, 1),
                .name = "testkey",
                .workingDir = "/work",
                .testDataDir = "/data",
                .sourceHost = Host{"localhost:8080"},
                .sinkHost = Host{"localhost:8080"}});
    }
};

/// The rewriter does not check the missing physical source.
TEST_F(TestFilePreparerTest, MergesADifferentialBlockIntoOneCase)
{
    const auto [name, key, originalNames, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
                                                                       "CREATE SINK sinkOneTuple(field_1 UINT64 NOT NULL) TYPE File;\n"
                                                                       "\n"
                                                                       "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n"
                                                                       "====\n"
                                                                       "SELECT field_1 FROM oneTuple INTO sinkOneTuple;\n");

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_TRUE(std::holds_alternative<RewrittenDifferential>(queries.at(0).action));
}

TEST_F(TestFilePreparerTest, RewritesInlineSourceNamedSinkAndQuery)
{
    const auto [name, key, originalNames, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
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
    EXPECT_EQ(getSqlOf(setup.at(0)), "CREATE LOGICAL SOURCE TESTKEY_ONETUPLE(field_1 UINT64 NOT NULL);");
    EXPECT_TRUE(std::holds_alternative<PlainStatement>(setup.at(0)));

    EXPECT_EQ(
        getSqlOf(setup.at(1)),
        R"(CREATE PHYSICAL SOURCE FOR TESTKEY_ONETUPLE TYPE File SET ('/work/sources/TESTKEY_0.csv' AS "SOURCE"."FILE_PATH", )"
        R"('localhost:8080' AS "SOURCE"."HOST", 'CSV' AS "INPUT_FORMATTER"."TYPE");)");
    const auto* withInline = std::get_if<StatementWithInlineData>(&setup.at(1));
    ASSERT_NE(withInline, nullptr);
    EXPECT_EQ(withInline->data.path, "/work/sources/TESTKEY_0.csv");
    EXPECT_EQ(withInline->data.rows, (std::vector<std::string>{"1"}));

    ASSERT_EQ(queries.size(), 1U);
    const auto& query = std::get<RewrittenQuery>(queries.at(0).action);
    EXPECT_EQ(
        query.sql,
        R"(SELECT field_1 FROM TESTKEY_ONETUPLE INTO File('localhost:8080' AS "SINK"."HOST", '/work/TESTKEY_1.csv' AS "SINK"."FILE_PATH", )"
        R"('CSV' AS "SINK"."OUTPUT_FORMAT", SCHEMA(field_1 UINT64 NOT NULL) AS "SINK"."SCHEMA");)");
    EXPECT_EQ(query.resultFile, "/work/TESTKEY_1.csv");
    const auto* expectedRows = std::get_if<ExpectedRows>(&query.expected);
    ASSERT_NE(expectedRows, nullptr);
    EXPECT_EQ(expectedRows->rows, (std::vector<std::string>{"1"}));

    /// The rewriter inlines the sink instead of registering it, so only the source has a prefixed name to restore.
    EXPECT_EQ(originalNames, (OriginalNames{{"TESTKEY_ONETUPLE", "ONETUPLE"}}));
}

/// The halves share one query number, and one shared result file would compare a result with itself.
TEST_F(TestFilePreparerTest, GivesEachHalfOfADifferentialBlockItsOwnResultFile)
{
    const auto [name, key, originalNames, setup, queries]
        = rewriteSlt("CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
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

/// The rewriter registers every name before rewriting any statement.
TEST_F(TestFilePreparerTest, PrefixesSourceDeclaredBelowTheQuery)
{
    const auto [name, key, originalNames, setup, queries] = rewriteSlt("CREATE SINK sinkOneTuple(field_1 UINT64 NOT NULL) TYPE File;\n"
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

/// The declaring pass stores every sink before rewriting any statement.
TEST_F(TestFilePreparerTest, InlinesSinkDeclaredBelowTheQuery)
{
    const auto [name, key, originalNames, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE oneTuple(field_1 UINT64 NOT NULL);\n"
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

/// A test may expect the syntax error, so the query fails alone instead of the whole file.
TEST_F(TestFilePreparerTest, PassesAQueryThatDoesNotParseThrough)
{
    const auto [name, key, originalNames, setup, queries]
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
    const auto& query = std::get<RewrittenQuery>(queries.at(0).action);
    EXPECT_EQ(query.sql, "SELECT field_1 FROM oneTuple GROUP BY field_1 WHERE field_1 > UINT64(1) INTO sinkOneTuple;");
    EXPECT_FALSE(query.resultFile.has_value());
}

/// A model gets a prefix like a source.
/// The worker resolves a relative model path from its own working directory, so the rewriter makes it absolute.
TEST_F(TestFilePreparerTest, RewritesModelAndTheQueryThatInfersWithIt)
{
    const auto [name, key, originalNames, setup, queries]
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
        getSqlOf(setup.at(2)),
        "CREATE MODEL TESTKEY_IRIS ('/data/model/iris.onnx')\n"
        "INPUT (p1 FLOAT32)\n"
        "OUTPUT (setosa FLOAT32);");
    EXPECT_TRUE(std::holds_alternative<PlainStatement>(setup.at(2)));

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_TRUE(std::get<RewrittenQuery>(queries.at(0).action).sql.contains("MODEL_INFERENCE(TESTKEY_IRIS, TESTKEY_STREAM)"));
}

TEST_F(TestFilePreparerTest, RewritesFileSourceWithoutMaterializing)
{
    const auto [name, key, originalNames, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
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
        getSqlOf(setup.at(1)),
        R"(CREATE PHYSICAL SOURCE FOR TESTKEY_STREAM TYPE File SET ('/data/small/stream8.csv' AS "SOURCE"."FILE_PATH", )"
        R"('localhost:8080' AS "SOURCE"."HOST", 'CSV' AS "INPUT_FORMATTER"."TYPE");)");
    /// An ATTACH FILE source points at an existing file, so no statement holds inline data to write.
    EXPECT_TRUE(std::holds_alternative<PlainStatement>(setup.at(1)));
}

/// The endpoint exists only once the server binds, so the runner adds it later.
TEST_F(TestFilePreparerTest, StagesTheDataOfASourceThatReadsFromASocket)
{
    const auto [name, key, originalNames, setup, queries]
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
        getSqlOf(setup.at(1)),
        R"(CREATE PHYSICAL SOURCE FOR TESTKEY_STREAM TYPE TCP SET ('localhost:8080' AS "SOURCE"."HOST", )"
        R"('CSV' AS "INPUT_FORMATTER"."TYPE", '|' AS INPUT_FORMATTER.FIELD_DELIMITER);)");
    const auto* served = std::get_if<StatementWithServedData>(&setup.at(1));
    ASSERT_NE(served, nullptr);
    EXPECT_EQ(std::get<std::vector<std::string>>(served->data.content), (std::vector<std::string>{"1|19"}));
}

TEST_F(TestFilePreparerTest, ServesTheFileOfASourceThatReadsFromASocket)
{
    const auto [name, key, originalNames, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
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

/// The runner adds the endpoint once the server binds.
TEST_F(TestFilePreparerTest, AddsSourceOptionsToAnAlreadyRewrittenStatement)
{
    EXPECT_EQ(
        addSourceOptions(
            R"(CREATE PHYSICAL SOURCE FOR TESTKEY_STREAM TYPE TCP SET ('localhost:8080' AS "SOURCE"."HOST");)",
            {SourceOption{.group = "SOURCE", .key = "SOCKET_PORT", .value = "4242"}}),
        R"(CREATE PHYSICAL SOURCE FOR TESTKEY_STREAM TYPE TCP SET ('4242' AS "SOURCE"."SOCKET_PORT", 'localhost:8080' AS "SOURCE"."HOST");)");
}

/// The binder rejects a key set twice.
TEST_F(TestFilePreparerTest, AddSourceOptionsKeepsAValueThatTheTestWrote)
{
    EXPECT_EQ(
        addSourceOptions(
            R"(CREATE PHYSICAL SOURCE FOR TESTKEY_STREAM TYPE TCP SET ('200' AS "SOURCE"."FLUSH_INTERVAL_MS");)",
            {SourceOption{.group = "SOURCE", .key = "SOCKET_PORT", .value = "4242"},
             SourceOption{.group = "SOURCE", .key = "FLUSH_INTERVAL_MS", .value = "100"}}),
        R"(CREATE PHYSICAL SOURCE FOR TESTKEY_STREAM TYPE TCP SET ('4242' AS "SOURCE"."SOCKET_PORT", '200' AS "SOURCE"."FLUSH_INTERVAL_MS");)");
}

/// The staged server sends the data, so an endpoint the test wrote would read from elsewhere.
TEST_F(TestFilePreparerTest, RejectsAServedSourceThatChoosesItsEndpoint)
{
    EXPECT_THROW(
        rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                   R"(CREATE PHYSICAL SOURCE FOR stream TYPE TCP SET('4242' AS "SOURCE"."SOCKET_PORT");)"
                   "\n"
                   "ATTACH INLINE\n"
                   "1\n"
                   "\n"
                   "SELECT id FROM stream INTO Void();\n"
                   "----\n"),
        Exception);
}

TEST_F(TestFilePreparerTest, KeepsTheEndpointOfASocketSourceWithoutAttachedData)
{
    const auto [name, key, originalNames, setup, queries] = rewriteSlt(
        "CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
        R"(CREATE PHYSICAL SOURCE FOR stream TYPE TCP SET('example.org' AS "SOURCE"."SOCKET_HOST", '4242' AS "SOURCE"."SOCKET_PORT");)"
        "\n"
        "\n"
        "SELECT id FROM stream INTO Void();\n"
        "----\n");

    ASSERT_EQ(setup.size(), 2U);
    ASSERT_TRUE(std::holds_alternative<PlainStatement>(setup.at(1)));
    EXPECT_TRUE(getSqlOf(setup.at(1)).contains(R"('example.org' AS "SOURCE"."SOCKET_HOST")"));
    EXPECT_TRUE(getSqlOf(setup.at(1)).contains(R"('4242' AS "SOURCE"."SOCKET_PORT")"));
}

TEST_F(TestFilePreparerTest, RejectsAServedSourceThatChoosesItsHost)
{
    EXPECT_THROW(
        rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                   R"(CREATE PHYSICAL SOURCE FOR stream TYPE TCP SET('localhost' AS "SOURCE"."SOCKET_HOST");)"
                   "\n"
                   "ATTACH INLINE\n"
                   "1\n"
                   "\n"
                   "SELECT id FROM stream INTO Void();\n"
                   "----\n"),
        Exception);
}

/// A discarding sink writes no file, so the query has no result to read back.
TEST_F(TestFilePreparerTest, GivesASinkThatDiscardsItsInputNoResultFile)
{
    const auto declared = rewriteSlt("CREATE SINK discard(id UINT64 NOT NULL) TYPE Void;\n"
                                     "\n"
                                     R"(SELECT id FROM File('small/stream8.csv' AS "SOURCE".FILE_PATH) INTO discard;)"
                                     "\n----\n");
    ASSERT_EQ(declared.testCases.size(), 1U);
    const auto& declaredQuery = std::get<RewrittenQuery>(declared.testCases.at(0).action);
    EXPECT_FALSE(declaredQuery.resultFile.has_value());
    EXPECT_TRUE(
        declaredQuery.sql.contains(R"(INTO Void('localhost:8080' AS "SINK"."HOST", SCHEMA(id UINT64 NOT NULL) AS "SINK"."SCHEMA"))"));

    const auto written = rewriteSlt(R"(SELECT id FROM File('small/stream8.csv' AS "SOURCE".FILE_PATH) INTO Void();)"
                                    "\n----\n");
    ASSERT_EQ(written.testCases.size(), 1U);
    const auto& writtenQuery = std::get<RewrittenQuery>(written.testCases.at(0).action);
    EXPECT_FALSE(writtenQuery.resultFile.has_value());
    EXPECT_TRUE(writtenQuery.sql.contains(R"(INTO Void('localhost:8080' AS "SINK"."HOST"))"));
}

/// The test file format admits ATTACH after any CREATE, so the rewriter reports a misplaced one instead of dropping it.
TEST_F(TestFilePreparerTest, RejectsDataAttachedToAnythingButAPhysicalSource)
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

/// The path is relative to the test-data directory.
TEST_F(TestFilePreparerTest, ResolvesTheFileOfASourceWrittenIntoTheQuery)
{
    const auto [name, key, originalNames, setup, queries]
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

/// It still gets a host, because placement needs one.
TEST_F(TestFilePreparerTest, KeepsTheOptionsOfASelfContainedSourceWrittenIntoTheQuery)
{
    const auto [name, key, originalNames, setup, queries]
        = rewriteSlt("CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
                     "\n"
                     R"(SELECT id FROM Generator('SEQUENCE UINT64 0 10 1' AS "SOURCE".GENERATOR_SCHEMA) INTO out;)"
                     "\n----\n"
                     "1\n");

    ASSERT_EQ(queries.size(), 1U);
    const auto& query = std::get<RewrittenQuery>(queries.at(0).action);
    EXPECT_TRUE(query.sql.contains(R"('SEQUENCE UINT64 0 10 1' AS "SOURCE".GENERATOR_SCHEMA)"));
    EXPECT_TRUE(query.sql.contains(R"('localhost:8080' AS "SOURCE"."HOST")"));
}

TEST_F(TestFilePreparerTest, KeepsAnAbsoluteFileOfASourceWrittenIntoTheQuery)
{
    const auto [name, key, originalNames, setup, queries]
        = rewriteSlt("CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
                     "\n"
                     R"(SELECT id FROM File('/elsewhere/stream8.csv' AS "SOURCE".FILE_PATH) INTO out;)"
                     "\n----\n"
                     "1\n");

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_TRUE(std::get<RewrittenQuery>(queries.at(0).action).sql.contains(R"('/elsewhere/stream8.csv' AS "SOURCE".FILE_PATH)"));
}

/// Such a source generates its own data, so the rewriter stages nothing and adds only defaults.
TEST_F(TestFilePreparerTest, RewritesSelfContainedSourceKeepingItsOptions)
{
    const auto [name, key, originalNames, setup, queries]
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
        getSqlOf(setup.at(1)),
        R"(CREATE PHYSICAL SOURCE FOR TESTKEY_GEN TYPE Generator SET ('localhost:8080' AS "SOURCE"."HOST", )"
        R"('CSV' AS "INPUT_FORMATTER"."TYPE", 'SEQUENCE UINT64 0 10 1' AS "SOURCE".GENERATOR_SCHEMA);)");
    EXPECT_TRUE(std::holds_alternative<PlainStatement>(setup.at(1)));
}

TEST_F(TestFilePreparerTest, KeepsTheHostAPhysicalSourceChose)
{
    const auto [name, key, originalNames, setup, queries]
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
    EXPECT_TRUE(getSqlOf(setup.at(1)).contains(R"('elsewhere:9999' AS "SOURCE"."HOST")"));
    EXPECT_FALSE(getSqlOf(setup.at(1)).contains(R"('localhost:8080' AS "SOURCE"."HOST")"));
}

TEST_F(TestFilePreparerTest, KeepsTheHostASinkWrittenIntoTheQueryChose)
{
    const auto [name, key, originalNames, setup, queries]
        = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                     "\n"
                     R"(SELECT id FROM stream INTO Void('elsewhere:9999' AS "SINK"."HOST");)"
                     "\n----\n"
                     "1\n");

    ASSERT_EQ(queries.size(), 1U);
    const auto& query = std::get<RewrittenQuery>(queries.at(0).action);
    EXPECT_TRUE(query.sql.contains(R"('elsewhere:9999' AS "SINK"."HOST")"));
    EXPECT_FALSE(query.sql.contains(R"('localhost:8080' AS "SINK"."HOST")"));
}

/// The data comes from ATTACH, and a path in SET would stay relative or clash with it.
TEST_F(TestFilePreparerTest, RejectsAPhysicalSourceThatChoosesItsDataFile)
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

/// The checker reads only the result file the rewriter chose.
TEST_F(TestFilePreparerTest, RejectsASinkThatChoosesItsResultFile)
{
    EXPECT_THROW(
        rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                   "\n"
                   R"(SELECT id FROM stream INTO File('/elsewhere/out.csv' AS "SINK"."FILE_PATH");)"
                   "\n----\n"
                   "1\n"),
        Exception);
}

/// The expected checksums were computed over quoted strings.
TEST_F(TestFilePreparerTest, ChecksumSinkGetsQuotedStrings)
{
    const auto [name, key, originalNames, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                                                                       "\n"
                                                                       "SELECT id FROM stream INTO Checksum();\n"
                                                                       "----\n"
                                                                       "1\n");

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_TRUE(std::get<RewrittenQuery>(queries.at(0).action).sql.contains(R"('true' AS "OUTPUT_FORMATTER"."QUOTE_STRINGS")"));
}

TEST_F(TestFilePreparerTest, ChecksumSinkKeepsItsOwnQuotingChoice)
{
    const auto [name, key, originalNames, setup, queries]
        = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                     "\n"
                     "SELECT id FROM stream INTO Checksum('false' AS \"OUTPUT_FORMATTER\".\"QUOTE_STRINGS\");\n"
                     "----\n"
                     "1\n");

    ASSERT_EQ(queries.size(), 1U);
    const auto& query = std::get<RewrittenQuery>(queries.at(0).action);
    EXPECT_TRUE(query.sql.contains(R"('false' AS "OUTPUT_FORMATTER"."QUOTE_STRINGS")"));
    EXPECT_FALSE(query.sql.contains(R"('true' AS "OUTPUT_FORMATTER"."QUOTE_STRINGS")"));
}

/// An explained query binds like a regular one, so its inline source needs the same path, host, and format.
TEST_F(TestFilePreparerTest, ExplainCompletesASourceWrittenIntoTheQuery)
{
    const auto [name, key, originalNames, setup, queries]
        = rewriteSlt("EXPLAIN (OPTIMIZED) FORMAT TEXT SELECT id FROM File('small/stream8.csv' AS \"SOURCE\".FILE_PATH) "
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

/// VISUAL is also the default, so the rewriter rejects an EXPLAIN that states no format as well.
TEST_F(TestFilePreparerTest, ExplainRejectsTheVisualFormat)
{
    const auto slt = [](const std::string_view format)
    {
        return fmt::format(
            "CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
            "\n"
            "EXPLAIN (OPTIMIZED) {} SELECT id FROM stream INTO Void('true' AS \"SINK\".\"NOOP\");\n"
            "----\n"
            "== Optimized Plan ==\n",
            format);
    };
    EXPECT_THROW(std::ignore = rewriteSlt(slt("FORMAT VISUAL")), Exception);
    EXPECT_THROW(std::ignore = rewriteSlt(slt("FORMAT visual")), Exception);
    EXPECT_THROW(std::ignore = rewriteSlt(slt("")), Exception);
    EXPECT_NO_THROW(std::ignore = rewriteSlt(slt("FORMAT TEXT")));
    EXPECT_NO_THROW(std::ignore = rewriteSlt(slt("FORMAT VERBOSE")));
}

/// The printed plan shows the sink's name, so the sink has to exist in the catalog.
TEST_F(TestFilePreparerTest, ExplainKeepsTheDeclaredSinkAndSubmitsItsDeclaration)
{
    const auto [name, key, originalNames, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                                                                       "CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
                                                                       "\n"
                                                                       "EXPLAIN (OPTIMIZED) FORMAT TEXT SELECT id FROM stream INTO out;\n"
                                                                       "----\n"
                                                                       "== Optimized Plan ==\n");

    ASSERT_EQ(setup.size(), 2U);
    EXPECT_EQ(
        getSqlOf(setup.at(1)),
        R"(CREATE SINK TESTKEY_OUT(id UINT64 NOT NULL) TYPE File )"
        R"(SET ('localhost:8080' AS "SINK"."HOST", '/work/TESTKEY_out.csv' AS "SINK"."FILE_PATH", 'CSV' AS "SINK"."OUTPUT_FORMAT");)");

    ASSERT_EQ(queries.size(), 1U);
    const auto* explain = std::get_if<RewrittenExplain>(&queries.at(0).action);
    ASSERT_NE(explain, nullptr);
    EXPECT_EQ(explain->sql, "EXPLAIN (OPTIMIZED) FORMAT TEXT SELECT id FROM TESTKEY_STREAM INTO TESTKEY_OUT;");
}

/// Only File and Checksum take a path, and another sink would report a parameter the test never wrote.
TEST_F(TestFilePreparerTest, RejectsASinkWhoseResultNoTestCanCheck)
{
    EXPECT_THROW(
        rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                   "\n"
                   "SELECT id FROM stream INTO Print();\n"
                   "----\n"
                   "1\n"),
        Exception);
}

/// A test may expect the engine's error for it.
TEST_F(TestFilePreparerTest, PassesAnUnknownSinkTypeThroughForTheEngineToReject)
{
    const auto [name, key, originalNames, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                                                                       "\n"
                                                                       "SELECT id FROM stream INTO FileSink();\n"
                                                                       "----\n"
                                                                       "ERROR 1000\n");

    ASSERT_EQ(queries.size(), 1U);
    const auto& query = std::get<RewrittenQuery>(queries.at(0).action);
    EXPECT_FALSE(query.resultFile.has_value());
    EXPECT_TRUE(query.sql.contains("INTO FileSink("));
}

/// A test picks Void to run a query without checking its output.
TEST_F(TestFilePreparerTest, KeepsAVoidSinkThatWritesNoResult)
{
    const auto [name, key, originalNames, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                                                                       "\n"
                                                                       "SELECT id FROM stream INTO Void();\n"
                                                                       "----\n");

    ASSERT_EQ(queries.size(), 1U);
    EXPECT_FALSE(std::get<RewrittenQuery>(queries.at(0).action).resultFile.has_value());
}

/// The grammar allows one SET clause per sink.
TEST_F(TestFilePreparerTest, MergesTheRewrittenSinkOptionsIntoTheOnesTheTestWrote)
{
    const auto [name, key, originalNames, setup, queries]
        = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                     "CREATE SINK out(id UINT64 NOT NULL) TYPE File SET ('JSON' AS \"SINK\".\"OUTPUT_FORMAT\");\n"
                     "\n"
                     "EXPLAIN (OPTIMIZED) FORMAT TEXT SELECT id FROM stream INTO out;\n"
                     "----\n"
                     "== Optimized Plan ==\n");

    ASSERT_EQ(setup.size(), 2U);
    /// The format that the test chose survives, and the rewriter adds no second one.
    EXPECT_EQ(
        getSqlOf(setup.at(1)),
        R"(CREATE SINK TESTKEY_OUT(id UINT64 NOT NULL) TYPE File )"
        R"(SET ('localhost:8080' AS "SINK"."HOST", '/work/TESTKEY_out.csv' AS "SINK"."FILE_PATH", 'JSON' AS "SINK"."OUTPUT_FORMAT");)");
}

/// The checker reads only the result file the rewriter chose, for declared and inline sinks alike.
TEST_F(TestFilePreparerTest, RejectsADeclaredSinkThatChoosesItsResultFile)
{
    EXPECT_THROW(
        rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                   "CREATE SINK out(id UINT64 NOT NULL) TYPE File SET ('/tmp/mine.csv' AS \"SINK\".\"FILE_PATH\");\n"
                   "\n"
                   "EXPLAIN (OPTIMIZED) FORMAT TEXT SELECT id FROM stream INTO out;\n"
                   "----\n"
                   "== Optimized Plan ==\n"),
        Exception);
}

TEST_F(TestFilePreparerTest, ExplainInlinesTheSinkThatItsQueryWrites)
{
    const auto [name, key, originalNames, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                                                                       "\n"
                                                                       "EXPLAIN (OPTIMIZED) FORMAT TEXT SELECT id FROM stream "
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

/// The EXPLAIN keeps the prefixed sink name for its printed plan, while the query inlines the sink.
TEST_F(TestFilePreparerTest, MixesAnExplainWithAQueryIntoTheSameDeclaredSink)
{
    const auto [name, key, originalNames, setup, queries] = rewriteSlt("CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL);\n"
                                                                       "CREATE SINK out(id UINT64 NOT NULL) TYPE File;\n"
                                                                       "\n"
                                                                       "EXPLAIN (OPTIMIZED) FORMAT TEXT SELECT id FROM stream INTO out;\n"
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
    EXPECT_EQ(explain->sql, "EXPLAIN (OPTIMIZED) FORMAT TEXT SELECT id FROM TESTKEY_STREAM INTO TESTKEY_OUT;");

    const auto* query = std::get_if<RewrittenQuery>(&queries.at(1).action);
    ASSERT_NE(query, nullptr);
    EXPECT_TRUE(query->sql.starts_with("SELECT id FROM TESTKEY_STREAM INTO File("));
}

/// Partitioning precedes selection, so a query selected alone keeps the key and rewrite it has in a full run.
TEST_F(TestFilePreparerTest, SelectionKeepsThePartitionKeyStable)
{
    const Testing::TemporaryDirectory tmp;
    const auto file = tmp.get() / "Partitioned.test";
    {
        std::ofstream out{file};
        out << "CREATE LOGICAL SOURCE s(id UINT64 NOT NULL);\n"
               "CREATE PHYSICAL SOURCE FOR s TYPE File;\n"
               "ATTACH INLINE\n"
               "1\n"
               "\n"
               "CREATE SINK sinkA(id UINT64 NOT NULL) TYPE File;\n"
               "\n"
               "SELECT id FROM s INTO sinkA;\n"
               "----\n"
               "1\n"
               "\n"
               "GlobalConfiguration worker.query_engine.number_of_worker_threads: [2]\n"
               "SELECT id FROM s INTO sinkA;\n"
               "----\n"
               "1\n";
    }

    SystestConfiguration config{};
    config.testDiscoverRoot = tmp.get().string();
    config.workingDir.setValue(tmp.get().string());
    config.clusterConfig = SystestClusterConfiguration{
        .workers = {}, .allowSourcePlacement = {Host{"localhost:8080"}}, .allowSinkPlacement = {Host{"localhost:8080"}}};

    const auto everything = TestFilePreparer{config}.prepare(DiscoveredTestFile{file, TestName{"Partitioned"}});
    ASSERT_EQ(everything.size(), 2U);

    TestFilePreparer selecting{config};
    const auto selected = selecting.prepare(DiscoveredTestFile{file, TestName{"Partitioned"}, std::unordered_set{SystestQueryId{2}}});
    ASSERT_EQ(selected.size(), 1U);
    const auto& [overrides, runnable] = selected.front();
    EXPECT_EQ(runnable.key, everything.at(1).file.key);
    EXPECT_TRUE(runnable.key.ends_with("_C1")) << runnable.key;
    EXPECT_EQ(overrides.at("worker.query_engine.number_of_worker_threads"), "2");
    EXPECT_TRUE(std::get<RewrittenQuery>(runnable.testCases.front().action).sql.contains(runnable.key));
}

/// All files of a run share one catalog.
TEST_F(TestFilePreparerTest, RejectsASecondFileWhosePrefixedNameCollides)
{
    const Testing::TemporaryDirectory tmp;
    std::filesystem::create_directories(tmp.get() / "a");
    const auto writeFile = [](const std::filesystem::path& path, const std::string_view source)
    {
        std::ofstream out{path};
        out << fmt::format(
            "CREATE LOGICAL SOURCE {0}(id UINT64 NOT NULL);\n"
            "\n"
            "SELECT id FROM {0} INTO Void();\n"
            "----\n",
            source);
    };
    writeFile(tmp.get() / "a.test", "d_b_s");
    writeFile(tmp.get() / "a" / "b.test", "s");

    SystestConfiguration config{};
    config.testDiscoverRoot = tmp.get().string();
    config.workingDir.setValue(tmp.get().string());
    config.clusterConfig = SystestClusterConfiguration{
        .workers = {}, .allowSourcePlacement = {Host{"localhost:8080"}}, .allowSinkPlacement = {Host{"localhost:8080"}}};

    TestFilePreparer rewriter{config};
    EXPECT_NO_THROW(std::ignore = rewriter.prepare(DiscoveredTestFile{tmp.get() / "a.test", TestName{"a"}}));
    EXPECT_THROW(std::ignore = rewriter.prepare(DiscoveredTestFile{tmp.get() / "a" / "b.test", TestName{"a/b"}}), Exception);
    EXPECT_NO_THROW(std::ignore = TestFilePreparer{config}.prepare(DiscoveredTestFile{tmp.get() / "a" / "b.test", TestName{"a/b"}}));
}

TEST_F(TestFilePreparerTest, AFileWithoutQueriesYieldsNothingToRun)
{
    const Testing::TemporaryDirectory tmp;
    const auto file = tmp.get() / "SetupOnly.test";
    {
        std::ofstream out{file};
        out << "CREATE LOGICAL SOURCE s(id UINT64 NOT NULL);\n";
    }
    SystestConfiguration config{};
    config.testDiscoverRoot = tmp.get().string();
    config.workingDir.setValue(tmp.get().string());
    config.clusterConfig = SystestClusterConfiguration{
        .workers = {}, .allowSourcePlacement = {Host{"localhost:8080"}}, .allowSinkPlacement = {Host{"localhost:8080"}}};

    EXPECT_TRUE(TestFilePreparer{config}.prepare(DiscoveredTestFile{file, TestName{"SetupOnly"}}).empty());
}

}
