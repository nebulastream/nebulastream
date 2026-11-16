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
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/AnonymousSinkLogicalOperator.hpp>
#include <Operators/Sources/AnonymousSourceLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/FileSink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

class StatementBinderTest : public Testing::BaseUnitTest
{
public:
    std::shared_ptr<StatementBinder> binder;

    /* Will be called before a test is executed. */
    static void SetUpTestSuite()
    {
        Logger::setupLogging("StatementBinderTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup StatementBinderTest test case.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        binder = std::make_shared<StatementBinder>(
            [](auto&& queryContext)
            { return AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(queryContext)>(queryContext)); });
    }
};

///NOLINTBEGIN(bugprone-unchecked-optional-access)
TEST_F(StatementBinderTest, BindQuery)
{
    /// This test only checks that the input was detected as a query, for more detailed testing of the QueryBinder
    /// see the AntlrSQLQueryParserTest and the Systests
    /// In the future this will require setting up the source catalog correctly as well
    const std::string queryString = "SELECT a FROM inputStream WHERE b < UINT32(5) INTO outputStream";
    const auto statement = binder->parseAndBindSingle(queryString);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));
}

TEST_F(StatementBinderTest, BindQueryWithNegativeTypedFloatLiteralInWherePredicate)
{
    const std::string queryString = "SELECT a FROM inputStream WHERE b > FLOAT64(-0.5) AND b < FLOAT64(0.5) INTO outputStream";
    const auto statement = binder->parseAndBindSingle(queryString);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement).plan;
    const auto selectionOperators = getOperatorByType<SelectionLogicalOperator>(plan);
    ASSERT_EQ(selectionOperators.size(), 1);

    const auto predicate = selectionOperators.front()->getPredicate().explain(ExplainVerbosity::Short);
    EXPECT_EQ(predicate, "B > -0.5 AND B < 0.5");
}

TEST_F(StatementBinderTest, BindQueryWithPositiveTypedFloatLiteralInWherePredicate)
{
    const std::string queryString = "SELECT a FROM inputStream WHERE b < FLOAT64(+0.5) INTO outputStream";
    const auto statement = binder->parseAndBindSingle(queryString);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement).plan;
    const auto selectionOperators = getOperatorByType<SelectionLogicalOperator>(plan);
    ASSERT_EQ(selectionOperators.size(), 1);

    const auto predicate = selectionOperators.front()->getPredicate().explain(ExplainVerbosity::Short);
    EXPECT_EQ(predicate, "B < 0.5");
}

TEST_F(StatementBinderTest, BindQueryWithRawNumericLiterals)
{
    const std::string queryString = "SELECT a + 1 AS x FROM inputStream WHERE b < 256 AND c > -1 AND d < 1.5 INTO outputStream";
    const auto statement = binder->parseAndBindSingle(queryString);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement).plan;
    const auto selectionOperators = getOperatorByType<SelectionLogicalOperator>(plan);
    ASSERT_EQ(selectionOperators.size(), 1);

    const auto predicate = selectionOperators.front()->getPredicate().explain(ExplainVerbosity::Short);
    EXPECT_EQ(predicate, "B < 256 AND C > -1 AND D < 1.5");
}

TEST_F(StatementBinderTest, BindQueryWithRawStringAndBooleanLiterals)
{
    const std::string queryString
        = "SELECT 'hello' AS s, TRUE AS b, VARSIZED('world') AS explicit_s, BOOLEAN(FALSE) AS explicit_b FROM inputStream "
          "WHERE text == '123' AND flag == false AND FALSE == FALSE INTO outputStream";
    const auto statement = binder->parseAndBindSingle(queryString);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement).plan;
    const auto selectionOperators = getOperatorByType<SelectionLogicalOperator>(plan);
    ASSERT_EQ(selectionOperators.size(), 1);

    const auto predicate = selectionOperators.front()->getPredicate().explain(ExplainVerbosity::Short);
    EXPECT_EQ(predicate, "TEXT = '123' AND FLAG = false AND FALSE = FALSE");
}

TEST_F(StatementBinderTest, BindQueryWithDoubleNegativeTypedFloatLiteralInWherePredicate)
{
    const std::string queryString = "SELECT a FROM inputStream WHERE b < FLOAT64(- -0.5) INTO outputStream";
    const auto statement = binder->parseAndBindSingle(queryString);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement).plan;
    const auto selectionOperators = getOperatorByType<SelectionLogicalOperator>(plan);
    ASSERT_EQ(selectionOperators.size(), 1);

    const auto predicate = selectionOperators.front()->getPredicate().explain(ExplainVerbosity::Short);
    EXPECT_EQ(predicate, "B < 0.5");
}

TEST_F(StatementBinderTest, BindQueryWithUnaryMinusOnFieldInWherePredicate)
{
    const std::string queryString = "SELECT a FROM inputStream WHERE -b < FLOAT64(0.5) INTO outputStream";
    const auto statement = binder->parseAndBindSingle(queryString);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement).plan;
    const auto selectionOperators = getOperatorByType<SelectionLogicalOperator>(plan);
    ASSERT_EQ(selectionOperators.size(), 1);

    const auto predicate = selectionOperators.front()->getPredicate().explain(ExplainVerbosity::Short);
    EXPECT_EQ(predicate, "-1 * B < 0.5");
}

TEST_F(StatementBinderTest, BindQueryWithBetweenPredicate)
{
    const std::string queryString = "SELECT a FROM inputStream WHERE b BETWEEN UINT32(1) AND UINT32(5) INTO outputStream";
    const auto statement = binder->parseAndBindSingle(queryString);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement).plan;
    const auto selectionOperators = getOperatorByType<SelectionLogicalOperator>(plan);
    ASSERT_EQ(selectionOperators.size(), 1);

    const auto predicate = selectionOperators.front()->getPredicate().explain(ExplainVerbosity::Short);
    EXPECT_EQ(predicate, "B >= 1 AND B <= 5");
}

TEST_F(StatementBinderTest, BindQueryWithNotBetweenPredicate)
{
    const std::string queryString = "SELECT a FROM inputStream WHERE b NOT BETWEEN UINT32(1) AND UINT32(5) INTO outputStream";
    const auto statement = binder->parseAndBindSingle(queryString);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement).plan;
    const auto selectionOperators = getOperatorByType<SelectionLogicalOperator>(plan);
    ASSERT_EQ(selectionOperators.size(), 1);

    const auto predicate = selectionOperators.front()->getPredicate().explain(ExplainVerbosity::Short);
    EXPECT_EQ(predicate, "NOT(B >= 1 AND B <= 5)");
}

TEST_F(StatementBinderTest, BindQueryWithInPredicate)
{
    const std::string queryString = "SELECT a FROM inputStream WHERE b IN (UINT32(1), UINT32(5), UINT32(8)) INTO outputStream";
    const auto statement = binder->parseAndBindSingle(queryString);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement).plan;
    const auto selectionOperators = getOperatorByType<SelectionLogicalOperator>(plan);
    ASSERT_EQ(selectionOperators.size(), 1);

    const auto predicate = selectionOperators.front()->getPredicate().explain(ExplainVerbosity::Short);
    EXPECT_EQ(predicate, "B = 1 OR B = 5 OR B = 8");
}

TEST_F(StatementBinderTest, BindQueryWithNotInPredicate)
{
    const std::string queryString = "SELECT a FROM inputStream WHERE b NOT IN (UINT32(1), UINT32(5)) INTO outputStream";
    const auto statement = binder->parseAndBindSingle(queryString);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement).plan;
    const auto selectionOperators = getOperatorByType<SelectionLogicalOperator>(plan);
    ASSERT_EQ(selectionOperators.size(), 1);

    const auto predicate = selectionOperators.front()->getPredicate().explain(ExplainVerbosity::Short);
    EXPECT_EQ(predicate, "NOT(B = 1 OR B = 5)");
}

TEST_F(StatementBinderTest, BindQueryWithLogicalNotPredicateInConjunction)
{
    const std::string queryString = "SELECT a FROM inputStream WHERE b = UINT32(1) AND NOT(c = UINT32(2)) INTO outputStream";
    const auto statement = binder->parseAndBindSingle(queryString);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement).plan;
    const auto selectionOperators = getOperatorByType<SelectionLogicalOperator>(plan);
    ASSERT_EQ(selectionOperators.size(), 1);

    const auto predicate = selectionOperators.front()->getPredicate().explain(ExplainVerbosity::Short);
    EXPECT_EQ(predicate, "B = 1 AND NOT(C = 2)");
}

TEST_F(StatementBinderTest, BindQueryWithIsNullPredicate)
{
    const std::string queryString = "SELECT a FROM inputStream WHERE b is NULL INTO outputStream";
    const auto statement = binder->parseAndBindSingle(queryString);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement).plan;
    const auto selectionOperators = getOperatorByType<SelectionLogicalOperator>(plan);
    ASSERT_EQ(selectionOperators.size(), 1);

    const auto predicate = selectionOperators.front()->getPredicate().explain(ExplainVerbosity::Short);
    EXPECT_EQ(predicate, "ISNULL(B)");
}

TEST_F(StatementBinderTest, BindQueryWithIsNotNullPredicate)
{
    const std::string queryString = "SELECT a FROM inputStream WHERE b IS NOT NULL INTO outputStream";
    const auto statement = binder->parseAndBindSingle(queryString);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement).plan;
    const auto selectionOperators = getOperatorByType<SelectionLogicalOperator>(plan);
    ASSERT_EQ(selectionOperators.size(), 1);

    const auto predicate = selectionOperators.front()->getPredicate().explain(ExplainVerbosity::Short);
    EXPECT_EQ(predicate, "NOT(ISNULL(B))");
}

TEST_F(StatementBinderTest, BindQueryWithIsNaNPredicate)
{
    const std::string queryString = "SELECT a FROM inputStream WHERE b is NaN INTO outputStream";
    const auto statement = binder->parseAndBindSingle(queryString);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement).plan;
    const auto selectionOperators = getOperatorByType<SelectionLogicalOperator>(plan);
    ASSERT_EQ(selectionOperators.size(), 1);

    const auto predicate = selectionOperators.front()->getPredicate().explain(ExplainVerbosity::Short);
    EXPECT_EQ(predicate, "ISNAN(B)");
}

TEST_F(StatementBinderTest, BindQueryWithIsNotNaNPredicate)
{
    const std::string queryString = "SELECT a FROM inputStream WHERE b IS NOT NAN INTO outputStream";
    const auto statement = binder->parseAndBindSingle(queryString);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement).plan;
    const auto selectionOperators = getOperatorByType<SelectionLogicalOperator>(plan);
    ASSERT_EQ(selectionOperators.size(), 1);

    const auto predicate = selectionOperators.front()->getPredicate().explain(ExplainVerbosity::Short);
    EXPECT_EQ(predicate, "NOT(ISNAN(B))");
}

TEST_F(StatementBinderTest, BindQueryWithUnsupportedIsTruePredicate)
{
    const std::string queryString = "SELECT a FROM inputStream WHERE b IS TRUE INTO outputStream";
    const auto statement = binder->parseAndBindSingle(queryString);
    ASSERT_FALSE(statement.has_value());
    ASSERT_EQ(statement.error().code(), ErrorCode::UnsupportedQuery);
}

TEST_F(StatementBinderTest, AnonymousSinkQuery)
{
    const std::string query = "SELECT id, text \n"
                              "FROM input\n"
                              "INTO FILE(\n"
                              "'out.csv' AS \"SINK\".FILE_PATH,\n"
                              "'CSV' as \"SINK\".OUTPUT_FORMAT,\n"
                              "SCHEMA(id UINT64, text VARSIZED) AS \"SINK\".\"SCHEMA\")\n";
    const auto statement = binder->parseAndBindSingle(query);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement).plan;

    const auto anonymousSinkOperatorList = getOperatorByType<AnonymousSinkLogicalOperator>(plan);
    ASSERT_EQ(1, anonymousSinkOperatorList.size());

    const auto& anonymousSinkOperator = anonymousSinkOperatorList.at(0);

    ASSERT_EQ(Identifier::parse("FILE"), anonymousSinkOperator->getSinkType());

    const std::unordered_map<Identifier, std::string> expectedSinkConfig
        = {{Identifier::parse("file_path"), "out.csv"}, {Identifier::parse("output_format"), "CSV"}};
    ASSERT_EQ(expectedSinkConfig, anonymousSinkOperator->getSinkConfig());

    const Schema<UnqualifiedUnboundField, Ordered> schema{
        UnqualifiedUnboundField{
            Identifier::parse("ID"), DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE)},
        UnqualifiedUnboundField{
            Identifier::parse("TEXT"), DataTypeProvider::provideDataType(DataType::Type::VARSIZED, DataType::NULLABLE::IS_NULLABLE)}};
    ASSERT_EQ(schema, anonymousSinkOperator->getTargetSchema());
}

TEST_F(StatementBinderTest, AnonymousSourceQuery)
{
    const std::string query = "SELECT id, text \n"
                              "FROM File(\n"
                              "'input.csv' AS \"SOURCE\".FILE_PATH,\n"
                              "'CSV' AS INPUT_FORMATTER.\"TYPE\",\n"
                              "SCHEMA(id UINT64, text VARSIZED) AS \"SOURCE\".\"SCHEMA\")\n"
                              "INTO output\n";
    const auto statement = binder->parseAndBindSingle(query);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement).plan;

    const auto anonymousSourceOperatorList = getOperatorByType<AnonymousSourceLogicalOperator>(plan);
    ASSERT_EQ(1, anonymousSourceOperatorList.size());

    const auto& anonymousSourceOperator = anonymousSourceOperatorList.at(0);

    ASSERT_EQ(Identifier::parse("FILE"), anonymousSourceOperator->getSourceType());

    const std::unordered_map<Identifier, std::string> expectedSourceConfig = {{Identifier::parse("file_path"), "'input.csv'"}};
    ASSERT_EQ(expectedSourceConfig, anonymousSourceOperator->getSourceConfig());

    const std::unordered_map<Identifier, std::string> expectedParserConfig = {{Identifier::parse("type"), "CSV"}};
    ASSERT_EQ(expectedParserConfig, anonymousSourceOperator->getParserConfig());

    const Schema<UnqualifiedUnboundField, Ordered> schema{
        UnqualifiedUnboundField{
            Identifier::parse("ID"), DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE)},
        UnqualifiedUnboundField{
            Identifier::parse("TEXT"), DataTypeProvider::provideDataType(DataType::Type::VARSIZED, DataType::NULLABLE::IS_NULLABLE)}};
    ASSERT_EQ(schema, anonymousSourceOperator->getSourceSchema());
}

TEST_F(StatementBinderTest, BindCreateLogicalSource)
{
    const auto statement
        = binder->parseAndBindSingle("CREATE LOGICAL SOURCE testSource (attribute1 UINT32 NOT NULL, attribute2 VARSIZED NOT NULL)");
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<CreateLogicalSourceStatement>(*statement));
    const auto& created = std::get<CreateLogicalSourceStatement>(*statement);
    auto expectedSchema = Schema<UnqualifiedUnboundField, Ordered>{
        UnqualifiedUnboundField{Identifier::parse("ATTRIBUTE1"), DataType::Type::UINT32},
        UnqualifiedUnboundField{Identifier::parse("ATTRIBUTE2"), DataType::Type::VARSIZED}};
    ASSERT_EQ(created.name, Identifier::parse("TESTSOURCE"));
    ASSERT_EQ(created.schema, expectedSchema);
}

TEST_F(StatementBinderTest, BindCreateLogicalSourceWithQuotedIdentifiersAndNullability)
{
    const auto statement
        = binder->parseAndBindSingle(R"(CREATE LOGICAL SOURCE "testSource" ("attribute1" UINT32, "attribute2" VARSIZED NOT NULL))");
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<CreateLogicalSourceStatement>(*statement));
    const auto& created = std::get<CreateLogicalSourceStatement>(*statement);
    auto expectedSchema = Schema<UnqualifiedUnboundField, Ordered>{
        UnqualifiedUnboundField{
            Identifier::parse("\"attribute1\""),
            DataTypeProvider::provideDataType(DataType::Type::UINT32, DataType::NULLABLE::IS_NULLABLE)},
        UnqualifiedUnboundField{
            Identifier::parse("\"attribute2\""),
            DataTypeProvider::provideDataType(DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE)}};
    ASSERT_EQ(created.name, Identifier::parse("\"testSource\""));
    ASSERT_EQ(created.schema, expectedSchema);
}

TEST_F(StatementBinderTest, BindCreatePhysicalSource)
{
    const auto statement = binder->parseAndBindSingle(
        R"(CREATE PHYSICAL SOURCE FOR testSource TYPE File SET ('/dev/null' AS "SOURCE".FILE_PATH, 'worker-1:8080' AS "SOURCE"."HOST", 'CSV' AS INPUT_FORMATTER."TYPE"))");
    ASSERT_TRUE(statement.has_value()) << statement.error().what();
    ASSERT_TRUE(std::holds_alternative<CreatePhysicalSourceStatement>(*statement));
    const auto& created = std::get<CreatePhysicalSourceStatement>(*statement);
    ASSERT_EQ(created.attachedTo, Identifier::parse("TESTSOURCE"));
    ASSERT_EQ(created.sourceType, Identifier::parse("File"));
    ASSERT_EQ(created.host, Host{"worker-1:8080"});
    /// A source option keeps the form of its SQL literal, so a string keeps its quotes.
    ASSERT_EQ(created.sourceConfig.at(Identifier::parse("FILE_PATH")), "'/dev/null'");
    ASSERT_FALSE(created.sourceConfig.contains(Identifier::parse("HOST")));
    ASSERT_EQ(created.parserConfig.at(Identifier::parse("TYPE")), "CSV");
}

TEST_F(StatementBinderTest, BindCreateSink)
{
    const auto statement = binder->parseAndBindSingle(
        R"(CREATE SINK testSink (attribute1 UINT32, attribute2 VARSIZED) TYPE File SET ('/dev/null' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT))");
    ASSERT_TRUE(statement.has_value()) << statement.error().what();
    ASSERT_TRUE(std::holds_alternative<CreateSinkStatement>(*statement));
    const auto& created = std::get<CreateSinkStatement>(*statement);
    auto expectedSchema = Schema<UnqualifiedUnboundField, Ordered>{
        UnqualifiedUnboundField{
            Identifier::parse("ATTRIBUTE1"), DataTypeProvider::provideDataType(DataType::Type::UINT32, DataType::NULLABLE::IS_NULLABLE)},
        UnqualifiedUnboundField{
            Identifier::parse("ATTRIBUTE2"), DataTypeProvider::provideDataType(DataType::Type::VARSIZED, DataType::NULLABLE::IS_NULLABLE)}};
    ASSERT_EQ(created.name, Identifier::parse("TESTSINK"));
    ASSERT_EQ(created.sinkType, Identifier::parse("File"));
    ASSERT_EQ(created.schema, expectedSchema);
    ASSERT_FALSE(created.host.has_value());
    ASSERT_EQ(created.sinkConfig.at(Identifier::parse("FILE_PATH")), "/dev/null");
    ASSERT_EQ(created.sinkConfig.at(Identifier::parse("OUTPUT_FORMAT")), "CSV");
}

TEST_F(StatementBinderTest, BindCreateSinkRejectsDigitOnlyName)
{
    const auto statement = binder->parseAndBindSingle(R"(CREATE SINK "42" (attribute1 UINT32) TYPE Void)");
    ASSERT_FALSE(statement.has_value());
    ASSERT_EQ(statement.error().code(), ErrorCode::InvalidQuerySyntax);
}

TEST_F(StatementBinderTest, BindCreateModel)
{
    const auto statement
        = binder->parseAndBindSingle("CREATE MODEL identity ('/models/tiny_identity.onnx') INPUT (f1 VARSIZED) OUTPUT (o1 VARSIZED)");
    ASSERT_TRUE(statement.has_value()) << statement.error().what();
    ASSERT_TRUE(std::holds_alternative<CreateModelStatement>(*statement));
    const auto& created = std::get<CreateModelStatement>(*statement);
    ASSERT_EQ(created.name, "IDENTITY");
    ASSERT_EQ(created.path, "/models/tiny_identity.onnx");
    ASSERT_EQ(created.inputs.size(), 1);
    ASSERT_EQ(created.outputs.size(), 1);
}

TEST_F(StatementBinderTest, BindCreateModelRejectsBackendClause)
{
    /// OpenVINO is the only inference backend, so there is no BACKEND clause to select one.
    const auto withBackendClause = binder->parseAndBindSingle(
        "CREATE MODEL backendClause ('/models/tiny_identity.onnx' BACKEND openvino) INPUT (f1 VARSIZED) OUTPUT (o1 VARSIZED)");
    ASSERT_FALSE(withBackendClause.has_value());
}

TEST_F(StatementBinderTest, BindShowSources)
{
    const auto allLogical = binder->parseAndBindSingle("SHOW LOGICAL SOURCES");
    ASSERT_TRUE(allLogical.has_value());
    ASSERT_FALSE(std::get<ShowLogicalSourcesStatement>(*allLogical).name.has_value());

    const auto logicalByName = binder->parseAndBindSingle("SHOW LOGICAL SOURCES WHERE NAME = 'TESTSOURCE1'");
    ASSERT_TRUE(logicalByName.has_value());
    ASSERT_EQ(std::get<ShowLogicalSourcesStatement>(*logicalByName).name, Identifier::parse("TESTSOURCE1"));

    const auto allPhysical = binder->parseAndBindSingle("SHOW PHYSICAL SOURCES");
    ASSERT_TRUE(allPhysical.has_value());
    ASSERT_FALSE(std::get<ShowPhysicalSourcesStatement>(*allPhysical).logicalSource.has_value());
    ASSERT_FALSE(std::get<ShowPhysicalSourcesStatement>(*allPhysical).id.has_value());

    const auto physicalById = binder->parseAndBindSingle("SHOW PHYSICAL SOURCES WHERE ID = 2");
    ASSERT_TRUE(physicalById.has_value());
    ASSERT_FALSE(std::get<ShowPhysicalSourcesStatement>(*physicalById).logicalSource.has_value());
    ASSERT_EQ(std::get<ShowPhysicalSourcesStatement>(*physicalById).id, 2);

    const auto physicalForLogical = binder->parseAndBindSingle("SHOW PHYSICAL SOURCES FOR testSourCe1");
    ASSERT_TRUE(physicalForLogical.has_value());
    ASSERT_EQ(std::get<ShowPhysicalSourcesStatement>(*physicalForLogical).logicalSource, Identifier::parse("TESTSOURCE1"));
    ASSERT_FALSE(std::get<ShowPhysicalSourcesStatement>(*physicalForLogical).id.has_value());

    const auto physicalForLogicalById = binder->parseAndBindSingle("SHOW PHYSICAL SOURCES FOR testSource2 WHERE ID = 3");
    ASSERT_TRUE(physicalForLogicalById.has_value());
    ASSERT_EQ(std::get<ShowPhysicalSourcesStatement>(*physicalForLogicalById).logicalSource, Identifier::parse("TESTSOURCE2"));
    ASSERT_EQ(std::get<ShowPhysicalSourcesStatement>(*physicalForLogicalById).id, 3);
}

TEST_F(StatementBinderTest, BindShowSinksAndModels)
{
    const auto allSinks = binder->parseAndBindSingle("SHOW SINKS");
    ASSERT_TRUE(allSinks.has_value());
    ASSERT_FALSE(std::get<ShowSinksStatement>(*allSinks).name.has_value());

    const auto sinkByName = binder->parseAndBindSingle("SHOW SINKS WHERE NAME = 'TESTSINK'");
    ASSERT_TRUE(sinkByName.has_value());
    ASSERT_EQ(std::get<ShowSinksStatement>(*sinkByName).name, Identifier::parse("TESTSINK"));

    const auto allModels = binder->parseAndBindSingle("SHOW MODELS");
    ASSERT_TRUE(allModels.has_value());
    ASSERT_FALSE(std::get<ShowModelsStatement>(*allModels).name.has_value());

    const auto modelByName = binder->parseAndBindSingle("SHOW MODELS WHERE NAME = 'IDENTITY'");
    ASSERT_TRUE(modelByName.has_value());
    ASSERT_EQ(std::get<ShowModelsStatement>(*modelByName).name, Identifier::parse("IDENTITY"));

    ASSERT_FALSE(binder->parseAndBindSingle("SHOW SINKS FORMAT JSON").has_value());
}

TEST_F(StatementBinderTest, BindShowQueries)
{
    const auto all = binder->parseAndBindSingle("SHOW QUERIES");
    ASSERT_TRUE(all.has_value());
    ASSERT_FALSE(std::get<ShowQueriesStatement>(*all).id.has_value());
    ASSERT_FALSE(std::get<ShowQueriesStatement>(*all).name.has_value());

    const auto byId = binder->parseAndBindSingle("SHOW QUERIES WHERE ID = 7");
    ASSERT_TRUE(byId.has_value());
    ASSERT_EQ(std::get<ShowQueriesStatement>(*byId).id, 7);
    ASSERT_FALSE(std::get<ShowQueriesStatement>(*byId).name.has_value());

    const auto byName = binder->parseAndBindSingle("SHOW QUERIES WHERE NAME = 'someQuery'");
    ASSERT_TRUE(byName.has_value());
    ASSERT_EQ(std::get<ShowQueriesStatement>(*byName).name, Identifier::parse("someQuery"));
    ASSERT_FALSE(std::get<ShowQueriesStatement>(*byName).id.has_value());
}

TEST_F(StatementBinderTest, BindShowWorkers)
{
    const auto all = binder->parseAndBindSingle("SHOW WORKERS");
    ASSERT_TRUE(all.has_value());
    ASSERT_FALSE(std::get<ShowWorkersStatement>(*all).host.has_value());

    const auto byHost = binder->parseAndBindSingle("SHOW WORKERS WHERE HOST = 'localhost:8080'");
    ASSERT_TRUE(byHost.has_value());
    ASSERT_EQ(std::get<ShowWorkersStatement>(*byHost).host, "localhost:8080");
}

TEST_F(StatementBinderTest, BindShowVersion)
{
    const auto statement = binder->parseAndBindSingle("SHOW VERSION");
    ASSERT_TRUE(statement.has_value()) << "Statement could not be parsed" << statement.error();
    ASSERT_TRUE(std::holds_alternative<ShowVersionStatement>(*statement));

    ASSERT_FALSE(binder->parseAndBindSingle("SHOW VERSION WHERE NAME = 'worker'").has_value());
}

TEST_F(StatementBinderTest, BindDropStatements)
{
    const auto physicalSource = binder->parseAndBindSingle("DROP PHYSICAL SOURCE WHERE ID = 1");
    ASSERT_TRUE(physicalSource.has_value());
    ASSERT_EQ(std::get<DropPhysicalSourceStatement>(*physicalSource).id, 1);

    const auto logicalSource = binder->parseAndBindSingle("DROP LOGICAL SOURCE WHERE NAME = 'TESTSOURCE'");
    ASSERT_TRUE(logicalSource.has_value());
    ASSERT_EQ(std::get<DropLogicalSourceStatement>(*logicalSource).source, Identifier::parse("TESTSOURCE"));

    const auto sink = binder->parseAndBindSingle("DROP SINK WHERE NAME = 'TESTSINK'");
    ASSERT_TRUE(sink.has_value());
    ASSERT_EQ(std::get<DropSinkStatement>(*sink).name, Identifier::parse("TESTSINK"));

    const auto everySink = binder->parseAndBindSingle("DROP SINK");
    ASSERT_TRUE(everySink.has_value());
    ASSERT_FALSE(std::get<DropSinkStatement>(*everySink).name.has_value());

    const auto model = binder->parseAndBindSingle("DROP MODEL WHERE NAME = 'IDENTITY'");
    ASSERT_TRUE(model.has_value());
    ASSERT_EQ(std::get<DropModelStatement>(*model).name, "IDENTITY");

    const auto worker = binder->parseAndBindSingle("DROP WORKER WHERE HOST = 'localhost:8080'");
    ASSERT_TRUE(worker.has_value());
    ASSERT_EQ(std::get<DropWorkerStatement>(*worker).host, "localhost:8080");

    const auto everyWorker = binder->parseAndBindSingle("DROP WORKER");
    ASSERT_FALSE(everyWorker.has_value());
    ASSERT_EQ(everyWorker.error().code(), ErrorCode::InvalidQuerySyntax);

    ASSERT_FALSE(binder->parseAndBindSingle("DROP QUERY SET ('x' AS \"QUERY\".\"NAME\")").has_value());
}

TEST_F(StatementBinderTest, SplitStatements)
{
    const auto statements = AntlrSQLQueryParser::splitStatements("SHOW QUERIES; DROP QUERY WHERE NAME = 'a;b';");
    ASSERT_TRUE(statements.has_value());
    ASSERT_EQ(statements->size(), 2);
    EXPECT_EQ(statements->at(0), "SHOW QUERIES");
    EXPECT_EQ(statements->at(1), "DROP QUERY WHERE NAME = 'a;b'");

    const auto empty = AntlrSQLQueryParser::splitStatements("");
    ASSERT_TRUE(empty.has_value());
    EXPECT_TRUE(empty->empty());
}

TEST_F(StatementBinderTest, BindDropQuery)
{
    const auto byId = binder->parseAndBindSingle("DROP QUERY WHERE ID = 42");
    ASSERT_TRUE(byId.has_value());
    ASSERT_TRUE(std::holds_alternative<DropQueryStatement>(*byId));
    ASSERT_EQ(std::get<DropQueryStatement>(*byId).id, 42);
    ASSERT_FALSE(std::get<DropQueryStatement>(*byId).name.has_value());

    const auto byName = binder->parseAndBindSingle("DROP QUERY WHERE NAME = 'someQuery'");
    ASSERT_TRUE(byName.has_value());
    ASSERT_EQ(std::get<DropQueryStatement>(*byName).name, Identifier::parse("someQuery"));
    ASSERT_FALSE(std::get<DropQueryStatement>(*byName).id.has_value());

    /// No filter drops every query, so both selectors stay empty rather than the statement being rejected.
    const auto unfiltered = binder->parseAndBindSingle("DROP QUERY");
    ASSERT_TRUE(unfiltered.has_value());
    ASSERT_FALSE(std::get<DropQueryStatement>(*unfiltered).id.has_value());
    ASSERT_FALSE(std::get<DropQueryStatement>(*unfiltered).name.has_value());

    const auto positional = binder->parseAndBindSingle("DROP QUERY 1");
    ASSERT_FALSE(positional.has_value());
    ASSERT_EQ(positional.error().code(), ErrorCode::InvalidQuerySyntax);
}

TEST_F(StatementBinderTest, ExplainStatement)
{
    const std::vector<std::string_view> validExplainStatement{
        R"(EXPLAIN SELECT * FROM "source" INTO "sink")", R"(explain SELECT * FROM "source" INTO "sink")"};
    const std::vector<std::string_view> matchingQueries{R"(SELECT * FROM "source" INTO "sink")", R"(SELECT * FROM "source" INTO "sink")"};

    for (const auto& [explain, query] : std::views::zip(validExplainStatement, matchingQueries))
    {
        const auto explainStatementResult = binder->parseAndBindSingle(explain);
        const auto queryStatementResult = binder->parseAndBindSingle(query);
        ASSERT_TRUE(explainStatementResult.has_value());
        ASSERT_TRUE(queryStatementResult.has_value());

        auto explainStatement = std::get<ExplainQueryStatement>(explainStatementResult.value());
        auto queryStatement = std::get<QueryStatement>(queryStatementResult.value());
        EXPECT_EQ(explainStatement.plan, queryStatement.plan);
    }

    const std::vector<std::string_view> badExplainStatement{
        "EXPLAIN CREATE SINK testSink1 (attribute1 UINT32, attribute2 VARSIZED) TYPE File SET ('/dev/null' AS \"SINK\".FILE_PATH, 'CSV' AS "
        "\"SINK\".OUTPUT_FORMAT)",
        "EXPLAIN SHOW PHYSICAL SOURCES"};

    for (const auto& explain : badExplainStatement)
    {
        const auto explainStatementResult = binder->parseAndBindSingle(explain);
        ASSERT_FALSE(explainStatementResult.has_value());
    }
}

TEST_F(StatementBinderTest, ExplainStatementStageOptions)
{
    const auto getExplainStatement = [&](std::string_view sql) -> ExplainQueryStatement
    {
        const auto result = binder->parseAndBindSingle(sql);
        EXPECT_TRUE(result.has_value()) << "Failed to parse: " << sql;
        return std::get<ExplainQueryStatement>(result.value());
    };

    EXPECT_EQ(
        getExplainStatement("EXPLAIN SELECT * FROM testSource INTO testSink").explainStages,
        (std::unordered_set<ExplainStage>{ExplainStage::Logical, ExplainStage::Optimized, ExplainStage::Distributed}));
    EXPECT_EQ(
        getExplainStatement("EXPLAIN (LOGICAL) SELECT * FROM testSource INTO testSink").explainStages,
        std::unordered_set{ExplainStage::Logical});
    EXPECT_EQ(
        getExplainStatement("EXPLAIN (OPTIMIZED) SELECT * FROM testSource INTO testSink").explainStages,
        std::unordered_set{ExplainStage::Optimized});
    EXPECT_EQ(
        getExplainStatement("EXPLAIN (DISTRIBUTED) SELECT * FROM testSource INTO testSink").explainStages,
        std::unordered_set{ExplainStage::Distributed});
    EXPECT_EQ(
        getExplainStatement("EXPLAIN (LOGICAL, OPTIMIZED) SELECT * FROM testSource INTO testSink").explainStages,
        (std::unordered_set{ExplainStage::Logical, ExplainStage::Optimized}));

    EXPECT_FALSE(binder->parseAndBindSingle("EXPLAIN (LOGICAL, LOGICAL) SELECT * FROM testSource INTO testSink").has_value());
}

TEST_F(StatementBinderTest, ExplainStatementFormatOptions)
{
    const auto getExplainStatement = [&](std::string_view sql) -> ExplainQueryStatement
    {
        const auto result = binder->parseAndBindSingle(sql);
        EXPECT_TRUE(result.has_value()) << "Failed to parse: " << sql;
        return std::get<ExplainQueryStatement>(result.value());
    };

    EXPECT_EQ(getExplainStatement("EXPLAIN SELECT * FROM testSource INTO testSink").explainFormat, ExplainFormat::Visual);
    EXPECT_EQ(getExplainStatement("EXPLAIN FORMAT VISUAL SELECT * FROM testSource INTO testSink").explainFormat, ExplainFormat::Visual);
    EXPECT_EQ(getExplainStatement("EXPLAIN FORMAT TEXT SELECT * FROM testSource INTO testSink").explainFormat, ExplainFormat::Text);
    EXPECT_EQ(getExplainStatement("EXPLAIN FORMAT VERBOSE SELECT * FROM testSource INTO testSink").explainFormat, ExplainFormat::Verbose);

    const auto duplicateFormat = binder->parseAndBindSingle("EXPLAIN FORMAT TEXT FORMAT VERBOSE SELECT * FROM testSource INTO testSink");
    EXPECT_FALSE(duplicateFormat.has_value());
}

TEST_F(StatementBinderTest, ExplainStatementCombinedOptions)
{
    const auto result = binder->parseAndBindSingle("EXPLAIN (LOGICAL) FORMAT VERBOSE SELECT * FROM testSource INTO testSink");
    ASSERT_TRUE(result.has_value());
    const auto& stmt = std::get<ExplainQueryStatement>(result.value());
    EXPECT_EQ(stmt.explainStages, std::unordered_set{ExplainStage::Logical});
    EXPECT_EQ(stmt.explainFormat, ExplainFormat::Verbose);
}

TEST_F(StatementBinderTest, ExplainStatementCaseInsensitive)
{
    const auto bind = [&](std::string_view sql) { return binder->parseAndBindSingle(sql); };
    EXPECT_TRUE(bind("EXPLAIN (logical) SELECT * FROM testSource INTO testSink").has_value());
    EXPECT_TRUE(bind("EXPLAIN format text SELECT * FROM testSource INTO testSink").has_value());
    EXPECT_TRUE(bind("EXPLAIN (logical) format verbose SELECT * FROM testSource INTO testSink").has_value());
}

TEST_F(StatementBinderTest, CreateWorkerStatementTest)
{
    const std::string statementString
        = R"(CREATE WORKER 'localhost:8080' SET ('localhost:9090' AS "DATA", 32 AS "MAX_OPERATORS", 'localhost2:9090' AS "DOWNSTREAM", 'localhost1:9090' AS "DOWNSTREAM"))";
    const auto statement = binder->parseAndBindSingle(statementString);
    ASSERT_TRUE(statement.has_value()) << "Statement could not be parsed" << statement.error();
    ASSERT_TRUE(std::holds_alternative<CreateWorkerStatement>(*statement));
    ASSERT_EQ(std::get<CreateWorkerStatement>(*statement).host, "localhost:8080");
    ASSERT_EQ(std::get<CreateWorkerStatement>(*statement).dataAddress, "localhost:9090");
    ASSERT_EQ(std::get<CreateWorkerStatement>(*statement).maxOperators, 32U);
}

TEST_F(StatementBinderTest, CreateWorkerStatementFlattensOtherOptions)
{
    const auto statement = binder->parseAndBindSingle(
        R"(CREATE WORKER 'localhost:8080' SET ('localhost:9090' AS DATA, '4' AS "worker"."query_engine"."number_of_worker_threads"))");
    ASSERT_TRUE(statement.has_value()) << "Statement could not be parsed" << statement.error();
    const auto& config = std::get<CreateWorkerStatement>(*statement).config;
    ASSERT_EQ(config.size(), 1);
    ASSERT_EQ(config.at("worker.query_engine.number_of_worker_threads"), "4");
}

TEST_F(StatementBinderTest, CreateLogicalQueryPlanRejectsNonQueryStatements)
{
    EXPECT_THROW(AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString("CREATE WORKER 'localhost:8080';"), Exception);
}

TEST_F(StatementBinderTest, LeftOuterJoinParsesToOuterLeftJoinType)
{
    const std::string query = "SELECT * FROM (SELECT * FROM s1) LEFT OUTER JOIN (SELECT * FROM s2) "
                              "ON s1key = s2key WINDOW TUMBLING(SIZE 1000 MS) INTO sink";
    const auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
    const auto joins = getOperatorByType<JoinLogicalOperator>(plan);
    ASSERT_EQ(1, joins.size());
    EXPECT_EQ(JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN, joins.at(0)->getJoinType());
}

TEST_F(StatementBinderTest, RightOuterJoinParsesToOuterRightJoinType)
{
    const std::string query = "SELECT * FROM (SELECT * FROM s1) RIGHT OUTER JOIN (SELECT * FROM s2) "
                              "ON s1key = s2key WINDOW TUMBLING(SIZE 1000 MS) INTO sink";
    const auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
    const auto joins = getOperatorByType<JoinLogicalOperator>(plan);
    ASSERT_EQ(1, joins.size());
    EXPECT_EQ(JoinLogicalOperator::JoinType::OUTER_RIGHT_JOIN, joins.at(0)->getJoinType());
}

TEST_F(StatementBinderTest, FullOuterJoinParsesToOuterFullJoinType)
{
    const std::string query = "SELECT * FROM (SELECT * FROM s1) FULL OUTER JOIN (SELECT * FROM s2) "
                              "ON s1key = s2key WINDOW TUMBLING(SIZE 1000 MS) INTO sink";
    const auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
    const auto joins = getOperatorByType<JoinLogicalOperator>(plan);
    ASSERT_EQ(1, joins.size());
    EXPECT_EQ(JoinLogicalOperator::JoinType::OUTER_FULL_JOIN, joins.at(0)->getJoinType());
}

TEST_F(StatementBinderTest, JoinWithoutKeywordParsesToInnerJoinType)
{
    const std::string query = "SELECT * FROM (SELECT * FROM s1) JOIN (SELECT * FROM s2) "
                              "ON s1key = s2key WINDOW TUMBLING(SIZE 1000 MS) INTO sink";
    const auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
    const auto joins = getOperatorByType<JoinLogicalOperator>(plan);
    ASSERT_EQ(1, joins.size());
    EXPECT_EQ(JoinLogicalOperator::JoinType::INNER_JOIN, joins.at(0)->getJoinType());
}

TEST_F(StatementBinderTest, InnerJoinParsesToInnerJoinType)
{
    const std::string query = "SELECT * FROM (SELECT * FROM s1) INNER JOIN (SELECT * FROM s2) "
                              "ON s1key = s2key WINDOW TUMBLING(SIZE 1000 MS) INTO sink";
    const auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
    const auto joins = getOperatorByType<JoinLogicalOperator>(plan);
    ASSERT_EQ(1, joins.size());
    EXPECT_EQ(JoinLogicalOperator::JoinType::INNER_JOIN, joins.at(0)->getJoinType());
}

TEST_F(StatementBinderTest, LowercaseOuterJoinParsesToOuterLeftJoinType)
{
    const std::string query = "SELECT * FROM (SELECT * FROM s1) LEFT outer JOIN (SELECT * FROM s2) "
                              "ON s1key = s2key WINDOW TUMBLING(SIZE 1000 MS) INTO sink";
    const auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
    const auto joins = getOperatorByType<JoinLogicalOperator>(plan);
    ASSERT_EQ(1, joins.size());
    EXPECT_EQ(JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN, joins.at(0)->getJoinType());
}

TEST_F(StatementBinderTest, LeftJoinWithoutOuterKeywordParsesToOuterLeftJoinType)
{
    const std::string query = "SELECT * FROM (SELECT * FROM s1) LEFT JOIN (SELECT * FROM s2) "
                              "ON s1key = s2key WINDOW TUMBLING(SIZE 1000 MS) INTO sink";
    const auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
    const auto joins = getOperatorByType<JoinLogicalOperator>(plan);
    ASSERT_EQ(1, joins.size());
    EXPECT_EQ(JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN, joins.at(0)->getJoinType());
}

TEST_F(StatementBinderTest, RightJoinWithoutOuterKeywordParsesToOuterRightJoinType)
{
    const std::string query = "SELECT * FROM (SELECT * FROM s1) RIGHT JOIN (SELECT * FROM s2) "
                              "ON s1key = s2key WINDOW TUMBLING(SIZE 1000 MS) INTO sink";
    const auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
    const auto joins = getOperatorByType<JoinLogicalOperator>(plan);
    ASSERT_EQ(1, joins.size());
    EXPECT_EQ(JoinLogicalOperator::JoinType::OUTER_RIGHT_JOIN, joins.at(0)->getJoinType());
}

TEST_F(StatementBinderTest, FullJoinWithoutOuterKeywordParsesToOuterFullJoinType)
{
    const std::string query = "SELECT * FROM (SELECT * FROM s1) FULL JOIN (SELECT * FROM s2) "
                              "ON s1key = s2key WINDOW TUMBLING(SIZE 1000 MS) INTO sink";
    const auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
    const auto joins = getOperatorByType<JoinLogicalOperator>(plan);
    ASSERT_EQ(1, joins.size());
    EXPECT_EQ(JoinLogicalOperator::JoinType::OUTER_FULL_JOIN, joins.at(0)->getJoinType());
}

TEST_F(StatementBinderTest, LowercaseLeftJoinParsesToOuterLeftJoinType)
{
    const std::string query = "SELECT * FROM (SELECT * FROM s1) left join (SELECT * FROM s2) "
                              "ON s1key = s2key WINDOW TUMBLING(SIZE 1000 MS) INTO sink";
    const auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
    const auto joins = getOperatorByType<JoinLogicalOperator>(plan);
    ASSERT_EQ(1, joins.size());
    EXPECT_EQ(JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN, joins.at(0)->getJoinType());
}

TEST_F(StatementBinderTest, LowercaseRightJoinParsesToOuterRightJoinType)
{
    const std::string query = "SELECT * FROM (SELECT * FROM s1) right join (SELECT * FROM s2) "
                              "ON s1key = s2key WINDOW TUMBLING(SIZE 1000 MS) INTO sink";
    const auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
    const auto joins = getOperatorByType<JoinLogicalOperator>(plan);
    ASSERT_EQ(1, joins.size());
    EXPECT_EQ(JoinLogicalOperator::JoinType::OUTER_RIGHT_JOIN, joins.at(0)->getJoinType());
}

TEST_F(StatementBinderTest, LowercaseFullJoinParsesToOuterFullJoinType)
{
    const std::string query = "SELECT * FROM (SELECT * FROM s1) full join (SELECT * FROM s2) "
                              "ON s1key = s2key WINDOW TUMBLING(SIZE 1000 MS) INTO sink";
    const auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
    const auto joins = getOperatorByType<JoinLogicalOperator>(plan);
    ASSERT_EQ(1, joins.size());
    EXPECT_EQ(JoinLogicalOperator::JoinType::OUTER_FULL_JOIN, joins.at(0)->getJoinType());
}

TEST_F(StatementBinderTest, BindQueryWithUnsupportedPredicateNamesEveryKeywordOfThePredicate)
{
    const std::vector<std::pair<std::string, std::string>> predicates{
        {"b IS TRUE", "IS TRUE is not supported"},
        {"b IS NOT TRUE", "IS NOT TRUE is not supported"},
        {"b IS DISTINCT FROM UINT32(5)", "IS DISTINCT FROM is not supported"},
        {"b LIKE VARSIZED('a%')", "LIKE is not supported"},
        {"b NOT LIKE VARSIZED('a%')", "NOT LIKE is not supported"}};

    for (const auto& [predicate, expectedMessage] : predicates)
    {
        SCOPED_TRACE(predicate);
        const auto statement = binder->parseAndBindSingle(fmt::format("SELECT a FROM inputStream WHERE {} INTO outputStream", predicate));
        ASSERT_FALSE(statement.has_value());
        EXPECT_EQ(statement.error().code(), ErrorCode::UnsupportedQuery);
        EXPECT_TRUE(std::string{statement.error().what()}.contains(expectedMessage));
    }
}

///NOLINTEND(bugprone-unchecked-optional-access)
}
}
