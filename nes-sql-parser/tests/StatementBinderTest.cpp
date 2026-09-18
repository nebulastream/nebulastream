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
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <cstdint>
#include <string_view>
#include <tuple>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/StatisticIdentifiers.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/AnonymousSinkLogicalOperator.hpp>
#include <Operators/Sources/AnonymousSourceLogicalOperator.hpp>
#include <Operators/Statistic/StatisticBlobType.hpp>
#include <Operators/Statistic/StatisticFieldNames.hpp>
#include <Operators/Statistic/StatisticStoreReaderLogicalOperator.hpp>
#include <Operators/Statistic/StatisticStoreWriterLogicalOperator.hpp>
#include <Operators/Statistic/StatisticWindowMatch.hpp>
#include <Operators/Windows/Aggregations/AggregationLogicalFunctionProvider.hpp>
#include <Operators/Windows/Aggregations/AggregationParameters.hpp>
#include <Operators/Windows/Aggregations/CountAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/SumAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Sinks/FileSink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterValidationProvider.hpp>

namespace NES
{
template <bool IsStatistic>
class TestAggregationLogicalFunction
{
public:
    static constexpr std::string_view NAME = IsStatistic ? "TestSynopsis" : "TestAggregation";
    static constexpr bool IS_STATISTIC = IsStatistic;

    TestAggregationLogicalFunction(AggregationFieldAccess inputFunction, const uint64_t parameter)
        : inputFunction(std::move(inputFunction)), parameter(parameter)
    {
    }

    [[nodiscard]] TestAggregationLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const
    {
        const TypedLogicalFunction<FieldAccessLogicalFunction> inferred{FieldAccessLogicalFunction{*schema.begin()}};
        return TestAggregationLogicalFunction{inferred, parameter};
    }

    [[nodiscard]] std::string_view getName() const noexcept { return NAME; }

    [[nodiscard]] static DataType getAggregateType() { return DataTypeProvider::provideDataType(DataType::Type::UINT64); }

    [[nodiscard]] AggregationFieldAccess getInputFunction() const { return inputFunction; }

    [[nodiscard]] static bool shallIncludeNullValues() noexcept { return true; }

    [[nodiscard]] uint64_t getParameter() const { return parameter; }

    [[nodiscard]] std::string explain(ExplainVerbosity) const { return fmt::format("{}({})", NAME, parameter); }

    [[nodiscard]] bool operator==(const TestAggregationLogicalFunction& other) const
    {
        return inputFunction == other.inputFunction and parameter == other.parameter;
    }

    /// Plain: NAME(field, parameter). Synopsis: NAME(statisticId, parameter), the parser strips the id first.
    static AggregationLogicalFunctionRegistryReturnType create(AggregationLogicalFunctionRegistryArguments arguments)
    {
        const auto& parameters = arguments.parameters;
        if (parameters.size() != (IsStatistic ? 1 : 2))
        {
            throw InvalidQuerySyntax("{} got {} arguments", NAME, parameters.size());
        }
        const auto parameter = parseUnsignedParameter(parameters.back(), fmt::format("the parameter of {}", NAME));
        if constexpr (IsStatistic)
        {
            const TypedLogicalFunction<UnboundFieldAccessLogicalFunction> placeholder{
                UnboundFieldAccessLogicalFunction{Identifier::parse("TESTSYNOPSISINPUT")}};
            return TestAggregationLogicalFunction{placeholder, parameter};
        }
        else
        {
            return TestAggregationLogicalFunction{parseFieldParameter(parameters.front(), fmt::format("the field of {}", NAME)), parameter};
        }
    }

private:
    AggregationFieldAccess inputFunction;
    uint64_t parameter;
};

using TestPlainAggregationLogicalFunction = TestAggregationLogicalFunction<false>;
using TestSynopsisAggregationLogicalFunction = TestAggregationLogicalFunction<true>;

template <bool IsStatistic>
struct Reflector<TestAggregationLogicalFunction<IsStatistic>>
{
    Reflected operator()(const TestAggregationLogicalFunction<IsStatistic>& function, const ReflectionContext& context) const
    {
        return context.reflect(function.getInputFunction());
    }
};
}

template <bool IsStatistic>
struct std::hash<NES::TestAggregationLogicalFunction<IsStatistic>>
{
    size_t operator()(const NES::TestAggregationLogicalFunction<IsStatistic>& function) const noexcept
    {
        return folly::hash::hash_combine(function.getInputFunction(), function.getParameter());
    }
};

static_assert(NES::WindowAggregationFunctionConcept<NES::TestPlainAggregationLogicalFunction>);
static_assert(NES::WindowAggregationFunctionConcept<NES::TestSynopsisAggregationLogicalFunction>);

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
        /// The registry is process-wide; a second suite in the same binary finds the entries already there.
        std::ignore = AggregationLogicalFunctionRegistry::instance().addEntry(
            std::string{TestPlainAggregationLogicalFunction::NAME},
            makeAggregationLogicalFunctionEntry<TestPlainAggregationLogicalFunction>());
        std::ignore = AggregationLogicalFunctionRegistry::instance().addEntry(
            std::string{TestSynopsisAggregationLogicalFunction::NAME},
            makeAggregationLogicalFunctionEntry<TestSynopsisAggregationLogicalFunction>());
    }

    [[nodiscard]] LogicalPlan bindPlan(const std::string& query) const
    {
        const auto statement = binder->parseAndBindSingle(query);
        if (not statement.has_value())
        {
            throw std::runtime_error(fmt::format("{} did not bind: {}", query, statement.error().what()));
        }
        return std::get<QueryStatement>(*statement).plan;
    }

    [[nodiscard]] std::optional<ErrorCode> bindError(const std::string& query) const
    {
        const auto statement = binder->parseAndBindSingle(query);
        if (statement.has_value())
        {
            return std::nullopt;
        }
        return statement.error().code();
    }

    [[nodiscard]] std::string bindErrorMessage(const std::string& query) const
    {
        const auto statement = binder->parseAndBindSingle(query);
        return statement.has_value() ? std::string{} : std::string{statement.error().what()};
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

    const std::unordered_map<Identifier, std::string> expectedSourceConfig = {{Identifier::parse("file_path"), "input.csv"}};
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
        = R"(CREATE WORKER 'localhost:8080' SET ('localhost:9090' AS "DATA", 32 AS "CAPACITY", 'localhost2:9090' AS "DOWNSTREAM", 'localhost1:9090' AS "DOWNSTREAM"))";
    const auto statement = binder->parseAndBindSingle(statementString);
    ASSERT_TRUE(statement.has_value()) << "Statement could not be parsed" << statement.error();
    ASSERT_TRUE(std::holds_alternative<CreateWorkerStatement>(*statement));
    ASSERT_EQ(std::get<CreateWorkerStatement>(*statement).host, "localhost:8080");
    ASSERT_EQ(std::get<CreateWorkerStatement>(*statement).dataAddress, "localhost:9090");
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

constexpr std::string_view WINDOWED = " FROM s WINDOW TUMBLING(ts, SIZE 1 SEC) INTO sink";

TEST_F(StatementBinderTest, StatisticBuildWrapsScalarAggregation)
{
    const auto plan = bindPlan(fmt::format("SELECT STATISTIC_BUILD(43, SUM(x)){}", WINDOWED));
    const auto writers = getOperatorByType<StatisticStoreWriterLogicalOperator>(plan);
    ASSERT_EQ(writers.size(), 1);
    EXPECT_EQ(writers.front()->getStatisticId(), StatisticId(43));
    EXPECT_EQ(writers.front()->getTypeName(), StatisticBlobType{"Sum"});
    const auto aggregations = getOperatorByType<WindowedAggregationLogicalOperator>(plan);
    ASSERT_EQ(aggregations.size(), 1);
    const auto projected = aggregations.front()->getWindowAggregation();
    ASSERT_EQ(projected.size(), 2);
    EXPECT_TRUE(projected.at(0).function.tryGetAs<SumAggregationLogicalFunction>().has_value());
    EXPECT_TRUE(projected.at(1).function.tryGetAs<CountAggregationLogicalFunction>().has_value());
    EXPECT_EQ(projected.at(1).name, Identifier::parse(std::string{StatisticFieldNames::NUMBER_OF_SEEN_MEASUREMENTS}));
}

TEST_F(StatementBinderTest, StatisticBuildWrapsCountStar)
{
    const auto plan = bindPlan(fmt::format("SELECT STATISTIC_BUILD(46, COUNT(*)){}", WINDOWED));
    const auto writers = getOperatorByType<StatisticStoreWriterLogicalOperator>(plan);
    ASSERT_EQ(writers.size(), 1);
    EXPECT_EQ(writers.front()->getTypeName(), StatisticBlobType{"Count"});
}

TEST_F(StatementBinderTest, StatisticBuildDesugarsExpressionArgument)
{
    const auto plan = bindPlan(fmt::format("SELECT STATISTIC_BUILD(43, SUM(x + UINT64(1))){}", WINDOWED));
    ASSERT_EQ(getOperatorByType<StatisticStoreWriterLogicalOperator>(plan).size(), 1);
    EXPECT_FALSE(getOperatorByType<ProjectionLogicalOperator>(plan).empty());
}

TEST_F(StatementBinderTest, RegisteredSynopsisIsBuiltFlat)
{
    for (const auto* call : {"TESTSYNOPSIS(42, 100)", "testsynopsis(42, 100)", "TESTSYNOPSIS(42, UINT64(100))"})
    {
        const auto plan = bindPlan(fmt::format("SELECT {}{}", call, WINDOWED));
        const auto writers = getOperatorByType<StatisticStoreWriterLogicalOperator>(plan);
        ASSERT_EQ(writers.size(), 1) << call;
        EXPECT_EQ(writers.front()->getStatisticId(), StatisticId(42)) << call;
        EXPECT_EQ(writers.front()->getTypeName(), StatisticBlobType{"TestSynopsis"}) << call;
        const auto aggregations = getOperatorByType<WindowedAggregationLogicalOperator>(plan);
        ASSERT_EQ(aggregations.size(), 1) << call;
        const auto projected = aggregations.front()->getWindowAggregation();
        ASSERT_EQ(projected.size(), 2) << call;
        const auto synopsis = projected.at(0).function.tryGetAs<TestSynopsisAggregationLogicalFunction>();
        ASSERT_TRUE(synopsis.has_value()) << call;
        EXPECT_EQ(synopsis.value()->getParameter(), 100) << call;
        EXPECT_EQ(projected.at(1).name, Identifier::parse(std::string{StatisticFieldNames::NUMBER_OF_SEEN_MEASUREMENTS})) << call;
    }
}

TEST_F(StatementBinderTest, RegisteredAggregationByNameIsAnOrdinaryAggregation)
{
    const auto plan = bindPlan(fmt::format("SELECT TESTAGGREGATION(x, 5){}", WINDOWED));
    EXPECT_TRUE(getOperatorByType<StatisticStoreWriterLogicalOperator>(plan).empty());
    const auto aggregations = getOperatorByType<WindowedAggregationLogicalOperator>(plan);
    ASSERT_EQ(aggregations.size(), 1);
    const auto projected = aggregations.front()->getWindowAggregation();
    ASSERT_EQ(projected.size(), 1);
    const auto function = projected.front().function.tryGetAs<TestPlainAggregationLogicalFunction>();
    ASSERT_TRUE(function.has_value());
    EXPECT_EQ(function.value()->getParameter(), 5);
    EXPECT_EQ(projected.front().name, Identifier::parse("X_TESTAGGREGATION"));

    const auto renamed = bindPlan(fmt::format("SELECT TESTAGGREGATION(x, 5) AS agg{}", WINDOWED));
    const auto renamedProjected = getOperatorByType<WindowedAggregationLogicalOperator>(renamed).front()->getWindowAggregation();
    ASSERT_EQ(renamedProjected.size(), 1);
    EXPECT_EQ(renamedProjected.front().name, Identifier::parse("AGG"));
}

TEST_F(StatementBinderTest, TokenAggregationsKeepTheirAutoName)
{
    const auto plan = bindPlan(fmt::format("SELECT SUM(x){}", WINDOWED));
    const auto projected = getOperatorByType<WindowedAggregationLogicalOperator>(plan).front()->getWindowAggregation();
    ASSERT_EQ(projected.size(), 1);
    EXPECT_EQ(projected.front().name, Identifier::parse("X_SUM"));
    EXPECT_NO_THROW(std::ignore = bindPlan(fmt::format("SELECT SUM(x) + UINT64(1){}", WINDOWED)));
}

TEST_F(StatementBinderTest, CountStarCountsEveryRecord)
{
    for (const auto* call : {"COUNT(*)", "count(*)", "COUNT(x)", "count(x)"})
    {
        const auto plan = bindPlan(fmt::format("SELECT {}{}", call, WINDOWED));
        const auto projected = getOperatorByType<WindowedAggregationLogicalOperator>(plan).front()->getWindowAggregation();
        ASSERT_EQ(projected.size(), 1) << call;
        const auto count = projected.front().function.tryGetAs<CountAggregationLogicalFunction>();
        ASSERT_TRUE(count.has_value()) << call;
        EXPECT_EQ(count.value()->shallIncludeNullValues(), std::string_view{call}.contains('*')) << call;
    }
}

TEST_F(StatementBinderTest, OnlyCountAcceptsStar)
{
    const LogicalFunction star{UnboundFieldAccessLogicalFunction{Identifier::parse("*")}};
    const auto count = AggregationLogicalFunctionProvider::provide("COUNT", {.parameters = {star}});
    ASSERT_TRUE(count.tryGetAs<CountAggregationLogicalFunction>().has_value());
    EXPECT_TRUE(count.tryGetAs<CountAggregationLogicalFunction>().value()->shallIncludeNullValues());
    ASSERT_EXCEPTION_ERRORCODE(
        std::ignore = AggregationLogicalFunctionProvider::provide("SUM", {.parameters = {star}}), ErrorCode::InvalidQuerySyntax);
}

/// Like COUNT and count, but never Count, a name is recognised in upper or in lower case only.
TEST_F(StatementBinderTest, AggregationAndStatisticNamesAreKeywordCased)
{
    for (const auto* select : {"testaggregation(x, 5)", "statistic_build(43, sum(x))"})
    {
        EXPECT_NO_THROW(std::ignore = bindPlan(fmt::format("SELECT {}{}", select, WINDOWED))) << select;
    }
    for (const auto* select :
         {"Count(*)", "Count(x)", "Sum(x)", "TestAggregation(x, 5)", "TestSynopsis(42, 100)", "Statistic_Build(43, SUM(x))"})
    {
        const auto query = fmt::format("SELECT {}{}", select, WINDOWED);
        EXPECT_EQ(bindError(query), ErrorCode::InvalidQuerySyntax) << query;
        EXPECT_THAT(bindErrorMessage(query), ::testing::HasSubstr("has to be written")) << query;
    }
    EXPECT_THAT(
        bindErrorMessage(fmt::format("SELECT Count(x){}", WINDOWED)), ::testing::HasSubstr("Count has to be written COUNT or count"));
    const std::string probe = "SELECT Sum_Probe(42, v, uint64) FROM s INTO sink";
    EXPECT_EQ(bindError(probe), ErrorCode::InvalidQuerySyntax);
    EXPECT_THAT(bindErrorMessage(probe), ::testing::HasSubstr("has to be written SUM_PROBE or sum_probe"));
}

TEST_F(StatementBinderTest, ProbeReadsAnyRegisteredAggregation)
{
    const auto synopsisPlan = bindPlan("SELECT TESTSYNOPSIS_PROBE(42, v, uint64, w, float64) FROM s INTO sink");
    const auto synopsisReaders = getOperatorByType<StatisticStoreReaderLogicalOperator>(synopsisPlan);
    ASSERT_EQ(synopsisReaders.size(), 1);
    EXPECT_EQ(synopsisReaders.front()->getStatisticId(), StatisticId(42));
    EXPECT_EQ(synopsisReaders.front()->getTypeName(), StatisticBlobType{"TestSynopsis"});
    EXPECT_EQ(synopsisReaders.front()->getWindowMatch(), StatisticWindowMatch::ExactWindow);
    ASSERT_EQ(synopsisReaders.front()->getPayloadFields().size(), 2);
    EXPECT_EQ(synopsisReaders.front()->getPayloadFields().at(1).second, DataTypeProvider::provideDataType(DataType::Type::FLOAT64));

    const auto scalarPlan = bindPlan("SELECT sum_probe_range(43, total, float64) FROM s INTO sink");
    const auto scalarReaders = getOperatorByType<StatisticStoreReaderLogicalOperator>(scalarPlan);
    ASSERT_EQ(scalarReaders.size(), 1);
    EXPECT_EQ(scalarReaders.front()->getTypeName(), StatisticBlobType{"Sum"});
    EXPECT_EQ(scalarReaders.front()->getWindowMatch(), StatisticWindowMatch::WithinRange);
    ASSERT_EQ(scalarReaders.front()->getPayloadFields().size(), 1);
}

TEST_F(StatementBinderTest, StatisticCallsWithBadArgumentsAreInvalidQuerySyntax)
{
    const std::vector<std::string> selects{
        "TESTSYNOPSIS()",
        "TESTSYNOPSIS(0, 100)",
        "TESTSYNOPSIS('a', 100)",
        "TESTSYNOPSIS(42)",
        "TESTSYNOPSIS(42, x)",
        "TESTSYNOPSIS(42, 1, 2)",
        "STATISTIC_BUILD(43, TESTSYNOPSIS(42, 100))",
        "STATISTIC_BUILD(43, x)",
        "STATISTIC_BUILD(43, SUM(x) + UINT64(1))",
        "STATISTIC_BUILD(0, SUM(x))",
        "STATISTIC_BUILD(SUM(x), 43)",
        "STATISTIC_BUILD(42, 'AVG', x)",
        "STATISTIC_BUILD(43, SUM(x)), AVG(y)",
        "STATISTIC_BUILD(1, SUM(x)), STATISTIC_BUILD(2, SUM(y))",
        "TESTSYNOPSIS(1, 10), TESTSYNOPSIS(2, 10)",
        "RESERVOIR(42, 100)",
    };
    for (const auto& select : selects)
    {
        const auto query = fmt::format("SELECT {}{}", select, WINDOWED);
        EXPECT_EQ(bindError(query), ErrorCode::InvalidQuerySyntax) << query;
    }
    EXPECT_EQ(bindError("SELECT STATISTIC_BUILD(43, SUM(x)) FROM s INTO sink"), ErrorCode::InvalidQuerySyntax) << "build without window";

    const std::vector<std::string> probes{
        "FOO_PROBE(42, v, uint64)",
        "SUM_PROBE(42)",
        "SUM_PROBE(42, v)",
        "SUM_PROBE(42, v, notatype)",
        "SUM_PROBE(0, v, uint64)",
        "STATISTIC_PROBE(42, 'Sum', v, float64)",
        "TESTSYNOPSIS_PROBE(1, v, uint64), SUM_PROBE(2, w, uint64)",
    };
    for (const auto& probe : probes)
    {
        const auto query = fmt::format("SELECT {} FROM s INTO sink", probe);
        EXPECT_EQ(bindError(query), ErrorCode::InvalidQuerySyntax) << query;
    }
}

/// STATISTIC_BUILD takes the aggregation call it wraps, not a field that happens to carry an aggregation's output name.
TEST_F(StatementBinderTest, StatisticBuildDoesNotTakeAnUnrelatedAggregation)
{
    for (const auto* select : {"MAX(a), STATISTIC_BUILD(7, a_MAX)", "MAX(a) AS m, STATISTIC_BUILD(7, m)"})
    {
        const auto query = fmt::format("SELECT {}{}", select, WINDOWED);
        EXPECT_EQ(bindError(query), ErrorCode::InvalidQuerySyntax) << query;
        EXPECT_THAT(bindErrorMessage(query), ::testing::HasSubstr("expects an aggregation call as its second argument")) << query;
    }
}

TEST_F(StatementBinderTest, WrappedSynopsisMessageOnlyNamesAWrappedSynopsis)
{
    constexpr std::string_view wrappedSynopsis = "is a statistic synopsis and is written without STATISTIC_BUILD";
    for (const auto* select : {"STATISTIC_BUILD(2, TESTSYNOPSIS(1, 100))", "MAX(a), STATISTIC_BUILD(2, TESTSYNOPSIS(1, 100))"})
    {
        const auto query = fmt::format("SELECT {}{}", select, WINDOWED);
        EXPECT_EQ(bindError(query), ErrorCode::InvalidQuerySyntax) << query;
        EXPECT_THAT(bindErrorMessage(query), ::testing::HasSubstr(std::string{wrappedSynopsis})) << query;
    }
    const auto afterBuild = fmt::format("SELECT STATISTIC_BUILD(1, SUM(x)), STATISTIC_BUILD(2, y){}", WINDOWED);
    EXPECT_EQ(bindError(afterBuild), ErrorCode::InvalidQuerySyntax);
    EXPECT_THAT(bindErrorMessage(afterBuild), ::testing::Not(::testing::HasSubstr(std::string{wrappedSynopsis})));
    const auto twoBuilds = fmt::format("SELECT STATISTIC_BUILD(1, SUM(x)), STATISTIC_BUILD(2, SUM(y)){}", WINDOWED);
    EXPECT_THAT(bindErrorMessage(twoBuilds), ::testing::HasSubstr("Only one statistic build"));
}

///NOLINTEND(bugprone-unchecked-optional-access)
}
}
