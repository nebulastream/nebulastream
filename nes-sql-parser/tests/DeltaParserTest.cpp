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

#include <string>
#include <variant>
#include <vector>

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/DeltaLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

class DeltaParserTest : public Testing::BaseUnitTest
{
public:
    std::shared_ptr<SourceCatalog> sourceCatalog;
    std::shared_ptr<StatementBinder> binder;

    static void SetUpTestSuite() { Logger::setupLogging("DeltaParserTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        sourceCatalog = std::make_shared<SourceCatalog>();
        binder = std::make_shared<StatementBinder>(
            sourceCatalog,
            [](auto&& queryContext)
            { return AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(queryContext)>(queryContext)); });
    }
};

/// Single delta expression with alias
TEST_F(DeltaParserTest, SingleExpressionWithAlias)
{
    const std::string query = "SELECT delta_value FROM DELTA(stream, value AS delta_value) INTO output";
    const auto statement = binder->parseAndBindSingle(query);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement);
    const auto deltaOps = getOperatorByType<DeltaLogicalOperator>(plan);
    ASSERT_EQ(1u, deltaOps.size());

    const auto& exprs = deltaOps.at(0)->getDeltaExpressions();
    ASSERT_EQ(1u, exprs.size());
    EXPECT_TRUE(exprs[0].first.getFieldName() == "DELTA_VALUE");
}

/// Multiple delta expressions with aliases
TEST_F(DeltaParserTest, MultipleExpressionsWithAlias)
{
    const std::string query = "SELECT delta_id, delta_value FROM DELTA(stream, id AS delta_id, value AS delta_value) INTO output";
    const auto statement = binder->parseAndBindSingle(query);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement);
    const auto deltaOps = getOperatorByType<DeltaLogicalOperator>(plan);
    ASSERT_EQ(1u, deltaOps.size());

    const auto& exprs = deltaOps.at(0)->getDeltaExpressions();
    ASSERT_EQ(2u, exprs.size());
    EXPECT_TRUE(exprs[0].first.getFieldName() == "DELTA_ID");
    EXPECT_TRUE(exprs[1].first.getFieldName() == "DELTA_VALUE");
}

/// Nested delta: second-order difference via subquery
TEST_F(DeltaParserTest, NestedDeltaSubquery)
{
    const std::string query
        = "SELECT delta2 FROM DELTA((SELECT id, delta_value FROM DELTA(stream, value AS delta_value)), delta_value AS delta2) INTO output";
    const auto statement = binder->parseAndBindSingle(query);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement);
    const auto deltaOps = getOperatorByType<DeltaLogicalOperator>(plan);
    /// Two delta operators: one from the inner query and one from the outer
    ASSERT_EQ(2u, deltaOps.size());
}

/// Nested delta with alias at both levels
TEST_F(DeltaParserTest, NestedDeltaWithAliases)
{
    const std::string query = "SELECT delta2 FROM DELTA("
                              "(SELECT delta_value FROM DELTA(stream, value AS delta_value))"
                              ", delta_value AS delta2) INTO output";
    const auto statement = binder->parseAndBindSingle(query);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement);
    const auto deltaOps = getOperatorByType<DeltaLogicalOperator>(plan);
    ASSERT_EQ(2u, deltaOps.size());

    /// The outermost delta (first in BFS order from root) should have the alias delta2
    const auto& outerExprs = deltaOps.at(0)->getDeltaExpressions();
    ASSERT_EQ(1u, outerExprs.size());
    EXPECT_TRUE(outerExprs[0].first.getFieldName() == "DELTA2");
}

/// Triple-nested delta: third-order difference
TEST_F(DeltaParserTest, TripleNestedDelta)
{
    const std::string query = "SELECT delta3 FROM DELTA("
                              "(SELECT delta2 FROM DELTA("
                              "(SELECT delta1 FROM DELTA(stream, value AS delta1))"
                              ", delta1 AS delta2))"
                              ", delta2 AS delta3) INTO output";
    const auto statement = binder->parseAndBindSingle(query);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement);
    const auto deltaOps = getOperatorByType<DeltaLogicalOperator>(plan);
    ASSERT_EQ(3u, deltaOps.size());
}

/// Nested delta with multiple columns projected from inner query
TEST_F(DeltaParserTest, NestedDeltaMultipleProjections)
{
    const std::string query = "SELECT timestamp, delta2 FROM DELTA("
                              "(SELECT timestamp, delta_value FROM DELTA(stream, value AS delta_value))"
                              ", delta_value AS delta2) INTO output";
    const auto statement = binder->parseAndBindSingle(query);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement);
    const auto deltaOps = getOperatorByType<DeltaLogicalOperator>(plan);
    ASSERT_EQ(2u, deltaOps.size());
}

/// Nested delta with multiple delta expressions at both levels
TEST_F(DeltaParserTest, NestedDeltaMultipleExpressionsAtBothLevels)
{
    const std::string query = "SELECT delta_did, delta_dval FROM DELTA("
                              "(SELECT delta_id, delta_value FROM DELTA(stream, id AS delta_id, value AS delta_value))"
                              ", delta_id AS delta_did, delta_value AS delta_dval) INTO output";
    const auto statement = binder->parseAndBindSingle(query);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement);
    const auto deltaOps = getOperatorByType<DeltaLogicalOperator>(plan);
    ASSERT_EQ(2u, deltaOps.size());

    /// Inner delta has 2 expressions
    const auto& innerExprs = deltaOps.at(1)->getDeltaExpressions();
    ASSERT_EQ(2u, innerExprs.size());

    /// Outer delta has 2 expressions
    const auto& outerExprs = deltaOps.at(0)->getDeltaExpressions();
    ASSERT_EQ(2u, outerExprs.size());
    EXPECT_TRUE(outerExprs[0].first.getFieldName() == "DELTA_DID");
    EXPECT_TRUE(outerExprs[1].first.getFieldName() == "DELTA_DVAL");
}

/// Delta expression without alias should fail to parse
TEST_F(DeltaParserTest, RejectsMissingAlias)
{
    const std::string query = "SELECT value FROM DELTA(stream, value) INTO output";
    const auto statement = binder->parseAndBindSingle(query);
    ASSERT_FALSE(statement.has_value());
    EXPECT_EQ(statement.error().code(), ErrorCode::InvalidQuerySyntax);
}

/// Arithmetic expression in delta should fail to parse
TEST_F(DeltaParserTest, RejectsArithmeticExpression)
{
    const std::string query = "SELECT delta_sum FROM DELTA(stream, value + id AS delta_sum) INTO output";
    const auto statement = binder->parseAndBindSingle(query);
    ASSERT_FALSE(statement.has_value());
    EXPECT_EQ(statement.error().code(), ErrorCode::InvalidQuerySyntax);
}

/// Constant expression in delta should fail to parse
TEST_F(DeltaParserTest, RejectsConstantExpression)
{
    const std::string query = "SELECT delta_val FROM DELTA(stream, UINT32(4) AS delta_val) INTO output";
    const auto statement = binder->parseAndBindSingle(query);
    ASSERT_FALSE(statement.has_value());
    EXPECT_EQ(statement.error().code(), ErrorCode::InvalidQuerySyntax);
}

}
}
