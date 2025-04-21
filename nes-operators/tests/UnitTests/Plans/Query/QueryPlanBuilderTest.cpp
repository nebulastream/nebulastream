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

#include <iostream>
#include <memory>
#include <vector>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunctions/EqualsLogicalFunction.hpp>
#include <Operators/MapLogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Plans/QueryPlanBuilder.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

using namespace NES;

class QueryPlanBuilderTest : public Testing::BaseUnitTest
{
public:
    /* Will be called before a test is executed. */
    void SetUp() override
    {
        NES::Logger::setupLogging("QueryPlanBuilderTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryPlanTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Setup QueryPlanBuilderTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down QueryPlanBuilderTest test class."); }
};

/// Helper functions
namespace
{
void assertSelectionOperator(const QueryPlan& qp, const std::string& expectedLeftField, const std::string& expectedRightField)
{
    auto selectionOperators = qp.getOperatorByType<SelectionLogicalOperator>();
    ASSERT_EQ(selectionOperators.size(), 1) << "Expected exactly one selection operator.";
    auto predicate = selectionOperators[0].getPredicate();
    auto equalsFunc = predicate.get<EqualsLogicalFunction>();

    auto leftField = equalsFunc.getChildren()[0].get<FieldAccessLogicalFunction>();
    auto rightField = equalsFunc.getChildren()[1].get<FieldAccessLogicalFunction>();

    EXPECT_EQ(leftField.getFieldName(), expectedLeftField) << "Left field name mismatch.";
    EXPECT_EQ(rightField.getFieldName(), expectedRightField) << "Right field name mismatch.";
}

void assertProjectionOperator(const QueryPlan& qp, const std::vector<std::string>& expectedFields)
{
    auto projectionOperators = qp.getOperatorByType<ProjectionLogicalOperator>();
    ASSERT_EQ(projectionOperators.size(), 1) << "Expected exactly one projection operator.";
    const auto& functions = projectionOperators[0].getFunctions();
    ASSERT_EQ(functions.size(), expectedFields.size()) << "Projection functions count mismatch.";

    for (size_t i = 0; i < functions.size(); ++i)
    {
        auto fieldFunc = functions[i].get<FieldAccessLogicalFunction>();
        EXPECT_EQ(fieldFunc.getFieldName(), expectedFields[i]) << "Projection field name mismatch at index " << i;
    }
}

void assertMapOperator(const QueryPlan& qp, const std::string& expectedField, const std::string& expectedValue)
{
    auto mapOperators = qp.getOperatorByType<MapLogicalOperator>();
    ASSERT_EQ(mapOperators.size(), 1) << "Expected exactly one map operator.";
    const auto& assignment = mapOperators[0].getMapFunction();
    auto* lhs = &assignment.getField();
    auto* rhs = &assignment.getAssignment().get<ConstantValueLogicalFunction>();
    ASSERT_NE(lhs, nullptr) << "Map left-hand side is not a FieldAccessLogicalFunction.";
    ASSERT_NE(rhs, nullptr) << "Map right-hand side is not a ConstantValueLogicalFunction.";

    EXPECT_EQ(lhs->getFieldName(), expectedField) << "Map field name mismatch.";
    EXPECT_EQ(rhs->getConstantValue(), expectedValue) << "Map constant value mismatch.";
}

void assertUnionOperator(const QueryPlan& qp)
{
    auto unionOperators = qp.getOperatorByType<UnionLogicalOperator>();
    ASSERT_EQ(unionOperators.size(), 1) << "Expected exactly one union operator.";
}

void assertSinkOperator(const QueryPlan& qp, const std::string& expectedSinkName)
{
    auto sinkOperators = qp.getOperatorByType<SinkLogicalOperator>();
    ASSERT_EQ(sinkOperators.size(), 1) << "Expected exactly one sink operator.";
    EXPECT_EQ(sinkOperators[0].getName(), expectedSinkName) << "Sink name mismatch.";
}
}

TEST_F(QueryPlanBuilderTest, testHasOperator)
{
    auto queryPlan = QueryPlanBuilder::createQueryPlan("test_stream");

    /// Add selection operator and verify
    auto filterFunction = EqualsLogicalFunction(FieldAccessLogicalFunction("a"), FieldAccessLogicalFunction("b"));
    queryPlan = QueryPlanBuilder::addSelection(filterFunction, queryPlan);
    assertSelectionOperator(queryPlan, "a", "b");

    /// Add projection operator and verify
    std::vector<LogicalFunction> functions;
    functions.push_back(FieldAccessLogicalFunction("id"));
    queryPlan = QueryPlanBuilder::addProjection(std::move(functions), queryPlan);
    assertProjectionOperator(queryPlan, {"id"});

    /// Add map operator and verify
    auto mapFunction = FieldAssignmentLogicalFunction(
        FieldAccessLogicalFunction("b"), ConstantValueLogicalFunction(DataTypeProvider::provideDataType(LogicalType::INT32), "1"));
    queryPlan = QueryPlanBuilder::addMap(std::move(mapFunction), queryPlan);
    assertMapOperator(queryPlan, "b", "1");

    /// Add union operator and verify
    auto rightQueryPlan = QueryPlanBuilder::createQueryPlan("test_stream_b");
    queryPlan = QueryPlanBuilder::addUnion(queryPlan, rightQueryPlan);
    assertUnionOperator(queryPlan);

    // Add sink operator and verify.
    queryPlan = QueryPlanBuilder::addSink("print_source", queryPlan, WorkerId(0));
    assertSinkOperator(queryPlan, "print_source");
}
