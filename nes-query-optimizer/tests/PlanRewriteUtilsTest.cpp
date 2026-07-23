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
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <gtest/gtest.h>

#include <DataTypes/DataType.hpp>
#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Schema/Field.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <OptimizerTestUtils.hpp>
#include <PlanRewriteUtils.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
namespace
{
class PlanRewriteUtilsTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("PlanRewriteUtilsTest.log", LogLevel::LOG_DEBUG); }

    OptimizerTestUtils utils;
};
}

TEST_F(PlanRewriteUtilsTest, testReplaceFieldAccesses)
{
    auto op1 = utils.createSource("first", {"a", "b"});
    auto op2 = utils.createSource("second", {"c", "d"});
    auto op3 = utils.createSource("third", {"e", "f"});

    auto op1New = utils.createSource("first_new", {"a", "b"});
    auto op2New = utils.createSource("second_new", {"c", "d"});

    const std::unordered_map<Field, Field> fields
        = {{op1.getOutputSchema().getFieldByName(Identifier::parse("a")).value(),
            op1New.getOutputSchema().getFieldByName(Identifier::parse("a")).value()},
           {op2.getOutputSchema().getFieldByName(Identifier::parse("c")).value(),
            op2New.getOutputSchema().getFieldByName(Identifier::parse("c")).value()}};

    const LogicalFunction function = AndLogicalFunction{
        GreaterLogicalFunction{
            FieldAccessLogicalFunction{op1.getOutputSchema().getFieldByName(Identifier::parse("a")).value()},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE}, "0"}},
        AndLogicalFunction{
            LessLogicalFunction{
                FieldAccessLogicalFunction{op2.getOutputSchema().getFieldByName(Identifier::parse("c")).value()},
                ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE}, "0"}},
            EqualsLogicalFunction{
                FieldAccessLogicalFunction{op3.getOutputSchema().getFieldByName(Identifier::parse("e")).value()},
                ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE}, "0"}}}};


    const auto newFunction = replaceFieldAccesses(function, fields);

    const auto fieldAccess1 = newFunction.getChildren().at(0).getChildren().at(0).getAs<FieldAccessLogicalFunction>();
    const auto fieldAccess2 = newFunction.getChildren().at(1).getChildren().at(0).getChildren().at(0).getAs<FieldAccessLogicalFunction>();
    const auto fieldAccess3 = newFunction.getChildren().at(1).getChildren().at(1).getChildren().at(0).getAs<FieldAccessLogicalFunction>();


    ASSERT_EQ(fieldAccess1->getField().getProducedBy(), op1New);
    ASSERT_EQ(fieldAccess2->getField().getProducedBy(), op2New);
    ASSERT_EQ(fieldAccess3->getField().getProducedBy(), op3);
}

namespace
{
LogicalFunction equalsZero(const LogicalOperator& producer, const std::string& fieldName)
{
    return EqualsLogicalFunction{
        FieldAccessLogicalFunction{producer.getOutputSchema()[Identifier::parse(fieldName)].value()},
        ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "0"}};
}
}

TEST_F(PlanRewriteUtilsTest, getSharedOperatorIdsOnSingleSinkPlan)
{
    const auto source = utils.createSource("shared_ids_single", {"a", "b"});
    const auto selection = SelectionLogicalOperator::create(source, equalsZero(source, "a"));
    const auto plan = utils.createPlan(utils.createSink(selection, "shared_ids_single_sink", {"a", "b"}));

    EXPECT_TRUE(getSharedOperatorIds(plan).empty());
}

TEST_F(PlanRewriteUtilsTest, getSharedOperatorIdsOnMultiSinkPlan)
{
    const auto source = utils.createSource("shared_ids_multi", {"a", "b"});
    const auto selection = SelectionLogicalOperator::create(source, equalsZero(source, "a"));
    const auto sink1 = utils.createSink(selection, "shared_ids_multi_sink1", {"a", "b"});
    const auto sink2 = utils.createSink(selection, "shared_ids_multi_sink2", {"a", "b"});
    const auto plan = utils.createPlan({sink1, sink2});

    /// Only the selection has two parents; the source is reachable through two paths but has a single parent edge.
    EXPECT_EQ(getSharedOperatorIds(plan), (std::unordered_set{selection.getId()}));
}

TEST_F(PlanRewriteUtilsTest, getSharedOperatorIdsCountsDuplicateEdgesFromSameParent)
{
    const auto source = utils.createSource("shared_ids_duplicate", {"a", "b"});
    const auto unionOp = UnionLogicalOperator::create(std::vector<LogicalOperator>{source, source});
    const auto plan = utils.createPlan(utils.createSink(unionOp, "shared_ids_duplicate_sink", {"a", "b"}));

    EXPECT_EQ(getSharedOperatorIds(plan), (std::unordered_set{source.getId()}));
}

TEST_F(PlanRewriteUtilsTest, rewritePlanBottomUpVisitsSharedOperatorsOnce)
{
    const auto source = utils.createSource("rewrite_once", {"a", "b"});
    const auto selection = SelectionLogicalOperator::create(source, equalsZero(source, "a"));
    const auto sink1 = utils.createSink(selection, "rewrite_once_sink1", {"a", "b"});
    const auto sink2 = utils.createSink(selection, "rewrite_once_sink2", {"a", "b"});
    const auto plan = utils.createPlan({sink1, sink2});

    int invocations = 0;
    const auto rewritten = rewritePlanBottomUp(
        plan,
        [&invocations](const LogicalOperator& original, std::vector<LogicalOperator> children)
        {
            ++invocations;
            return original.withChildren(std::move(children));
        });

    /// source, selection, sink1, sink2 — the shared selection and source are rewritten once
    EXPECT_EQ(invocations, 4);
    EXPECT_EQ(OptimizerTestUtils::collectOperatorIds(rewritten).size(), 4);
}

TEST_F(PlanRewriteUtilsTest, rewritePlanBottomUpPreservesSharing)
{
    const auto source = utils.createSource("rewrite_sharing", {"a", "b"});
    const auto selection = SelectionLogicalOperator::create(source, equalsZero(source, "a"));
    const auto sink1 = utils.createSink(selection, "rewrite_sharing_sink1", {"a", "b"});
    const auto sink2 = utils.createSink(selection, "rewrite_sharing_sink2", {"a", "b"});
    const auto plan = utils.createPlan({sink1, sink2});

    const auto rewritten = rewritePlanBottomUp(
        plan,
        [](const LogicalOperator& original, std::vector<LogicalOperator> children) { return original.withChildren(std::move(children)); });

    const auto roots = rewritten.getRootOperators();
    ASSERT_EQ(roots.size(), 2);
    const auto sharedChild1 = roots.at(0).getChildren().at(0);
    const auto sharedChild2 = roots.at(1).getChildren().at(0);
    EXPECT_EQ(sharedChild1.getId(), sharedChild2.getId());
    EXPECT_EQ(sharedChild1.getChildren().at(0).getId(), sharedChild2.getChildren().at(0).getId());
}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
