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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <Operators/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Operators/Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Operators/Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Operators/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Operators/Expressions/ExpressionNode.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/Joins/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Optimizer/QueryRewrite/RedundancyEliminationRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <queue>
#include <string>

namespace NES::Optimizer {

RedundancyEliminationRulePtr RedundancyEliminationRule::create() {
    return std::make_shared<RedundancyEliminationRule>(RedundancyEliminationRule());
}

RedundancyEliminationRule::RedundancyEliminationRule() = default;

QueryPlanPtr RedundancyEliminationRule::apply(QueryPlanPtr queryPlan) {
    NES_INFO("Applying RedundancyEliminationRule to query {}", queryPlan->toString());
    NES_DEBUG("Applying rule to filter operators");
    auto filterOperators = queryPlan->getOperatorByType<FilterLogicalOperatorNode>();
    for (auto& filter : filterOperators) {
        const ExpressionNodePtr filterPredicate = filter->getPredicate();
        ExpressionNodePtr updatedPredicate;
        while (updatedPredicate != filterPredicate) {
            updatedPredicate = eliminateRedundancy(filterPredicate);
        }
        auto updatedFilter = LogicalOperatorFactory::createFilterOperator(updatedPredicate);
        filter->replace(updatedFilter);
    }
    NES_DEBUG("Applying rule to map operators");
    auto mapOperators = queryPlan->getOperatorByType<MapLogicalOperatorNode>();
    for (auto& map : mapOperators) {
        const ExpressionNodePtr mapExpression = map->getMapExpression();
        ExpressionNodePtr updatedMapExpression;
        while (updatedMapExpression != mapExpression) {
            updatedMapExpression = eliminateRedundancy(mapExpression);
        }
        auto updatedMap = LogicalOperatorFactory::createFilterOperator(updatedMapExpression);
        mapExpression->replace(updatedMap);
    }
    return queryPlan;
}

NES::ExpressionNodePtr RedundancyEliminationRule::eliminateRedundancy(const ExpressionNodePtr& predicate) {
    // Given a predicate, perform a series of optimizations by calling specific rewrite methods
    if (predicate->instanceOf<EqualsExpressionNode>() || predicate->instanceOf<GreaterEqualsExpressionNode>()
        || predicate->instanceOf<GreaterExpressionNode>() || predicate->instanceOf<LessEqualsExpressionNode>()
        || predicate->instanceOf<LessExpressionNode>()) {
        NES_DEBUG("The predicate has a comparison operator, proceed by moving constants if possible");
        return constantMoving(predicate);
    }
    NES_DEBUG("No redundancy elimination is applicable, returning passed predicate");
    return predicate;
}

NES::ExpressionNodePtr RedundancyEliminationRule::constantMoving(const ExpressionNodePtr& predicate) {
    // Move all constant values to the same side of the expression. Apply the change when comparison operators
    // are found, e.g.: equals, greaterThan,...
    NES_DEBUG("RedundancyEliminationRule.constantMoving not implemented yet");
    return predicate;
}

NES::ExpressionNodePtr RedundancyEliminationRule::constantFolding(const ExpressionNodePtr& predicate) {
    // Detect sum/subtraction/multiplication/division of constants inside a predicate and resolve them
    NES_DEBUG("Applying RedundancyEliminationRule.constantFolding to predicate {}", predicate->toString());
    if (predicate->instanceOf<AddExpressionNode>() || predicate->instanceOf<SubExpressionNode>()
        || predicate->instanceOf<MulExpressionNode>() || predicate->instanceOf<DivExpressionNode>()) {
        NES_DEBUG("The predicate is an addition/multiplication/subtraction/division, constant folding could be applied");
        auto operands = predicate->getChildren();
        auto leftOperand = operands.at(0);
        auto rightOperand = operands.at(1);
        if (leftOperand->instanceOf<ConstantValueExpressionNode>() && rightOperand->instanceOf<ConstantValueExpressionNode>()) {
            NES_DEBUG("Both of the predicate expressions are constant and can be folded together");
            auto leftOperandValue = leftOperand->as<ConstantValueExpressionNode>()->getConstantValue();
            auto leftValueType = std::dynamic_pointer_cast<BasicValue>(leftOperandValue);
            auto leftValue = stoi(leftValueType->value);
            auto rightOperandValue = rightOperand->as<ConstantValueExpressionNode>()->getConstantValue();
            auto rightValueType = std::dynamic_pointer_cast<BasicValue>(leftOperandValue);
            auto rightValue = stoi(rightValueType->value);
            auto resultValue = 0;
            if (predicate->instanceOf<AddExpressionNode>()) {
                NES_DEBUG("Summing the operands");
                resultValue = leftValue + rightValue;
            } else if (predicate->instanceOf<SubExpressionNode>()) {
                NES_DEBUG("Subtracting the operands");
                resultValue = leftValue - rightValue;
            } else if (predicate->instanceOf<MulExpressionNode>()) {
                NES_DEBUG("Multiplying the operands");
                resultValue = leftValue * rightValue;
            } else if (predicate->instanceOf<DivExpressionNode>()) {
                if (rightValue != 0) {
                    resultValue = leftValue / rightValue;
                } else {
                    resultValue = 0;
                }
            }
            NES_DEBUG("Computed the result, which is equal to ", resultValue);
            NES_DEBUG("Creating a new constant expression node with the result value");
            ExpressionNodePtr resultExpressionNode = ConstantValueExpressionNode::create(
                DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), std::to_string(resultValue)));
            return resultExpressionNode;
        } else {
            NES_DEBUG("Not all the predicate expressions are constant, cannot apply folding");
        }
    } else {
        NES_DEBUG("The predicate is not an addition/multiplication/subtract/division, cannot apply folding");
    }
    NES_DEBUG("Returning original unmodified predicate");
    return predicate;
}

NES::ExpressionNodePtr RedundancyEliminationRule::arithmeticSimplification(const NES::ExpressionNodePtr& predicate) {
    // Handle cases when a field value is multiplied by 0, 1 or summed with 0. Replace the two expressions with
    // one equivalent expression
    NES_DEBUG("Applying RedundancyEliminationRule.arithmeticSimplification to predicate {}", predicate->toString());
    if (predicate->instanceOf<AddExpressionNode>() || predicate->instanceOf<MulExpressionNode>()) {
        NES_DEBUG("The predicate involves an addition or multiplication, the rule can be applied");
        auto operands = predicate->getChildren();
        NES_DEBUG("Extracted the operands of the predicate");
        ConstantValueExpressionNodePtr constantOperand = nullptr;
        FieldAccessExpressionNodePtr fieldAccessOperand = nullptr;
        if (operands.size() == 2) {
            NES_DEBUG("Check if the operands are a combination of a field access and a constant");
            for (const auto& addend : operands) {
                if (addend->instanceOf<ConstantValueExpressionNode>()) {
                    constantOperand = addend->as<ConstantValueExpressionNode>();
                } else if (addend->instanceOf<FieldAccessExpressionNode>()) {
                    fieldAccessOperand = addend->as<FieldAccessExpressionNode>();
                }
            }
            if (constantOperand && fieldAccessOperand) {
                NES_DEBUG("The operands contains of a field access and a constant");
                auto constantOperandValue = constantOperand->as<ConstantValueExpressionNode>()->getConstantValue();
                auto basicValueType = std::dynamic_pointer_cast<BasicValue>(constantOperandValue);
                auto constantValue = stoi(basicValueType->value);
                NES_DEBUG("Extracted the constant value from the constant operand");
                if (constantValue == 0 && predicate->instanceOf<AddExpressionNode>()) {
                    NES_DEBUG("Case 1: Sum with 0: return the FieldAccessExpressionNode");
                    return fieldAccessOperand->as<ExpressionNode>();
                } else if (constantValue == 0 && predicate->instanceOf<MulExpressionNode>()) {
                    NES_DEBUG("Case 2: Multiplication by 0: return the ConstantValueExpressionNode, that is 0");
                    return constantOperand->as<ExpressionNode>();
                } else if (constantValue == 1 && predicate->instanceOf<MulExpressionNode>()) {
                    NES_DEBUG("Case 3: Multiplication by 1: return the FieldAccessExpressionNode");
                    return fieldAccessOperand->as<ExpressionNode>();
                } else {
                    NES_DEBUG(
                        "Given the combination of the constant value and predicate, no arithmetic simplification is possible");
                }
            } else {
                NES_DEBUG(
                    "The predicate is not a combination of constant and value access, no arithmetic simplification is possible");
            }
        } else {
            NES_DEBUG("The predicate does not have two children, no arithmetic simplification is possible");
        }
    } else {
        NES_DEBUG("The predicate does not involve an addition or multiplication, no arithmetic simplification is possible");
    }
    NES_DEBUG("Returning original unmodified predicate");
    return predicate;
}

NES::ExpressionNodePtr RedundancyEliminationRule::conjunctionDisjunctionSimplification(const NES::ExpressionNodePtr& predicate) {
    // Perform optimizations with some combinations of boolean values:
    // - FALSE in AND operation, TRUE in OR operation -> the result of the expression is FALSE
    // - TRUE in AND operation, FALSE in OR operation -> expression can be omitted
    NES_DEBUG("At the moment boolean operator cannot be specified in the queries, no simplification is possible");
    NES_DEBUG("Returning original unmodified predicate");
    return predicate;
}

}// namespace NES::Optimizer
