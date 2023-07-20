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
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Optimizer/QueryRewrite/RedundancyEliminationRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <queue>
#include <string>

namespace NES::Optimizer {

RedundancyEliminationRulePtr RedundancyEliminationRule::create() { return std::make_shared<RedundancyEliminationRule>(RedundancyEliminationRule()); }

RedundancyEliminationRule::RedundancyEliminationRule() = default;

QueryPlanPtr RedundancyEliminationRule::apply(QueryPlanPtr queryPlan) {
    NES_INFO("Applying FilterPushDownRule to query {}", queryPlan->toString());
    auto filters = queryPlan->getOperatorByType<FilterLogicalOperatorNode>();
    for (auto& filter : filters){
        const ExpressionNodePtr filterPredicate = filter->getPredicate();
        ExpressionNodePtr updatedPredicate;
        while (updatedPredicate != filterPredicate){
            updatedPredicate = eliminateRedundancy(filterPredicate);
        }
        auto updatedFilter = LogicalOperatorFactory::createFilterOperator(updatedPredicate);
        filter->replace(updatedFilter);
    }
    return queryPlan;
}

NES::ExpressionNodePtr RedundancyEliminationRule::eliminateRedundancy(const ExpressionNodePtr& predicate) {
    if (predicate->instanceOf<EqualsExpressionNode>() || predicate->instanceOf<GreaterEqualsExpressionNode>() ||
        predicate->instanceOf<GreaterExpressionNode>() || predicate->instanceOf<LessEqualsExpressionNode>() ||
        predicate->instanceOf<LessExpressionNode>()){
        NES_INFO("The predicate has a comparison operator, proceed by moving constants if possible");
        return constantMoving(predicate);
    }
    NES_INFO("No redundancy elimination is applicable, returning passed predicate");
    return predicate;
}

NES::ExpressionNodePtr RedundancyEliminationRule::constantMoving(const ExpressionNodePtr& predicate) {
    NES_INFO("RedundancyEliminationRule.constantMoving not implemented yet");
    return predicate;
}

NES::ExpressionNodePtr RedundancyEliminationRule::constantFolding(const ExpressionNodePtr& predicate) {
    NES_INFO("Applying RedundancyEliminationRule.constantFolding to predicate {}", predicate->toString());
    if (predicate->instanceOf<AddExpressionNode>() || predicate->instanceOf<SubExpressionNode>() ||
        predicate->instanceOf<MulExpressionNode>() || predicate->instanceOf<DivExpressionNode>()){
        NES_INFO("The predicate is an addition/multiplication/subtraction/division, constant folding could be applied");
        auto operands = predicate->getChildren();
        auto leftOperand = operands.at(0);
        auto rightOperand = operands.at(1);
        if (leftOperand->instanceOf<ConstantValueExpressionNode>() && rightOperand->instanceOf<ConstantValueExpressionNode>()){
            NES_INFO("Both of the predicate expressions are constant and can be folded together");
            auto leftOperandValue = leftOperand->as<ConstantValueExpressionNode>()->getConstantValue();
            auto leftValueType = std::dynamic_pointer_cast<BasicValue>(leftOperandValue);
            auto leftValue = stoi(leftValueType->value);
            auto rightOperandValue = rightOperand->as<ConstantValueExpressionNode>()->getConstantValue();
            auto rightValueType = std::dynamic_pointer_cast<BasicValue>(leftOperandValue);
            auto rightValue = stoi(rightValueType->value);
            auto resultValue = 0;
            if (predicate->instanceOf<AddExpressionNode>()){
                NES_INFO("Summing the operands");
                resultValue = leftValue + rightValue;
            }
            else if(predicate->instanceOf<SubExpressionNode>()){
                NES_INFO("Subtracting the operands");
                resultValue = leftValue - rightValue;
            }
            else if(predicate->instanceOf<MulExpressionNode>()){
                NES_INFO("Multiplying the operands");
                resultValue = leftValue * rightValue;
            }
            else if(predicate->instanceOf<DivExpressionNode>()){
                if (rightValue != 0){
                    resultValue = leftValue / rightValue;
                } else{
                    resultValue = 0;
                }
            }
            NES_INFO("Computed the result, which is equal to ", resultValue);
            NES_INFO("Creating a new constant expression node with the result value");
            ExpressionNodePtr resultExpressionNode = ConstantValueExpressionNode::create(
                    DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), std::to_string(resultValue)));
            return resultExpressionNode;
        }
        else{
            NES_INFO("Not all the predicate expressions are constant, cannot apply folding");
        }
    }
    else{
        NES_INFO("The predicate is not an addition/multiplication/subtract/division, cannot apply folding");
    }
    NES_INFO("Returning original unmodified predicate");
    return predicate;
}

NES::ExpressionNodePtr RedundancyEliminationRule::arithmeticSimplification(const NES::ExpressionNodePtr& predicate) {
    NES_INFO("Applying RedundancyEliminationRule.arithmeticSimplification to predicate {}", predicate->toString());
    if (predicate->instanceOf<AddExpressionNode>() || predicate->instanceOf<MulExpressionNode>()){
        NES_INFO("The predicate involves an addition or multiplication, the rule can be applied");
        auto operands = predicate->getChildren();
        NES_INFO("Extracted the operands of the predicate");
        ConstantValueExpressionNodePtr constantOperand = nullptr;
        FieldAccessExpressionNodePtr fieldAccessOperand = nullptr;
        if (operands.size() == 2){
            NES_INFO("Check if the operands are a combination of a field access and a constant");
            for (const auto& addend : operands){
                if(addend->instanceOf<ConstantValueExpressionNode>()) {
                    constantOperand = addend->as<ConstantValueExpressionNode>();
                }
                else if(addend->instanceOf<FieldAccessExpressionNode>()){
                    fieldAccessOperand = addend->as<FieldAccessExpressionNode>();
                }
            }
            if(constantOperand && fieldAccessOperand){
                NES_INFO("The operands contains of a field access and a constant");
                auto constantOperandValue = constantOperand->as<ConstantValueExpressionNode>()->getConstantValue();
                auto basicValueType = std::dynamic_pointer_cast<BasicValue>(constantOperandValue);
                int constantValue = stoi(basicValueType->value);
                NES_INFO("Extracted the constant value from the constant operand");
                if (constantValue == 0 && predicate->instanceOf<AddExpressionNode>()){
                    NES_INFO("Case 1: Sum with 0: return the FieldAccessExpressionNode");
                    return fieldAccessOperand->as<ExpressionNode>();
                }
                else if(constantValue == 0 && predicate->instanceOf<MulExpressionNode>()){
                    NES_INFO("Case 2: Multiplication by 0: return the ConstantValueExpressionNode, that is 0");
                    return constantOperand->as<ExpressionNode>();
                }
                else if(constantValue == 1 && predicate->instanceOf<MulExpressionNode>()){
                    NES_INFO("Case 3: Multiplication by 1: return the FieldAccessExpressionNode");
                    return fieldAccessOperand->as<ExpressionNode>();
                }
                else{
                    NES_INFO("Given the combination of the constant value and predicate, no arithmetic simplification is possible");
                }
            }
            else{
                NES_INFO("The predicate is not a combination of constant and value access, no arithmetic simplification is possible");
            }
        }
        else{
            NES_INFO("The predicate does not have two children, no arithmetic simplification is possible");
        }
    }
    else{
        NES_INFO("The predicate does not involve an addition or multiplication, no arithmetic simplification is possible");
    }
    NES_INFO("Returning original unmodified predicate");
    return predicate;
}

NES::ExpressionNodePtr RedundancyEliminationRule::conjunctionDisjunctionSimplification(const NES::ExpressionNodePtr& predicate) {
    NES_INFO("At the moment boolean operator cannot be specified in the queries, no simplification is possible");
    NES_INFO("Returning original unmodified predicate");
    return predicate;
}


}// namespace NES::Optimizer
