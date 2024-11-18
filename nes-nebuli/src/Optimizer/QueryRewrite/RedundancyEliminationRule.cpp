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

#include <queue>
#include <string>
#include <Functions/ArithmeticalFunctions/NodeFunctionAdd.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionDiv.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionMul.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionSub.hpp>
#include <Functions/LogicalFunctions/NodeFunctionAnd.hpp>
#include <Functions/LogicalFunctions/NodeFunctionEquals.hpp>
#include <Functions/LogicalFunctions/NodeFunctionGreater.hpp>
#include <Functions/LogicalFunctions/NodeFunctionGreaterEquals.hpp>
#include <Functions/LogicalFunctions/NodeFunctionLess.hpp>
#include <Functions/LogicalFunctions/NodeFunctionLessEquals.hpp>
#include <Functions/LogicalFunctions/NodeFunctionNegate.hpp>
#include <Functions/LogicalFunctions/NodeFunctionOr.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Optimizer/QueryRewrite/RedundancyEliminationRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/ValueTypes/BasicValue.hpp>

namespace NES::Optimizer
{

RedundancyEliminationRulePtr RedundancyEliminationRule::create()
{
    return std::make_shared<RedundancyEliminationRule>(RedundancyEliminationRule());
}

RedundancyEliminationRule::RedundancyEliminationRule() = default;

QueryPlanPtr RedundancyEliminationRule::apply(QueryPlanPtr queryPlan)
{
    NES_INFO("Applying RedundancyEliminationRule to query {}", queryPlan->toString());
    NES_DEBUG("Applying rule to filter operators");
    auto filterOperators = queryPlan->getOperatorByType<LogicalFilterOperator>();
    for (auto& filter : filterOperators)
    {
        const NodeFunctionPtr filterPredicate = filter->getPredicate();
        NodeFunctionPtr updatedPredicate;
        while (updatedPredicate != filterPredicate)
        {
            updatedPredicate = eliminatePredicateRedundancy(filterPredicate);
        }
        auto updatedFilter = LogicalOperatorFactory::createFilterOperator(updatedPredicate);
        filter->replace(updatedFilter);
    }
    NES_DEBUG("Applying rule to map operators");
    auto mapOperators = queryPlan->getOperatorByType<LogicalMapOperator>();
    for (auto& map : mapOperators)
    {
        const NodeFunctionPtr mapFunction = map->getMapFunction();
        NodeFunctionPtr updatedMapFunction;
        while (updatedMapFunction != mapFunction)
        {
            updatedMapFunction = eliminatePredicateRedundancy(mapFunction);
        }
        auto updatedMap = LogicalOperatorFactory::createFilterOperator(updatedMapFunction);
        mapFunction->replace(updatedMap);
    }
    return queryPlan;
}

NES::NodeFunctionPtr RedundancyEliminationRule::eliminatePredicateRedundancy(const NodeFunctionPtr& predicate)
{
    /// Given a predicate, perform a series of optimizations by calling specific rewrite methods
    if (NES::Util::instanceOf<NodeFunctionEquals>(predicate) || NES::Util::instanceOf<NodeFunctionGreaterEquals>(predicate)
        || NES::Util::instanceOf<NodeFunctionGreater>(predicate) || NES::Util::instanceOf<NodeFunctionLessEquals>(predicate)
        || NES::Util::instanceOf<NodeFunctionLess>(predicate))
    {
        NES_DEBUG("The predicate has a comparison operator, proceed by moving constants if possible");
        return constantMoving(predicate);
    }
    NES_DEBUG("No redundancy elimination is applicable, returning passed predicate");
    return predicate;
}

NES::NodeFunctionPtr RedundancyEliminationRule::constantMoving(const NodeFunctionPtr& predicate)
{
    /// Move all constant values to the same side of the function. Apply the change when comparison operators
    /// are found, e.g.: equals, greaterThan,...
    NES_DEBUG("RedundancyEliminationRule.constantMoving not implemented yet");
    return predicate;
}

NES::NodeFunctionPtr RedundancyEliminationRule::constantFolding(const NodeFunctionPtr& predicate)
{
    /// Detect sum/subtraction/multiplication/division of constants inside a predicate and resolve them
    NES_DEBUG("Applying RedundancyEliminationRule.constantFolding to predicate {}", *predicate);
    if (Util::instanceOf<NodeFunctionAdd>(predicate) || Util::instanceOf<NodeFunctionSub>(predicate)
        || Util::instanceOf<NodeFunctionMul>(predicate) || Util::instanceOf<NodeFunctionDiv>(predicate))
    {
        NES_DEBUG("The predicate is an addition/multiplication/subtraction/division, constant folding could be applied");
        auto operands = predicate->getChildren();
        auto leftOperand = operands.at(0);
        auto rightOperand = operands.at(1);
        if (NES::Util::instanceOf<NodeFunctionConstantValue>(leftOperand) && NES::Util::instanceOf<NodeFunctionConstantValue>(rightOperand))
        {
            NES_DEBUG("Both of the predicate functions are constant and can be folded together");
            auto leftOperandValue = NES::Util::as<NodeFunctionConstantValue>(leftOperand)->getConstantValue();
            auto leftValueType = std::dynamic_pointer_cast<BasicValue>(leftOperandValue);
            auto leftValue = stoi(leftValueType->value);
            auto rightOperandValue = NES::Util::as<NodeFunctionConstantValue>(rightOperand)->getConstantValue();
            auto rightValueType = std::dynamic_pointer_cast<BasicValue>(leftOperandValue);
            auto rightValue = stoi(rightValueType->value);
            auto resultValue = 0;
            if (NES::Util::instanceOf<NodeFunctionAdd>(predicate))
            {
                NES_DEBUG("Summing the operands");
                resultValue = leftValue + rightValue;
            }
            else if (NES::Util::instanceOf<NodeFunctionSub>(predicate))
            {
                NES_DEBUG("Subtracting the operands");
                resultValue = leftValue - rightValue;
            }
            else if (NES::Util::instanceOf<NodeFunctionMul>(predicate))
            {
                NES_DEBUG("Multiplying the operands");
                resultValue = leftValue * rightValue;
            }
            else if (NES::Util::instanceOf<NodeFunctionDiv>(predicate))
            {
                if (rightValue != 0)
                {
                    resultValue = leftValue / rightValue;
                }
                else
                {
                    resultValue = 0;
                }
            }
            NES_DEBUG("Computed the result, which is equal to ", resultValue);
            NES_DEBUG("Creating a new constant function node with the result value");
            NodeFunctionPtr resultNodeFunction = NodeFunctionConstantValue::create(
                DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), std::to_string(resultValue)));
            return resultNodeFunction;
        }
        else
        {
            NES_DEBUG("Not all the predicate functions are constant, cannot apply folding");
        }
    }
    else
    {
        NES_DEBUG("The predicate is not an addition/multiplication/subtract/division, cannot apply folding");
    }
    NES_DEBUG("Returning original unmodified predicate");
    return predicate;
}

NES::NodeFunctionPtr RedundancyEliminationRule::arithmeticSimplification(const NES::NodeFunctionPtr& predicate)
{
    /// Handle cases when a field value is multiplied by 0, 1 or summed with 0. Replace the two functions with
    /// one equivalent function
    NES_DEBUG("Applying RedundancyEliminationRule.arithmeticSimplification to predicate {}", *predicate);
    if (NES::Util::instanceOf<NodeFunctionAdd>(predicate) || NES::Util::instanceOf<NodeFunctionMul>(predicate))
    {
        NES_DEBUG("The predicate involves an addition or multiplication, the rule can be applied");
        auto operands = predicate->getChildren();
        NES_DEBUG("Extracted the operands of the predicate");
        NodeFunctionConstantValuePtr constantOperand = nullptr;
        NodeFunctionFieldAccessPtr fieldAccessOperand = nullptr;
        if (operands.size() == 2)
        {
            NES_DEBUG("Check if the operands are a combination of a field access and a constant");
            for (const auto& addend : operands)
            {
                if (NES::Util::instanceOf<NodeFunctionConstantValue>(addend))
                {
                    constantOperand = NES::Util::as<NodeFunctionConstantValue>(addend);
                }
                else if (NES::Util::instanceOf<NodeFunctionFieldAccess>(addend))
                {
                    fieldAccessOperand = NES::Util::as<NodeFunctionFieldAccess>(addend);
                }
            }
            if (constantOperand && fieldAccessOperand)
            {
                NES_DEBUG("The operands contains of a field access and a constant");
                auto constantOperandValue = NES::Util::as<NodeFunctionConstantValue>(constantOperand)->getConstantValue();
                auto basicValueType = std::dynamic_pointer_cast<BasicValue>(constantOperandValue);
                auto constantValue = stoi(basicValueType->value);
                NES_DEBUG("Extracted the constant value from the constant operand");
                if (constantValue == 0 && NES::Util::instanceOf<NodeFunctionAdd>(predicate))
                {
                    NES_DEBUG("Case 1: Sum with 0: return the NodeFunctionFieldAccess");
                    return NES::Util::as<NodeFunction>(fieldAccessOperand);
                }
                else if (constantValue == 0 && NES::Util::instanceOf<NodeFunctionMul>(predicate))
                {
                    NES_DEBUG("Case 2: Multiplication by 0: return the NodeFunctionConstantValue, that is 0");
                    return NES::Util::as<NodeFunction>(constantOperand);
                }
                else if (constantValue == 1 && NES::Util::instanceOf<NodeFunctionMul>(predicate))
                {
                    NES_DEBUG("Case 3: Multiplication by 1: return the NodeFunctionFieldAccess");
                    return NES::Util::as<NodeFunction>(fieldAccessOperand);
                }
                else
                {
                    NES_DEBUG("Given the combination of the constant value and predicate, no arithmetic simplification is possible");
                }
            }
            else
            {
                NES_DEBUG("The predicate is not a combination of constant and value access, no arithmetic simplification is possible");
            }
        }
        else
        {
            NES_DEBUG("The predicate does not have two children, no arithmetic simplification is possible");
        }
    }
    else
    {
        NES_DEBUG("The predicate does not involve an addition or multiplication, no arithmetic simplification is possible");
    }
    NES_DEBUG("Returning original unmodified predicate");
    return predicate;
}

NES::NodeFunctionPtr RedundancyEliminationRule::conjunctionDisjunctionSimplification(const NES::NodeFunctionPtr& predicate)
{
    /// Perform optimizations with some combinations of boolean values:
    /// - FALSE in AND operation, TRUE in OR operation -> the result of the function is FALSE
    /// - TRUE in AND operation, FALSE in OR operation -> function can be omitted
    NES_DEBUG("At the moment boolean operator cannot be specified in the queries, no simplification is possible");
    NES_DEBUG("Returning original unmodified predicate");
    return predicate;
}

}
