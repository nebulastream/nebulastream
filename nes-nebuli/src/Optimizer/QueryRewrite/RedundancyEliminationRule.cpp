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
#include <Functions/ArithmeticalFunctions/DivBinaryLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/LogicalFunctionAdd.hpp>
#include <Functions/ArithmeticalFunctions/MulBinaryLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/SubBinaryLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAssignmentBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/LogicalFunctions/AndBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/EqualsBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/GreaterBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/GreaterEqualsBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/LessBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/LessEqualsBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/NegateUnaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/OrBinaryLogicalFunction.hpp>
#include <Iterators/DFSIterator.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/MapLogicalOperator.hpp>
#include <Operators/LogicalOperators/SelectionLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/JoinLogicalOperator.hpp>
#include <Optimizer/QueryRewrite/RedundancyEliminationRule.hpp>
#include <Plans/QueryPlan.hpp>
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

std::shared_ptr<QueryPlan> RedundancyEliminationRule::apply(std::shared_ptr<QueryPlan> queryPlan)
{
    NES_INFO("Applying RedundancyEliminationRule to query {}", queryPlan->toString());
    NES_DEBUG("Applying rule to filter operators");
    auto filterOperators = queryPlan->getOperatorByType<SelectionLogicalOperator>();
    for (auto& filter : filterOperators)
    {
        const std::shared_ptr<LogicalFunction> filterPredicate = filter->getPredicate();
        std::shared_ptr<LogicalFunction> updatedPredicate;
        while (updatedPredicate != filterPredicate)
        {
            updatedPredicate = eliminatePredicateRedundancy(filterPredicate);
        }
        auto updatedFilter = LogicalOperatorFactory::createFilterOperator(updatedPredicate);
        filter->replace(updatedFilter);
    }
    NES_DEBUG("Applying rule to map operators");
    auto mapOperators = queryPlan->getOperatorByType<MapLogicalOperator>();
    for (auto& map : mapOperators)
    {
        const std::shared_ptr<LogicalFunction> mapFunction = map->getMapFunction();
        std::shared_ptr<LogicalFunction> updatedMapFunction;
        while (updatedMapFunction != mapFunction)
        {
            updatedMapFunction = eliminatePredicateRedundancy(mapFunction);
        }
        auto updatedMap = LogicalOperatorFactory::createFilterOperator(updatedMapFunction);
        mapFunction->replace(updatedMap);
    }
    return queryPlan;
}

std::shared_ptr<LogicalFunction> RedundancyEliminationRule::eliminatePredicateRedundancy(const std::shared_ptr<LogicalFunction>& predicate)
{
    /// Given a predicate, perform a series of optimizations by calling specific rewrite methods
    if (NES::Util::instanceOf<EqualsBinaryLogicalFunction>(predicate)
        || NES::Util::instanceOf<GreaterBinaryLogicalFunctionEquals>(predicate)
        || NES::Util::instanceOf<GreaterBinaryLogicalFunction>(predicate)
        || NES::Util::instanceOf<LessEqualsBinaryLogicalFunction>(predicate) || NES::Util::instanceOf<LessBinaryLogicalFunction>(predicate))
    {
        NES_DEBUG("The predicate has a comparison operator, proceed by moving constants if possible");
        return constantMoving(predicate);
    }
    NES_DEBUG("No redundancy elimination is applicable, returning passed predicate");
    return predicate;
}

std::shared_ptr<LogicalFunction> RedundancyEliminationRule::constantMoving(const std::shared_ptr<LogicalFunction>& predicate)
{
    /// Move all constant values to the same side of the function. Apply the change when comparison operators
    /// are found, e.g.: equals, greaterThan,...
    NES_DEBUG("RedundancyEliminationRule.constantMoving not implemented yet");
    return predicate;
}

std::shared_ptr<LogicalFunction> RedundancyEliminationRule::constantFolding(const std::shared_ptr<LogicalFunction>& predicate)
{
    /// Detect sum/subtraction/multiplication/division of constants inside a predicate and resolve them
    NES_DEBUG("Applying RedundancyEliminationRule.constantFolding to predicate {}", predicate->toString());
    if (Util::instanceOf<LogicalFunctionAdd>(predicate) || Util::instanceOf<SubBinaryLogicalFunction>(predicate)
        || Util::instanceOf<MulBinaryLogicalFunction>(predicate) || Util::instanceOf<DivBinaryLogicalFunction>(predicate))
    {
        NES_DEBUG("The predicate is an addition/multiplication/subtraction/division, constant folding could be applied");
        auto operands = predicate->children;
        auto leftOperand = operands.at(0);
        auto rightOperand = operands.at(1);
        if (NES::Util::instanceOf<ConstantValueLogicalFunction>(leftOperand)
            && NES::Util::instanceOf<ConstantValueLogicalFunction>(rightOperand))
        {
            NES_DEBUG("Both of the predicate functions are constant and can be folded together");
            auto leftOperandValue = NES::Util::as<ConstantValueLogicalFunction>(leftOperand)->getConstantValue();
            auto leftValueType = std::dynamic_pointer_cast<BasicValue>(leftOperandValue);
            auto leftValue = stoi(leftValueType->value);
            auto rightOperandValue = NES::Util::as<ConstantValueLogicalFunction>(rightOperand)->getConstantValue();
            auto rightValueType = std::dynamic_pointer_cast<BasicValue>(leftOperandValue);
            auto rightValue = stoi(rightValueType->value);
            auto resultValue = 0;
            if (NES::Util::instanceOf<LogicalFunctionAdd>(predicate))
            {
                NES_DEBUG("Summing the operands");
                resultValue = leftValue + rightValue;
            }
            else if (NES::Util::instanceOf<SubBinaryLogicalFunction>(predicate))
            {
                NES_DEBUG("Subtracting the operands");
                resultValue = leftValue - rightValue;
            }
            else if (NES::Util::instanceOf<MulBinaryLogicalFunction>(predicate))
            {
                NES_DEBUG("Multiplying the operands");
                resultValue = leftValue * rightValue;
            }
            else if (NES::Util::instanceOf<DivBinaryLogicalFunction>(predicate))
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
            std::shared_ptr<LogicalFunction> resultLogicalFunction = ConstantValueLogicalFunction::create(
                DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), std::to_string(resultValue)));
            return resultLogicalFunction;
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

std::shared_ptr<LogicalFunction> RedundancyEliminationRule::arithmeticSimplification(const std::shared_ptr<LogicalFunction>& predicate)
{
    /// Handle cases when a field value is multiplied by 0, 1 or summed with 0. Replace the two functions with
    /// one equivalent function
    NES_DEBUG("Applying RedundancyEliminationRule.arithmeticSimplification to predicate {}", predicate->toString());
    if (NES::Util::instanceOf<LogicalFunctionAdd>(predicate) || NES::Util::instanceOf<MulBinaryLogicalFunction>(predicate))
    {
        NES_DEBUG("The predicate involves an addition or multiplication, the rule can be applied");
        auto operands = predicate->children;
        NES_DEBUG("Extracted the operands of the predicate");
        ConstantValueLogicalFunctionPtr constantOperand = nullptr;
        std::shared_ptr<FieldAccessLogicalFunction> fieldAccessOperand = nullptr;
        if (operands.size() == 2)
        {
            NES_DEBUG("Check if the operands are a combination of a field access and a constant");
            for (const auto& addend : operands)
            {
                if (NES::Util::instanceOf<ConstantValueLogicalFunction>(addend))
                {
                    constantOperand = NES::Util::as<ConstantValueLogicalFunction>(addend);
                }
                else if (NES::Util::instanceOf<FieldAccessLogicalFunction>(addend))
                {
                    fieldAccessOperand = NES::Util::as<FieldAccessLogicalFunction>(addend);
                }
            }
            if (constantOperand && fieldAccessOperand)
            {
                NES_DEBUG("The operands contains of a field access and a constant");
                auto constantOperandValue = NES::Util::as<ConstantValueLogicalFunction>(constantOperand)->getConstantValue();
                auto basicValueType = std::dynamic_pointer_cast<BasicValue>(constantOperandValue);
                auto constantValue = stoi(basicValueType->value);
                NES_DEBUG("Extracted the constant value from the constant operand");
                if (constantValue == 0 && NES::Util::instanceOf<LogicalFunctionAdd>(predicate))
                {
                    NES_DEBUG("Case 1: Sum with 0: return the FieldAccessLogicalFunction");
                    return NES::Util::as<LogicalFunction>(fieldAccessOperand);
                }
                else if (constantValue == 0 && NES::Util::instanceOf<MulBinaryLogicalFunction>(predicate))
                {
                    NES_DEBUG("Case 2: Multiplication by 0: return the ConstantValueLogicalFunction, that is 0");
                    return NES::Util::as<LogicalFunction>(constantOperand);
                }
                else if (constantValue == 1 && NES::Util::instanceOf<MulBinaryLogicalFunction>(predicate))
                {
                    NES_DEBUG("Case 3: Multiplication by 1: return the FieldAccessLogicalFunction");
                    return NES::Util::as<LogicalFunction>(fieldAccessOperand);
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

std::shared_ptr<LogicalFunction>
RedundancyEliminationRule::conjunctionDisjunctionSimplification(const std::shared_ptr<LogicalFunction>& predicate)
{
    /// Perform optimizations with some combinations of boolean values:
    /// - FALSE in AND operation, TRUE in OR operation -> the result of the function is FALSE
    /// - TRUE in AND operation, FALSE in OR operation -> function can be omitted
    NES_DEBUG("At the moment boolean operator cannot be specified in the queries, no simplification is possible");
    NES_DEBUG("Returning original unmodified predicate");
    return predicate;
}

}
