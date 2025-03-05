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

#include <algorithm>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>
#include <API/Schema.hpp>
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
#include <Functions/NodeFunctionConstantValue.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalSelectionOperator.hpp>
#include <Operators/Operator.hpp>
#include <Optimizer/QueryRewrite/AttributeSortRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::Optimizer
{

std::shared_ptr<AttributeSortRule> AttributeSortRule::create()
{
    return std::make_shared<AttributeSortRule>();
}

std::shared_ptr<QueryPlan> AttributeSortRule::apply(std::shared_ptr<QueryPlan> queryPlan)
{
    auto selectionOperators = queryPlan->getOperatorByType<LogicalSelectionOperator>();
    for (const auto& selectionOperator : selectionOperators)
    {
        const auto predicate = selectionOperator->getPredicate();
        auto updatedPredicate = sortAttributesInFunction(predicate);
        auto updatedFilter = std::make_shared<LogicalSelectionOperator>(updatedPredicate, getNextOperatorId());
        updatedFilter->setInputSchema(selectionOperator->getInputSchema()->copy());
        Util::as_if<LogicalOperator>(updatedFilter)
            ->setOutputSchema(Util::as_if<LogicalOperator>(selectionOperator)->getOutputSchema()->copy());
        selectionOperator->replace(updatedFilter);
    }

    auto mapOperators = queryPlan->getOperatorByType<LogicalMapOperator>();
    for (const auto& mapOperator : mapOperators)
    {
        auto mapFunction = mapOperator->getMapFunction();
        auto updatedMapFunction = Util::as<NodeFunctionFieldAssignment>(sortAttributesInFunction(mapFunction));
        auto updatedMap = std::make_shared<LogicalMapOperator>(updatedMapFunction, getNextOperatorId());
        updatedMap->setInputSchema(mapOperator->getInputSchema()->copy());
        Util::as_if<LogicalOperator>(updatedMap)->setOutputSchema(Util::as_if<LogicalOperator>(mapOperator)->getOutputSchema()->copy());
        mapOperator->replace(updatedMap);
    }
    return queryPlan;
}

std::shared_ptr<NodeFunction> AttributeSortRule::sortAttributesInFunction(std::shared_ptr<NodeFunction> function)
{
    NES_DEBUG("Sorting attributed for input function {}", *function);
    if (Util::instanceOf<NES::LogicalNodeFunction>(function))
    {
        return sortAttributesInLogicalFunctions(function);
    }
    if (Util::instanceOf<NES::NodeFunctionArithmetical>(function))
    {
        return sortAttributesInArithmeticalFunctions(function);
    }
    else if (Util::instanceOf<NES::NodeFunctionFieldAssignment>(function))
    {
        const auto fieldAssignmentNodeFunction = Util::as<NES::NodeFunctionFieldAssignment>(function);
        const auto assignment = fieldAssignmentNodeFunction->getAssignment();
        const auto updatedAssignment = sortAttributesInFunction(assignment);
        const auto field = fieldAssignmentNodeFunction->getField();
        return NES::NodeFunctionFieldAssignment::create(field, updatedAssignment);
    }
    else if (Util::instanceOf<NES::NodeFunctionConstantValue>(function) || Util::instanceOf<NES::NodeFunctionFieldAccess>(function))
    {
        return function;
    }
    throw NotImplemented("No conversion to Z3 function implemented for the function: ", *function);
}

std::shared_ptr<NodeFunction> AttributeSortRule::sortAttributesInArithmeticalFunctions(std::shared_ptr<NodeFunction> function)
{
    NES_DEBUG("Create Z3 function for arithmetical function {}", *function);
    if (Util::instanceOf<NES::NodeFunctionAdd>(function))
    {
        auto addNodeFunction = Util::as<NES::NodeFunctionAdd>(function);

        auto sortedLeft = sortAttributesInFunction(addNodeFunction->getLeft());
        auto sortedRight = sortAttributesInFunction(addNodeFunction->getRight());

        auto leftCommutativeFields = fetchCommutativeFields<NES::NodeFunctionAdd>(sortedLeft);
        auto rightCommutativeFields = fetchCommutativeFields<NES::NodeFunctionAdd>(sortedRight);

        std::vector<std::shared_ptr<NodeFunction>> allCommutativeFields;
        allCommutativeFields.insert(allCommutativeFields.end(), leftCommutativeFields.begin(), leftCommutativeFields.end());
        allCommutativeFields.insert(allCommutativeFields.end(), rightCommutativeFields.begin(), rightCommutativeFields.end());

        std::vector<std::shared_ptr<NodeFunction>> sortedCommutativeFields;
        sortedCommutativeFields.reserve(allCommutativeFields.size());
        for (const auto& commutativeField : allCommutativeFields)
        {
            sortedCommutativeFields.push_back(commutativeField->deepCopy());
        }

        std::ranges::sort(
            sortedCommutativeFields,
            [](const std::shared_ptr<NodeFunction>& lhsField, const std::shared_ptr<NodeFunction>& rhsField)
            {
                std::string leftValue;
                std::string rightValue;

                if (Util::instanceOf<NES::NodeFunctionConstantValue>(lhsField))
                {
                    leftValue = Util::as<NES::NodeFunctionConstantValue>(lhsField)->getConstantValue();
                }
                else
                {
                    leftValue = Util::as<NES::NodeFunctionFieldAccess>(lhsField)->getFieldName();
                }

                if (Util::instanceOf<NodeFunctionConstantValue>(rhsField))
                {
                    rightValue = Util::as<NodeFunctionConstantValue>(rhsField)->getConstantValue();
                }
                else
                {
                    rightValue = Util::as<NES::NodeFunctionFieldAccess>(rhsField)->getFieldName();
                }
                return leftValue.compare(rightValue) < 0;
            });

        for (unsigned long i = 0; i < sortedCommutativeFields.size(); i++)
        {
            auto originalField = allCommutativeFields[i];
            auto updatedField = sortedCommutativeFields[i];

            if (sortedLeft.get() == originalField.get())
            {
                sortedLeft = updatedField;
            }
            else if (!(Util::instanceOf<NodeFunctionFieldAccess>(sortedLeft) || Util::instanceOf<NodeFunctionConstantValue>(sortedLeft)))
            {
                bool replaced = replaceCommutativeFunctions(sortedLeft, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }

            if (sortedRight.get() == originalField.get())
            {
                sortedRight = updatedField;
            }
            else if (!(Util::instanceOf<NodeFunctionFieldAccess>(sortedRight) || Util::instanceOf<NodeFunctionConstantValue>(sortedRight)))
            {
                bool replaced = replaceCommutativeFunctions(sortedRight, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }
        }

        if (!Util::instanceOf<NodeFunctionAdd>(sortedLeft) || !Util::instanceOf<NodeFunctionAdd>(sortedRight))
        {
            auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
            auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
            int compared = leftSortedFieldName.compare(rightSortedFieldName);
            if (compared > 0)
            {
                return NodeFunctionAdd::create(sortedRight, sortedLeft);
            }
        }

        return NodeFunctionAdd::create(sortedLeft, sortedRight);
    }
    if (Util::instanceOf<NodeFunctionSub>(function))
    {
        auto subNodeFunction = Util::as<NodeFunctionSub>(function);
        auto left = subNodeFunction->getLeft();
        auto right = subNodeFunction->getRight();
        sortAttributesInFunction(left);
        sortAttributesInFunction(right);
        return function;
    }
    else if (Util::instanceOf<NodeFunctionMul>(function))
    {
        auto mulNodeFunction = Util::as<NodeFunctionMul>(function);
        auto left = mulNodeFunction->getLeft();
        auto right = mulNodeFunction->getRight();

        auto sortedLeft = sortAttributesInFunction(left);
        auto sortedRight = sortAttributesInFunction(right);

        auto leftCommutativeFields = fetchCommutativeFields<NodeFunctionMul>(sortedLeft);
        auto rightCommutativeFields = fetchCommutativeFields<NodeFunctionMul>(sortedRight);

        std::vector<std::shared_ptr<NodeFunction>> allCommutativeFields;
        allCommutativeFields.insert(allCommutativeFields.end(), leftCommutativeFields.begin(), leftCommutativeFields.end());
        allCommutativeFields.insert(allCommutativeFields.end(), rightCommutativeFields.begin(), rightCommutativeFields.end());

        std::vector<std::shared_ptr<NodeFunction>> sortedCommutativeFields;
        sortedCommutativeFields.reserve(allCommutativeFields.size());
        for (const auto& commutativeField : allCommutativeFields)
        {
            sortedCommutativeFields.push_back(commutativeField->deepCopy());
        }

        std::sort(
            sortedCommutativeFields.begin(),
            sortedCommutativeFields.end(),
            [](const std::shared_ptr<NodeFunction>& lhsField, const std::shared_ptr<NodeFunction>& rhsField)
            {
                std::string leftValue;
                std::string rightValue;

                if (Util::instanceOf<NodeFunctionConstantValue>(lhsField))
                {
                    leftValue = Util::as<NodeFunctionConstantValue>(lhsField)->getConstantValue();
                }
                else
                {
                    leftValue = Util::as<NodeFunctionFieldAccess>(lhsField)->getFieldName();
                }

                if (Util::instanceOf<NodeFunctionConstantValue>(rhsField))
                {
                    rightValue = Util::as<NodeFunctionConstantValue>(rhsField)->getConstantValue();
                }
                else
                {
                    rightValue = Util::as<NodeFunctionFieldAccess>(rhsField)->getFieldName();
                }
                return leftValue.compare(rightValue) < 0;
            });

        for (unsigned long i = 0; i < sortedCommutativeFields.size(); i++)
        {
            auto originalField = allCommutativeFields[i];
            auto updatedField = sortedCommutativeFields[i];

            if (sortedLeft.get() == originalField.get())
            {
                sortedLeft = updatedField;
            }
            else if (!(Util::instanceOf<NodeFunctionFieldAccess>(sortedLeft) || Util::instanceOf<NodeFunctionConstantValue>(sortedLeft)))
            {
                bool replaced = replaceCommutativeFunctions(sortedLeft, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }

            if (sortedRight.get() == originalField.get())
            {
                sortedRight = updatedField;
            }
            else if (!(Util::instanceOf<NodeFunctionFieldAccess>(sortedRight) || Util::instanceOf<NodeFunctionConstantValue>(sortedRight)))
            {
                bool replaced = replaceCommutativeFunctions(sortedRight, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }
        }

        if (!Util::instanceOf<NodeFunctionMul>(sortedLeft) || !Util::instanceOf<NodeFunctionMul>(sortedRight))
        {
            auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
            auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
            int compared = leftSortedFieldName.compare(rightSortedFieldName);
            if (compared > 0)
            {
                return NodeFunctionMul::create(sortedRight, sortedLeft);
            }
        }

        return NodeFunctionMul::create(sortedLeft, sortedRight);
    }
    else if (Util::instanceOf<NodeFunctionDiv>(function))
    {
        auto divNodeFunction = Util::as<NodeFunctionDiv>(function);
        auto left = divNodeFunction->getLeft();
        auto right = divNodeFunction->getRight();
        sortAttributesInFunction(left);
        sortAttributesInFunction(right);
        return function;
    }
    throw NotImplemented("No conversion to Z3 function implemented for the arithmetical function node: ", *function);
}

std::shared_ptr<NodeFunction> AttributeSortRule::sortAttributesInLogicalFunctions(const std::shared_ptr<NodeFunction>& function)
{
    NES_DEBUG("Create Z3 function node for logical function {}", *function);
    if (Util::instanceOf<NodeFunctionAnd>(function))
    {
        auto andNodeFunction = Util::as<NodeFunctionAnd>(function);
        auto left = andNodeFunction->getLeft();
        auto right = andNodeFunction->getRight();
        auto sortedLeft = sortAttributesInFunction(left);
        auto sortedRight = sortAttributesInFunction(right);

        auto leftCommutativeFields = fetchCommutativeFields<NodeFunctionAnd>(sortedLeft);
        auto rightCommutativeFields = fetchCommutativeFields<NodeFunctionAnd>(sortedRight);

        std::vector<std::shared_ptr<NodeFunction>> allCommutativeFields;
        allCommutativeFields.insert(allCommutativeFields.end(), leftCommutativeFields.begin(), leftCommutativeFields.end());
        allCommutativeFields.insert(allCommutativeFields.end(), rightCommutativeFields.begin(), rightCommutativeFields.end());

        std::vector<std::shared_ptr<NodeFunction>> sortedCommutativeFields;
        sortedCommutativeFields.reserve(allCommutativeFields.size());
        for (const auto& commutativeField : allCommutativeFields)
        {
            sortedCommutativeFields.push_back(commutativeField->deepCopy());
        }

        std::sort(
            sortedCommutativeFields.begin(),
            sortedCommutativeFields.end(),
            [](const std::shared_ptr<NodeFunction>& lhsField, const std::shared_ptr<NodeFunction>& rhsField)
            {
                std::string leftValue;
                std::string rightValue;

                if (Util::instanceOf<NodeFunctionConstantValue>(lhsField))
                {
                    leftValue = Util::as<NodeFunctionConstantValue>(lhsField)->getConstantValue();
                }
                else
                {
                    leftValue = Util::as<NodeFunctionFieldAccess>(lhsField)->getFieldName();
                }

                if (Util::instanceOf<NodeFunctionConstantValue>(rhsField))
                {
                    rightValue = Util::as<NodeFunctionConstantValue>(rhsField)->getConstantValue();
                }
                else
                {
                    rightValue = Util::as<NodeFunctionFieldAccess>(rhsField)->getFieldName();
                }
                return leftValue.compare(rightValue) < 0;
            });

        for (unsigned long i = 0; i < sortedCommutativeFields.size(); i++)
        {
            auto originalField = allCommutativeFields[i];
            auto updatedField = sortedCommutativeFields[i];

            if (sortedLeft.get() == originalField.get())
            {
                sortedLeft = updatedField;
            }
            else if (!(Util::instanceOf<NodeFunctionFieldAccess>(sortedLeft) || Util::instanceOf<NodeFunctionConstantValue>(sortedLeft)))
            {
                bool replaced = replaceCommutativeFunctions(sortedLeft, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }

            if (sortedRight.get() == originalField.get())
            {
                sortedRight = updatedField;
            }
            else if (!(Util::instanceOf<NodeFunctionFieldAccess>(sortedRight) || Util::instanceOf<NodeFunctionConstantValue>(sortedRight)))
            {
                bool replaced = replaceCommutativeFunctions(sortedRight, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }
        }

        if (!Util::instanceOf<NodeFunctionAnd>(sortedLeft) || !Util::instanceOf<NodeFunctionAnd>(sortedRight))
        {
            auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
            auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
            int compared = leftSortedFieldName.compare(rightSortedFieldName);
            if (compared > 0)
            {
                return NodeFunctionAnd::create(sortedRight, sortedLeft);
            }
        }
        return NodeFunctionAnd::create(sortedLeft, sortedRight);
    }
    if (Util::instanceOf<NodeFunctionOr>(function))
    {
        auto orNodeFunction = Util::as<NodeFunctionOr>(function);
        auto left = orNodeFunction->getLeft();
        auto right = orNodeFunction->getRight();
        auto sortedLeft = sortAttributesInFunction(left);
        auto sortedRight = sortAttributesInFunction(right);

        auto leftCommutativeFields = fetchCommutativeFields<NodeFunctionOr>(sortedLeft);
        auto rightCommutativeFields = fetchCommutativeFields<NodeFunctionOr>(sortedRight);

        std::vector<std::shared_ptr<NodeFunction>> allCommutativeFields;
        allCommutativeFields.insert(allCommutativeFields.end(), leftCommutativeFields.begin(), leftCommutativeFields.end());
        allCommutativeFields.insert(allCommutativeFields.end(), rightCommutativeFields.begin(), rightCommutativeFields.end());

        std::vector<std::shared_ptr<NodeFunction>> sortedCommutativeFields;
        sortedCommutativeFields.reserve(allCommutativeFields.size());
        for (const auto& commutativeField : allCommutativeFields)
        {
            sortedCommutativeFields.push_back(commutativeField->deepCopy());
        }

        std::sort(
            sortedCommutativeFields.begin(),
            sortedCommutativeFields.end(),
            [](const std::shared_ptr<NodeFunction>& lhsField, const std::shared_ptr<NodeFunction>& rhsField)
            {
                std::string leftValue;
                std::string rightValue;

                if (Util::instanceOf<NodeFunctionConstantValue>(lhsField))
                {
                    leftValue = Util::as<NodeFunctionConstantValue>(lhsField)->getConstantValue();
                }
                else
                {
                    leftValue = Util::as<NodeFunctionFieldAccess>(lhsField)->getFieldName();
                }

                if (Util::instanceOf<NodeFunctionConstantValue>(rhsField))
                {
                    rightValue = Util::as<NodeFunctionConstantValue>(rhsField)->getConstantValue();
                }
                else
                {
                    rightValue = Util::as<NodeFunctionFieldAccess>(rhsField)->getFieldName();
                }
                return leftValue.compare(rightValue) < 0;
            });

        for (unsigned long i = 0; i < sortedCommutativeFields.size(); i++)
        {
            auto originalField = allCommutativeFields[i];
            auto updatedField = sortedCommutativeFields[i];

            if (sortedLeft.get() == originalField.get())
            {
                sortedLeft = updatedField;
            }
            else if (!(Util::instanceOf<NodeFunctionFieldAccess>(sortedLeft) || Util::instanceOf<NodeFunctionConstantValue>(sortedLeft)))
            {
                bool replaced = replaceCommutativeFunctions(sortedLeft, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }

            if (sortedRight.get() == originalField.get())
            {
                sortedRight = updatedField;
            }
            else if (!(Util::instanceOf<NodeFunctionFieldAccess>(sortedRight) || Util::instanceOf<NodeFunctionConstantValue>(sortedRight)))
            {
                bool replaced = replaceCommutativeFunctions(sortedRight, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }
        }

        if (!Util::instanceOf<NodeFunctionOr>(sortedLeft) || !Util::instanceOf<NodeFunctionOr>(sortedRight))
        {
            auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
            auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
            int compared = leftSortedFieldName.compare(rightSortedFieldName);
            if (compared > 0)
            {
                return NodeFunctionOr::create(sortedRight, sortedLeft);
            }
        }
        return NodeFunctionOr::create(sortedLeft, sortedRight);
    }
    else if (Util::instanceOf<NodeFunctionLess>(function))
    {
        auto lessNodeFunction = Util::as<NodeFunctionLess>(function);
        auto left = lessNodeFunction->getLeft();
        auto right = lessNodeFunction->getRight();

        auto sortedLeft = sortAttributesInFunction(left);
        auto sortedRight = sortAttributesInFunction(right);

        auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
        auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
        int compared = leftSortedFieldName.compare(rightSortedFieldName);
        if (compared > 0)
        {
            return NodeFunctionGreater::create(sortedRight, sortedLeft);
        }
        return NodeFunctionLess::create(sortedLeft, sortedRight);
    }
    else if (Util::instanceOf<NodeFunctionLessEquals>(function))
    {
        auto lessNodeFunctionEquals = Util::as<NodeFunctionLessEquals>(function);
        auto left = lessNodeFunctionEquals->getLeft();
        auto right = lessNodeFunctionEquals->getRight();
        auto sortedLeft = sortAttributesInFunction(left);
        auto sortedRight = sortAttributesInFunction(right);

        auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
        auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
        int compared = leftSortedFieldName.compare(rightSortedFieldName);
        if (compared > 0)
        {
            return NodeFunctionGreaterEquals::create(sortedRight, sortedLeft);
        }
        return NodeFunctionLessEquals::create(sortedLeft, sortedRight);
    }
    else if (Util::instanceOf<NodeFunctionGreater>(function))
    {
        auto greaterNodeFunction = Util::as<NodeFunctionGreater>(function);
        auto left = greaterNodeFunction->getLeft();
        auto right = greaterNodeFunction->getRight();

        auto sortedLeft = sortAttributesInFunction(left);
        auto sortedRight = sortAttributesInFunction(right);

        auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
        auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
        int compared = leftSortedFieldName.compare(rightSortedFieldName);
        if (compared > 0)
        {
            return NodeFunctionLess::create(sortedRight, sortedLeft);
        }
        return NodeFunctionGreater::create(sortedLeft, sortedRight);
    }
    else if (Util::instanceOf<NodeFunctionGreaterEquals>(function))
    {
        auto greaterNodeFunctionEquals = Util::as<NodeFunctionGreaterEquals>(function);
        auto left = greaterNodeFunctionEquals->getLeft();
        auto right = greaterNodeFunctionEquals->getRight();

        auto sortedLeft = sortAttributesInFunction(left);
        auto sortedRight = sortAttributesInFunction(right);

        auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
        auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
        int compared = leftSortedFieldName.compare(rightSortedFieldName);
        if (compared > 0)
        {
            return NodeFunctionLessEquals::create(sortedRight, sortedLeft);
        }
        return NodeFunctionGreaterEquals::create(sortedLeft, sortedRight);
    }
    else if (Util::instanceOf<NodeFunctionEquals>(function))
    {
        auto equalsNodeFunction = Util::as<NodeFunctionEquals>(function);
        auto left = equalsNodeFunction->getLeft();
        auto right = equalsNodeFunction->getRight();
        auto sortedLeft = sortAttributesInFunction(left);
        auto sortedRight = sortAttributesInFunction(right);

        auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
        auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
        int compared = leftSortedFieldName.compare(rightSortedFieldName);
        if (compared > 0)
        {
            return NodeFunctionEquals::create(sortedRight, sortedLeft);
        }
        return NodeFunctionEquals::create(sortedLeft, sortedRight);
    }
    else if (Util::instanceOf<NodeFunctionNegate>(function))
    {
        auto negateNodeFunction = Util::as<NodeFunctionNegate>(function);
        auto childFunction = negateNodeFunction->child();
        auto updatedChildFunction = sortAttributesInFunction(childFunction);
        return NodeFunctionNegate::create(updatedChildFunction);
    }
    throw NotImplemented("No conversion to Z3 function implemented for the logical function node: ", *function);
}

bool AttributeSortRule::replaceCommutativeFunctions(
    const std::shared_ptr<NodeFunction>& parentFunction,
    const std::shared_ptr<NodeFunction>& originalFunction,
    const std::shared_ptr<NodeFunction>& updatedFunction)
{
    const auto binaryFunction = Util::as<NodeFunctionBinary>(parentFunction);

    const std::shared_ptr<NodeFunction>& leftChild = binaryFunction->getLeft();
    const std::shared_ptr<NodeFunction>& rightChild = binaryFunction->getRight();
    if (leftChild.get() == originalFunction.get())
    {
        binaryFunction->removeChildren();
        binaryFunction->setChildren(updatedFunction, rightChild);
        return true;
    }
    if (rightChild.get() == originalFunction.get())
    {
        binaryFunction->removeChildren();
        binaryFunction->setChildren(leftChild, updatedFunction);
        return true;
    }
    else
    {
        auto children = parentFunction->getChildren();
        for (const auto& child : children)
        {
            if (!(Util::instanceOf<NodeFunctionFieldAccess>(child) || Util::instanceOf<NodeFunctionConstantValue>(child)))
            {
                const bool replaced = replaceCommutativeFunctions(Util::as<NodeFunction>(child), originalFunction, updatedFunction);
                if (replaced)
                {
                    return true;
                }
            }
        }
    }
    return false;
}

std::string AttributeSortRule::fetchLeftMostConstantValueOrFieldName(std::shared_ptr<NodeFunction> function)
{
    std::shared_ptr<NodeFunction> startPoint = std::move(function);
    while (!(Util::instanceOf<NodeFunctionFieldAccess>(startPoint) || Util::instanceOf<NodeFunctionConstantValue>(startPoint)))
    {
        startPoint = Util::as<NodeFunction>(startPoint->getChildren()[0]);
    }

    if (Util::instanceOf<NodeFunctionFieldAccess>(startPoint))
    {
        return Util::as<NodeFunctionFieldAccess>(startPoint)->getFieldName();
    }
    return Util::as<NodeFunctionConstantValue>(startPoint)->getConstantValue();
}

}
