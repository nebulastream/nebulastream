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
#include <Functions/ArithmeticalFunctions/AddBinaryLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/DivBinaryLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/MulBinaryLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/SubBinaryLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAssignmentBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/AndBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/EqualsBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/GreaterBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/GreaterEqualsBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/LessBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/LessEqualsBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/NegateUnaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/OrBinaryLogicalFunction.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalSelectionOperator.hpp>
#include <Operators/Operator.hpp>
#include <Optimizer/QueryRewrite/AttributeSortRule.hpp>
#include <Plans/QueryPlan.hpp>
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
        auto predicate = selectionOperator->getPredicate();
        auto updatedPredicate = sortAttributesInFunction(predicate);
        auto updatedFilter = std::make_shared<LogicalSelectionOperator>(updatedPredicate, getNextOperatorId());
        updatedFilter->setInputSchema(selectionOperator->getInputSchema()->clone());
        Util::as_if<LogicalOperator>(updatedFilter)
            ->setOutputSchema(Util::as_if<LogicalOperator>(selectionOperator)->getOutputSchema()->clone());
        selectionOperator->replace(updatedFilter);
    }

    auto mapOperators = queryPlan->getOperatorByType<LogicalMapOperator>();
    for (const auto& mapOperator : mapOperators)
    {
        auto mapFunction = mapOperator->getMapFunction();
        auto updatedMapFunction = Util::as<FieldAssignmentBinaryLogicalFunction>(sortAttributesInFunction(mapFunction));
        auto updatedMap = std::make_shared<LogicalMapOperator>(updatedMapFunction, getNextOperatorId());
        updatedMap->setInputSchema(mapOperator->getInputSchema()->clone());
        Util::as_if<LogicalOperator>(updatedMap)->setOutputSchema(Util::as_if<LogicalOperator>(mapOperator)->getOutputSchema()->clone());
        mapOperator->replace(updatedMap);
    }
    return queryPlan;
}

std::shared_ptr<LogicalFunction> AttributeSortRule::sortAttributesInFunction(std::shared_ptr<LogicalFunction> function)
{
    NES_DEBUG("Sorting attributed for input function {}", *function);
    if (Util::instanceOf<NES::LogicalLogicalFunction>(function))
    {
        return sortAttributesInLogicalFunctions(function);
    }
    if (Util::instanceOf<NES::LogicalFunctionArithmetical>(function))
    {
        return sortAttributesInArithmeticalFunctions(function);
    }
    else if (Util::instanceOf<NES::FieldAssignmentBinaryLogicalFunction>(function))
    {
        auto fieldAssignmentLogicalFunction = Util::as<NES::FieldAssignmentBinaryLogicalFunction>(function);
        auto assignment = fieldAssignmentLogicalFunction->getAssignment();
        auto updatedAssignment = sortAttributesInFunction(assignment);
        auto field = fieldAssignmentLogicalFunction->getField();
        return NES::FieldAssignmentBinaryLogicalFunction::create(field, updatedAssignment);
    }
    else if (Util::instanceOf<NES::ConstantValueLogicalFunction>(function) || Util::instanceOf<NES::FieldAccessLogicalFunction>(function))
    {
        return function;
    }
    throw NotImplemented("No conversion to Z3 function implemented for the function: ", *function);
}

std::shared_ptr<LogicalFunction> AttributeSortRule::sortAttributesInArithmeticalFunctions(std::shared_ptr<LogicalFunction> function)
{
    NES_DEBUG("Create Z3 function for arithmetical function {}", *function);
    if (Util::instanceOf<NES::LogicalFunctionAdd>(function))
    {
        auto addLogicalFunction = Util::as<NES::LogicalFunctionAdd>(function);

        auto sortedLeft = sortAttributesInFunction(addLogicalFunction->getLeft());
        auto sortedRight = sortAttributesInFunction(addLogicalFunction->getRight());

        auto leftCommutativeFields = fetchCommutativeFields<NES::LogicalFunctionAdd>(sortedLeft);
        auto rightCommutativeFields = fetchCommutativeFields<NES::LogicalFunctionAdd>(sortedRight);

        std::vector<std::shared_ptr<LogicalFunction>> allCommutativeFields;
        allCommutativeFields.insert(allCommutativeFields.end(), leftCommutativeFields.begin(), leftCommutativeFields.end());
        allCommutativeFields.insert(allCommutativeFields.end(), rightCommutativeFields.begin(), rightCommutativeFields.end());

        std::vector<std::shared_ptr<LogicalFunction>> sortedCommutativeFields;
        sortedCommutativeFields.reserve(allCommutativeFields.size());
        for (const auto& commutativeField : allCommutativeFields)
        {
            sortedCommutativeFields.push_back(commutativeField->clone());
        }

        std::ranges::sort(
            sortedCommutativeFields,
            [](const std::shared_ptr<NodeFunction>& lhsField, const std::shared_ptr<NodeFunction>& rhsField)
            {
                std::string leftValue;
                std::string rightValue;

                if (Util::instanceOf<NES::ConstantValueLogicalFunction>(lhsField))
                {
                    leftValue = Util::as<NES::ConstantValueLogicalFunction>(lhsField)->getConstantValue();
                }
                else
                {
                    leftValue = Util::as<NES::FieldAccessLogicalFunction>(lhsField)->getFieldName();
                }

                if (Util::instanceOf<ConstantValueLogicalFunction>(rhsField))
                {
                    rightValue = Util::as<ConstantValueLogicalFunction>(rhsField)->getConstantValue();
                }
                else
                {
                    rightValue = Util::as<NES::FieldAccessLogicalFunction>(rhsField)->getFieldName();
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
            else if (!(Util::instanceOf<FieldAccessLogicalFunction>(sortedLeft)
                       || Util::instanceOf<ConstantValueLogicalFunction>(sortedLeft)))
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
            else if (!(Util::instanceOf<FieldAccessLogicalFunction>(sortedRight)
                       || Util::instanceOf<ConstantValueLogicalFunction>(sortedRight)))
            {
                bool replaced = replaceCommutativeFunctions(sortedRight, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }
        }

        if (!Util::instanceOf<LogicalFunctionAdd>(sortedLeft) || !Util::instanceOf<LogicalFunctionAdd>(sortedRight))
        {
            auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
            auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
            int compared = leftSortedFieldName.compare(rightSortedFieldName);
            if (compared > 0)
            {
                return LogicalFunctionAdd::create(sortedRight, sortedLeft);
            }
        }

        return LogicalFunctionAdd::create(sortedLeft, sortedRight);
    }
    if (Util::instanceOf<SubBinaryLogicalFunction>(function))
    {
        auto subLogicalFunction = Util::as<SubBinaryLogicalFunction>(function);
        auto left = subLogicalFunction->getLeft();
        auto right = subLogicalFunction->getRight();
        sortAttributesInFunction(left);
        sortAttributesInFunction(right);
        return function;
    }
    else if (Util::instanceOf<MulBinaryLogicalFunction>(function))
    {
        auto mulLogicalFunction = Util::as<MulBinaryLogicalFunction>(function);
        auto left = mulLogicalFunction->getLeft();
        auto right = mulLogicalFunction->getRight();

        auto sortedLeft = sortAttributesInFunction(left);
        auto sortedRight = sortAttributesInFunction(right);

        auto leftCommutativeFields = fetchCommutativeFields<MulBinaryLogicalFunction>(sortedLeft);
        auto rightCommutativeFields = fetchCommutativeFields<MulBinaryLogicalFunction>(sortedRight);

        std::vector<std::shared_ptr<LogicalFunction>> allCommutativeFields;
        allCommutativeFields.insert(allCommutativeFields.end(), leftCommutativeFields.begin(), leftCommutativeFields.end());
        allCommutativeFields.insert(allCommutativeFields.end(), rightCommutativeFields.begin(), rightCommutativeFields.end());

        std::vector<std::shared_ptr<LogicalFunction>> sortedCommutativeFields;
        sortedCommutativeFields.reserve(allCommutativeFields.size());
        for (const auto& commutativeField : allCommutativeFields)
        {
            sortedCommutativeFields.push_back(commutativeField->deepCopy());
        }

        std::sort(
            sortedCommutativeFields.begin(),
            sortedCommutativeFields.end(),
            [](const std::shared_ptr<LogicalFunction>& lhsField, const std::shared_ptr<LogicalFunction>& rhsField)
            {
                std::string leftValue;
                std::string rightValue;

                if (Util::instanceOf<ConstantValueLogicalFunction>(lhsField))
                {
                    leftValue = Util::as<ConstantValueLogicalFunction>(lhsField)->getConstantValue();
                }
                else
                {
                    leftValue = Util::as<FieldAccessLogicalFunction>(lhsField)->getFieldName();
                }

                if (Util::instanceOf<ConstantValueLogicalFunction>(rhsField))
                {
                    rightValue = Util::as<ConstantValueLogicalFunction>(rhsField)->getConstantValue();
                }
                else
                {
                    rightValue = Util::as<FieldAccessLogicalFunction>(rhsField)->getFieldName();
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
            else if (!(Util::instanceOf<FieldAccessLogicalFunction>(sortedLeft)
                       || Util::instanceOf<ConstantValueLogicalFunction>(sortedLeft)))
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
            else if (!(Util::instanceOf<FieldAccessLogicalFunction>(sortedRight)
                       || Util::instanceOf<ConstantValueLogicalFunction>(sortedRight)))
            {
                bool replaced = replaceCommutativeFunctions(sortedRight, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }
        }

        if (!Util::instanceOf<MulBinaryLogicalFunction>(sortedLeft) || !Util::instanceOf<MulBinaryLogicalFunction>(sortedRight))
        {
            auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
            auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
            int compared = leftSortedFieldName.compare(rightSortedFieldName);
            if (compared > 0)
            {
                return MulBinaryLogicalFunction::create(sortedRight, sortedLeft);
            }
        }

        return MulBinaryLogicalFunction::create(sortedLeft, sortedRight);
    }
    else if (Util::instanceOf<DivBinaryLogicalFunction>(function))
    {
        auto divLogicalFunction = Util::as<DivBinaryLogicalFunction>(function);
        auto left = divLogicalFunction->getLeft();
        auto right = divLogicalFunction->getRight();
        sortAttributesInFunction(left);
        sortAttributesInFunction(right);
        return function;
    }
    throw NotImplemented("No conversion to Z3 function implemented for the arithmetical function node: ", *function);
}

std::shared_ptr<LogicalFunction> AttributeSortRule::sortAttributesInLogicalFunctions(const std::shared_ptr<LogicalFunction>& function)
{
    NES_DEBUG("Create Z3 function node for logical function {}", *function);
    if (Util::instanceOf<AndBinaryLogicalFunction>(function))
    {
        auto andLogicalFunction = Util::as<AndBinaryLogicalFunction>(function);
        auto left = andLogicalFunction->getLeft();
        auto right = andLogicalFunction->getRight();
        auto sortedLeft = sortAttributesInFunction(left);
        auto sortedRight = sortAttributesInFunction(right);

        auto leftCommutativeFields = fetchCommutativeFields<AndBinaryLogicalFunction>(sortedLeft);
        auto rightCommutativeFields = fetchCommutativeFields<AndBinaryLogicalFunction>(sortedRight);

        std::vector<std::shared_ptr<LogicalFunction>> allCommutativeFields;
        allCommutativeFields.insert(allCommutativeFields.end(), leftCommutativeFields.begin(), leftCommutativeFields.end());
        allCommutativeFields.insert(allCommutativeFields.end(), rightCommutativeFields.begin(), rightCommutativeFields.end());

        std::vector<std::shared_ptr<LogicalFunction>> sortedCommutativeFields;
        sortedCommutativeFields.reserve(allCommutativeFields.size());
        for (const auto& commutativeField : allCommutativeFields)
        {
            sortedCommutativeFields.push_back(commutativeField->deepCopy());
        }

        std::sort(
            sortedCommutativeFields.begin(),
            sortedCommutativeFields.end(),
            [](const std::shared_ptr<LogicalFunction>& lhsField, const std::shared_ptr<LogicalFunction>& rhsField)
            {
                std::string leftValue;
                std::string rightValue;

                if (Util::instanceOf<ConstantValueLogicalFunction>(lhsField))
                {
                    leftValue = Util::as<ConstantValueLogicalFunction>(lhsField)->getConstantValue();
                }
                else
                {
                    leftValue = Util::as<FieldAccessLogicalFunction>(lhsField)->getFieldName();
                }

                if (Util::instanceOf<ConstantValueLogicalFunction>(rhsField))
                {
                    rightValue = Util::as<ConstantValueLogicalFunction>(rhsField)->getConstantValue();
                }
                else
                {
                    rightValue = Util::as<FieldAccessLogicalFunction>(rhsField)->getFieldName();
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
            else if (!(Util::instanceOf<FieldAccessLogicalFunction>(sortedLeft)
                       || Util::instanceOf<ConstantValueLogicalFunction>(sortedLeft)))
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
            else if (!(Util::instanceOf<FieldAccessLogicalFunction>(sortedRight)
                       || Util::instanceOf<ConstantValueLogicalFunction>(sortedRight)))
            {
                bool replaced = replaceCommutativeFunctions(sortedRight, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }
        }

        if (!Util::instanceOf<AndBinaryLogicalFunction>(sortedLeft) || !Util::instanceOf<AndBinaryLogicalFunction>(sortedRight))
        {
            auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
            auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
            int compared = leftSortedFieldName.compare(rightSortedFieldName);
            if (compared > 0)
            {
                return AndBinaryLogicalFunction::create(sortedRight, sortedLeft);
            }
        }
        return AndBinaryLogicalFunction::create(sortedLeft, sortedRight);
    }
    if (Util::instanceOf<OrBinaryLogicalFunction>(function))
    {
        auto orLogicalFunction = Util::as<OrBinaryLogicalFunction>(function);
        auto left = orLogicalFunction->getLeft();
        auto right = orLogicalFunction->getRight();
        auto sortedLeft = sortAttributesInFunction(left);
        auto sortedRight = sortAttributesInFunction(right);

        auto leftCommutativeFields = fetchCommutativeFields<OrBinaryLogicalFunction>(sortedLeft);
        auto rightCommutativeFields = fetchCommutativeFields<OrBinaryLogicalFunction>(sortedRight);

        std::vector<std::shared_ptr<LogicalFunction>> allCommutativeFields;
        allCommutativeFields.insert(allCommutativeFields.end(), leftCommutativeFields.begin(), leftCommutativeFields.end());
        allCommutativeFields.insert(allCommutativeFields.end(), rightCommutativeFields.begin(), rightCommutativeFields.end());

        std::vector<std::shared_ptr<LogicalFunction>> sortedCommutativeFields;
        sortedCommutativeFields.reserve(allCommutativeFields.size());
        for (const auto& commutativeField : allCommutativeFields)
        {
            sortedCommutativeFields.push_back(commutativeField->deepCopy());
        }

        std::sort(
            sortedCommutativeFields.begin(),
            sortedCommutativeFields.end(),
            [](const std::shared_ptr<LogicalFunction>& lhsField, const std::shared_ptr<LogicalFunction>& rhsField)
            {
                std::string leftValue;
                std::string rightValue;

                if (Util::instanceOf<ConstantValueLogicalFunction>(lhsField))
                {
                    leftValue = Util::as<ConstantValueLogicalFunction>(lhsField)->getConstantValue();
                }
                else
                {
                    leftValue = Util::as<FieldAccessLogicalFunction>(lhsField)->getFieldName();
                }

                if (Util::instanceOf<ConstantValueLogicalFunction>(rhsField))
                {
                    rightValue = Util::as<ConstantValueLogicalFunction>(rhsField)->getConstantValue();
                }
                else
                {
                    rightValue = Util::as<FieldAccessLogicalFunction>(rhsField)->getFieldName();
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
            else if (!(Util::instanceOf<FieldAccessLogicalFunction>(sortedLeft)
                       || Util::instanceOf<ConstantValueLogicalFunction>(sortedLeft)))
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
            else if (!(Util::instanceOf<FieldAccessLogicalFunction>(sortedRight)
                       || Util::instanceOf<ConstantValueLogicalFunction>(sortedRight)))
            {
                bool replaced = replaceCommutativeFunctions(sortedRight, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }
        }

        if (!Util::instanceOf<OrBinaryLogicalFunction>(sortedLeft) || !Util::instanceOf<OrBinaryLogicalFunction>(sortedRight))
        {
            auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
            auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
            int compared = leftSortedFieldName.compare(rightSortedFieldName);
            if (compared > 0)
            {
                return OrBinaryLogicalFunction::create(sortedRight, sortedLeft);
            }
        }
        return OrBinaryLogicalFunction::create(sortedLeft, sortedRight);
    }
    else if (Util::instanceOf<LessBinaryLogicalFunction>(function))
    {
        auto lessLogicalFunction = Util::as<LessBinaryLogicalFunction>(function);
        auto left = lessLogicalFunction->getLeft();
        auto right = lessLogicalFunction->getRight();

        auto sortedLeft = sortAttributesInFunction(left);
        auto sortedRight = sortAttributesInFunction(right);

        auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
        auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
        int compared = leftSortedFieldName.compare(rightSortedFieldName);
        if (compared > 0)
        {
            return GreaterBinaryLogicalFunction::create(sortedRight, sortedLeft);
        }
        return LessBinaryLogicalFunction::create(sortedLeft, sortedRight);
    }
    else if (Util::instanceOf<LessEqualsBinaryLogicalFunction>(function))
    {
        auto lessEqualsBinaryLogicalFunction = Util::as<LessEqualsBinaryLogicalFunction>(function);
        auto left = lessEqualsBinaryLogicalFunction->getLeft();
        auto right = lessEqualsBinaryLogicalFunction->getRight();
        auto sortedLeft = sortAttributesInFunction(left);
        auto sortedRight = sortAttributesInFunction(right);

        auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
        auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
        int compared = leftSortedFieldName.compare(rightSortedFieldName);
        if (compared > 0)
        {
            return GreaterBinaryLogicalFunctionEquals::create(sortedRight, sortedLeft);
        }
        return LessEqualsBinaryLogicalFunction::create(sortedLeft, sortedRight);
    }
    else if (Util::instanceOf<GreaterBinaryLogicalFunction>(function))
    {
        auto greaterLogicalFunction = Util::as<GreaterBinaryLogicalFunction>(function);
        auto left = greaterLogicalFunction->getLeft();
        auto right = greaterLogicalFunction->getRight();

        auto sortedLeft = sortAttributesInFunction(left);
        auto sortedRight = sortAttributesInFunction(right);

        auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
        auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
        int compared = leftSortedFieldName.compare(rightSortedFieldName);
        if (compared > 0)
        {
            return LessBinaryLogicalFunction::create(sortedRight, sortedLeft);
        }
        return GreaterBinaryLogicalFunction::create(sortedLeft, sortedRight);
    }
    else if (Util::instanceOf<GreaterBinaryLogicalFunctionEquals>(function))
    {
        auto greaterEqualsBinaryLogicalFunction = Util::as<GreaterBinaryLogicalFunctionEquals>(function);
        auto left = greaterEqualsBinaryLogicalFunction->getLeft();
        auto right = greaterEqualsBinaryLogicalFunction->getRight();

        auto sortedLeft = sortAttributesInFunction(left);
        auto sortedRight = sortAttributesInFunction(right);

        auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
        auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
        int compared = leftSortedFieldName.compare(rightSortedFieldName);
        if (compared > 0)
        {
            return LessEqualsBinaryLogicalFunction::create(sortedRight, sortedLeft);
        }
        return GreaterBinaryLogicalFunctionEquals::create(sortedLeft, sortedRight);
    }
    else if (Util::instanceOf<EqualsBinaryLogicalFunction>(function))
    {
        auto equalsLogicalFunction = Util::as<EqualsBinaryLogicalFunction>(function);
        auto left = equalsLogicalFunction->getLeft();
        auto right = equalsLogicalFunction->getRight();
        auto sortedLeft = sortAttributesInFunction(left);
        auto sortedRight = sortAttributesInFunction(right);

        auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
        auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
        int compared = leftSortedFieldName.compare(rightSortedFieldName);
        if (compared > 0)
        {
            return EqualsBinaryLogicalFunction::create(sortedRight, sortedLeft);
        }
        return EqualsBinaryLogicalFunction::create(sortedLeft, sortedRight);
    }
    else if (Util::instanceOf<NegateUnaryLogicalFunction>(function))
    {
        auto negateLogicalFunction = Util::as<NegateUnaryLogicalFunction>(function);
        auto childFunction = negateLogicalFunction->child();
        auto updatedChildFunction = sortAttributesInFunction(childFunction);
        return NegateUnaryLogicalFunction::create(updatedChildFunction);
    }
    throw NotImplemented("No conversion to Z3 function implemented for the logical function node: ", *function);
}

bool AttributeSortRule::replaceCommutativeFunctions(
    const std::shared_ptr<LogicalFunction>& parentFunction,
    const std::shared_ptr<LogicalFunction>& originalFunction,
    const std::shared_ptr<LogicalFunction>& updatedFunction)
{
    auto binaryFunction = Util::as<BinaryLogicalFunction>(parentFunction);

    const std::shared_ptr<LogicalFunction>& leftChild = binaryFunction->getLeft();
    const std::shared_ptr<LogicalFunction>& rightChild = binaryFunction->getRight();
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
        auto children = parentFunction->children;
        for (const auto& child : children)
        {
            if (!(Util::instanceOf<FieldAccessLogicalFunction>(child) || Util::instanceOf<ConstantValueLogicalFunction>(child)))
            {
                bool replaced = replaceCommutativeFunctions(Util::as<LogicalFunction>(child), originalFunction, updatedFunction);
                if (replaced)
                {
                    return true;
                }
            }
        }
    }
    return false;
}

std::string AttributeSortRule::fetchLeftMostConstantValueOrFieldName(std::shared_ptr<LogicalFunction> function)
{
    std::shared_ptr<LogicalFunction> startPoint = std::move(function);
    while (!(Util::instanceOf<FieldAccessLogicalFunction>(startPoint) || Util::instanceOf<ConstantValueLogicalFunction>(startPoint)))
    {
        startPoint = Util::as<LogicalFunction>(startPoint->children[0]);
    }

    if (Util::instanceOf<FieldAccessLogicalFunction>(startPoint))
    {
        return Util::as<FieldAccessLogicalFunction>(startPoint)->getFieldName();
    }
    return Util::as<ConstantValueLogicalFunction>(startPoint)->getConstantValue();
}

}
