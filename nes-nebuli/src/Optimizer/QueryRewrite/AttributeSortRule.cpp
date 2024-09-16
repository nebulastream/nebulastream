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

#include <numeric>
#include <utility>
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
#include <Functions/NodeFunctionConstantValue.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Optimizer/QueryRewrite/AttributeSortRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/ValueTypes/ArrayValue.hpp>
#include <Common/ValueTypes/BasicValue.hpp>

namespace NES::Optimizer
{

AttributeSortRulePtr AttributeSortRule::create()
{
    return std::make_shared<AttributeSortRule>();
}

QueryPlanPtr AttributeSortRule::apply(NES::QueryPlanPtr queryPlan)
{
    auto filterOperators = queryPlan->getOperatorByType<LogicalFilterOperator>();
    for (auto const& filterOperator : filterOperators)
    {
        auto predicate = filterOperator->getPredicate();
        auto updatedPredicate = sortAttributesInFunction(predicate);
        auto updatedFilter = LogicalOperatorFactory::createFilterOperator(updatedPredicate);
        updatedFilter->setInputSchema(filterOperator->getInputSchema()->copy());
        updatedFilter->as_if<LogicalOperator>()->setOutputSchema(filterOperator->as_if<LogicalOperator>()->getOutputSchema()->copy());
        filterOperator->replace(updatedFilter);
    }

    auto mapOperators = queryPlan->getOperatorByType<LogicalMapOperator>();
    for (auto const& mapOperator : mapOperators)
    {
        auto mapFunction = mapOperator->getMapFunction();
        auto updatedMapFunction = sortAttributesInFunction(mapFunction)->as<NodeFunctionFieldAssignment>();
        auto updatedMap = LogicalOperatorFactory::createMapOperator(updatedMapFunction);
        updatedMap->setInputSchema(mapOperator->getInputSchema()->copy());
        updatedMap->as_if<LogicalOperator>()->setOutputSchema(mapOperator->as_if<LogicalOperator>()->getOutputSchema()->copy());
        mapOperator->replace(updatedMap);
    }
    return queryPlan;
}

NES::NodeFunctionPtr AttributeSortRule::sortAttributesInFunction(NES::NodeFunctionPtr function)
{
    NES_DEBUG("Sorting attributed for input function {}", function->toString());
    if (function->instanceOf<NES::LogicalFunctionNode>())
    {
        return sortAttributesInLogicalFunctions(function);
    }
    if (function->instanceOf<NES::ArithmeticalFunctionNode>())
    {
        return sortAttributesInArithmeticalFunctions(function);
    }
    else if (function->instanceOf<NES::NodeFunctionFieldAssignment>())
    {
        auto fieldAssignmentFunctionNode = function->as<NES::NodeFunctionFieldAssignment>();
        auto assignment = fieldAssignmentFunctionNode->getAssignment();
        auto updatedAssignment = sortAttributesInFunction(assignment);
        auto field = fieldAssignmentFunctionNode->getField();
        return NES::NodeFunctionFieldAssignment::create(field, updatedAssignment);
    }
    else if (function->instanceOf<NES::NodeFunctionConstantValue>() || function->instanceOf<NES::NodeFunctionFieldAccess>())
    {
        return function;
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 function implemented for the function: " + function->toString());
    return nullptr;
}

NodeFunctionPtr AttributeSortRule::sortAttributesInArithmeticalFunctions(NodeFunctionPtr function)
{
    NES_DEBUG("Create Z3 function for arithmetical function {}", function->toString());
    if (function->instanceOf<NES::NodeFunctionAdd>())
    {
        auto addFunctionNode = function->as<NES::NodeFunctionAdd>();

        auto sortedLeft = sortAttributesInFunction(addFunctionNode->getLeft());
        auto sortedRight = sortAttributesInFunction(addFunctionNode->getRight());

        auto leftCommutativeFields = fetchCommutativeFields<NES::NodeFunctionAdd>(sortedLeft);
        auto rightCommutativeFields = fetchCommutativeFields<NES::NodeFunctionAdd>(sortedRight);

        std::vector<NodeFunctionPtr> allCommutativeFields;
        allCommutativeFields.insert(allCommutativeFields.end(), leftCommutativeFields.begin(), leftCommutativeFields.end());
        allCommutativeFields.insert(allCommutativeFields.end(), rightCommutativeFields.begin(), rightCommutativeFields.end());

        std::vector<NodeFunctionPtr> sortedCommutativeFields;
        sortedCommutativeFields.reserve(allCommutativeFields.size());
        for (const auto& commutativeField : allCommutativeFields)
        {
            sortedCommutativeFields.push_back(commutativeField->deepCopy());
        }

        std::sort(
            sortedCommutativeFields.begin(),
            sortedCommutativeFields.end(),
            [](const NES::NodeFunctionPtr& lhsField, const NES::NodeFunctionPtr& rhsField)
            {
                std::string leftValue;
                std::string rightValue;

                if (lhsField->instanceOf<NES::NodeFunctionConstantValue>())
                {
                    auto constantValue = lhsField->as<NES::NodeFunctionConstantValue>()->getConstantValue();
                    auto basicValueType = std::dynamic_pointer_cast<BasicValue>(constantValue);
                    leftValue = basicValueType->value;
                }
                else
                {
                    leftValue = lhsField->as<NES::NodeFunctionFieldAccess>()->getFieldName();
                }

                if (rhsField->instanceOf<NodeFunctionConstantValue>())
                {
                    auto constantValue = rhsField->as<NodeFunctionConstantValue>()->getConstantValue();
                    auto basicValueType = std::dynamic_pointer_cast<BasicValue>(constantValue);
                    rightValue = basicValueType->value;
                }
                else
                {
                    rightValue = rhsField->as<NES::NodeFunctionFieldAccess>()->getFieldName();
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
            else if (!(sortedLeft->instanceOf<NodeFunctionFieldAccess>() || sortedLeft->instanceOf<NodeFunctionConstantValue>()))
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
            else if (!(sortedRight->instanceOf<NodeFunctionFieldAccess>() || sortedRight->instanceOf<NodeFunctionConstantValue>()))
            {
                bool replaced = replaceCommutativeFunctions(sortedRight, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }
        }

        if (!sortedLeft->instanceOf<NodeFunctionAdd>() || !sortedRight->instanceOf<NodeFunctionAdd>())
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
    if (function->instanceOf<NodeFunctionSub>())
    {
        auto subFunctionNode = function->as<NodeFunctionSub>();
        auto left = subFunctionNode->getLeft();
        auto right = subFunctionNode->getRight();
        sortAttributesInFunction(left);
        sortAttributesInFunction(right);
        return function;
    }
    else if (function->instanceOf<NodeFunctionMul>())
    {
        auto mulFunctionNode = function->as<NodeFunctionMul>();
        auto left = mulFunctionNode->getLeft();
        auto right = mulFunctionNode->getRight();

        auto sortedLeft = sortAttributesInFunction(left);
        auto sortedRight = sortAttributesInFunction(right);

        auto leftCommutativeFields = fetchCommutativeFields<NodeFunctionMul>(sortedLeft);
        auto rightCommutativeFields = fetchCommutativeFields<NodeFunctionMul>(sortedRight);

        std::vector<NodeFunctionPtr> allCommutativeFields;
        allCommutativeFields.insert(allCommutativeFields.end(), leftCommutativeFields.begin(), leftCommutativeFields.end());
        allCommutativeFields.insert(allCommutativeFields.end(), rightCommutativeFields.begin(), rightCommutativeFields.end());

        std::vector<NodeFunctionPtr> sortedCommutativeFields;
        sortedCommutativeFields.reserve(allCommutativeFields.size());
        for (const auto& commutativeField : allCommutativeFields)
        {
            sortedCommutativeFields.push_back(commutativeField->deepCopy());
        }

        std::sort(
            sortedCommutativeFields.begin(),
            sortedCommutativeFields.end(),
            [](const NodeFunctionPtr& lhsField, const NodeFunctionPtr& rhsField)
            {
                std::string leftValue;
                std::string rightValue;

                if (lhsField->instanceOf<NodeFunctionConstantValue>())
                {
                    auto constantValue = lhsField->as<NodeFunctionConstantValue>()->getConstantValue();
                    auto basicValueType = std::dynamic_pointer_cast<BasicValue>(constantValue);
                    leftValue = basicValueType->value;
                }
                else
                {
                    leftValue = lhsField->as<NodeFunctionFieldAccess>()->getFieldName();
                }

                if (rhsField->instanceOf<NodeFunctionConstantValue>())
                {
                    auto constantValue = rhsField->as<NodeFunctionConstantValue>()->getConstantValue();
                    auto basicValueType = std::dynamic_pointer_cast<BasicValue>(constantValue);
                    rightValue = basicValueType->value;
                }
                else
                {
                    rightValue = rhsField->as<NodeFunctionFieldAccess>()->getFieldName();
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
            else if (!(sortedLeft->instanceOf<NodeFunctionFieldAccess>() || sortedLeft->instanceOf<NodeFunctionConstantValue>()))
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
            else if (!(sortedRight->instanceOf<NodeFunctionFieldAccess>() || sortedRight->instanceOf<NodeFunctionConstantValue>()))
            {
                bool replaced = replaceCommutativeFunctions(sortedRight, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }
        }

        if (!sortedLeft->instanceOf<NodeFunctionMul>() || !sortedRight->instanceOf<NodeFunctionMul>())
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
    else if (function->instanceOf<NodeFunctionDiv>())
    {
        auto divFunctionNode = function->as<NodeFunctionDiv>();
        auto left = divFunctionNode->getLeft();
        auto right = divFunctionNode->getRight();
        sortAttributesInFunction(left);
        sortAttributesInFunction(right);
        return function;
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 function implemented for the arithmetical function node: " + function->toString());
    return nullptr;
}

NodeFunctionPtr AttributeSortRule::sortAttributesInLogicalFunctions(const NodeFunctionPtr& function)
{
    NES_DEBUG("Create Z3 function node for logical function {}", function->toString());
    if (function->instanceOf<NodeFunctionAnd>())
    {
        auto andFunctionNode = function->as<NodeFunctionAnd>();
        auto left = andFunctionNode->getLeft();
        auto right = andFunctionNode->getRight();
        auto sortedLeft = sortAttributesInFunction(left);
        auto sortedRight = sortAttributesInFunction(right);

        auto leftCommutativeFields = fetchCommutativeFields<NodeFunctionAnd>(sortedLeft);
        auto rightCommutativeFields = fetchCommutativeFields<NodeFunctionAnd>(sortedRight);

        std::vector<NodeFunctionPtr> allCommutativeFields;
        allCommutativeFields.insert(allCommutativeFields.end(), leftCommutativeFields.begin(), leftCommutativeFields.end());
        allCommutativeFields.insert(allCommutativeFields.end(), rightCommutativeFields.begin(), rightCommutativeFields.end());

        std::vector<NodeFunctionPtr> sortedCommutativeFields;
        sortedCommutativeFields.reserve(allCommutativeFields.size());
        for (const auto& commutativeField : allCommutativeFields)
        {
            sortedCommutativeFields.push_back(commutativeField->deepCopy());
        }

        std::sort(
            sortedCommutativeFields.begin(),
            sortedCommutativeFields.end(),
            [](const NodeFunctionPtr& lhsField, const NodeFunctionPtr& rhsField)
            {
                std::string leftValue;
                std::string rightValue;

                if (lhsField->instanceOf<NodeFunctionConstantValue>())
                {
                    auto constantValue = lhsField->as<NodeFunctionConstantValue>()->getConstantValue();
                    auto basicValueType = std::dynamic_pointer_cast<BasicValue>(constantValue);
                    leftValue = basicValueType->value;
                }
                else
                {
                    leftValue = lhsField->as<NodeFunctionFieldAccess>()->getFieldName();
                }

                if (rhsField->instanceOf<NodeFunctionConstantValue>())
                {
                    auto constantValue = rhsField->as<NodeFunctionConstantValue>()->getConstantValue();
                    auto basicValueType = std::dynamic_pointer_cast<BasicValue>(constantValue);
                    rightValue = basicValueType->value;
                }
                else
                {
                    rightValue = rhsField->as<NodeFunctionFieldAccess>()->getFieldName();
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
            else if (!(sortedLeft->instanceOf<NodeFunctionFieldAccess>() || sortedLeft->instanceOf<NodeFunctionConstantValue>()))
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
            else if (!(sortedRight->instanceOf<NodeFunctionFieldAccess>() || sortedRight->instanceOf<NodeFunctionConstantValue>()))
            {
                bool replaced = replaceCommutativeFunctions(sortedRight, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }
        }

        if (!sortedLeft->instanceOf<NodeFunctionAnd>() || !sortedRight->instanceOf<NodeFunctionAnd>())
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
    if (function->instanceOf<NodeFunctionOr>())
    {
        auto orFunctionNode = function->as<NodeFunctionOr>();
        auto left = orFunctionNode->getLeft();
        auto right = orFunctionNode->getRight();
        auto sortedLeft = sortAttributesInFunction(left);
        auto sortedRight = sortAttributesInFunction(right);

        auto leftCommutativeFields = fetchCommutativeFields<NodeFunctionOr>(sortedLeft);
        auto rightCommutativeFields = fetchCommutativeFields<NodeFunctionOr>(sortedRight);

        std::vector<NodeFunctionPtr> allCommutativeFields;
        allCommutativeFields.insert(allCommutativeFields.end(), leftCommutativeFields.begin(), leftCommutativeFields.end());
        allCommutativeFields.insert(allCommutativeFields.end(), rightCommutativeFields.begin(), rightCommutativeFields.end());

        std::vector<NodeFunctionPtr> sortedCommutativeFields;
        sortedCommutativeFields.reserve(allCommutativeFields.size());
        for (const auto& commutativeField : allCommutativeFields)
        {
            sortedCommutativeFields.push_back(commutativeField->deepCopy());
        }

        std::sort(
            sortedCommutativeFields.begin(),
            sortedCommutativeFields.end(),
            [](const NodeFunctionPtr& lhsField, const NodeFunctionPtr& rhsField)
            {
                std::string leftValue;
                std::string rightValue;

                if (lhsField->instanceOf<NodeFunctionConstantValue>())
                {
                    auto constantValue = lhsField->as<NodeFunctionConstantValue>()->getConstantValue();
                    auto basicValueType = std::dynamic_pointer_cast<BasicValue>(constantValue);
                    leftValue = basicValueType->value;
                }
                else
                {
                    leftValue = lhsField->as<NodeFunctionFieldAccess>()->getFieldName();
                }

                if (rhsField->instanceOf<NodeFunctionConstantValue>())
                {
                    auto constantValue = rhsField->as<NodeFunctionConstantValue>()->getConstantValue();
                    auto basicValueType = std::dynamic_pointer_cast<BasicValue>(constantValue);
                    rightValue = basicValueType->value;
                }
                else
                {
                    rightValue = rhsField->as<NodeFunctionFieldAccess>()->getFieldName();
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
            else if (!(sortedLeft->instanceOf<NodeFunctionFieldAccess>() || sortedLeft->instanceOf<NodeFunctionConstantValue>()))
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
            else if (!(sortedRight->instanceOf<NodeFunctionFieldAccess>() || sortedRight->instanceOf<NodeFunctionConstantValue>()))
            {
                bool replaced = replaceCommutativeFunctions(sortedRight, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }
        }

        if (!sortedLeft->instanceOf<NodeFunctionOr>() || !sortedRight->instanceOf<NodeFunctionOr>())
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
    else if (function->instanceOf<NodeFunctionLess>())
    {
        auto lessFunctionNode = function->as<NodeFunctionLess>();
        auto left = lessFunctionNode->getLeft();
        auto right = lessFunctionNode->getRight();

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
    else if (function->instanceOf<NodeFunctionLessEquals>())
    {
        auto lessNodeFunctionEquals = function->as<NodeFunctionLessEquals>();
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
    else if (function->instanceOf<NodeFunctionGreater>())
    {
        auto greaterFunctionNode = function->as<NodeFunctionGreater>();
        auto left = greaterFunctionNode->getLeft();
        auto right = greaterFunctionNode->getRight();

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
    else if (function->instanceOf<NodeFunctionGreaterEquals>())
    {
        auto greaterNodeFunctionEquals = function->as<NodeFunctionGreaterEquals>();
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
    else if (function->instanceOf<NodeFunctionEquals>())
    {
        auto equalsFunctionNode = function->as<NodeFunctionEquals>();
        auto left = equalsFunctionNode->getLeft();
        auto right = equalsFunctionNode->getRight();
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
    else if (function->instanceOf<NodeFunctionNegate>())
    {
        auto negateFunctionNode = function->as<NodeFunctionNegate>();
        auto childFunction = negateFunctionNode->child();
        auto updatedChildFunction = sortAttributesInFunction(childFunction);
        return NodeFunctionNegate::create(updatedChildFunction);
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 function possible for the logical function node: " + function->toString());
    return nullptr;
}

bool AttributeSortRule::replaceCommutativeFunctions(
    const NodeFunctionPtr& parentFunction, const NodeFunctionPtr& originalFunction, const NodeFunctionPtr& updatedFunction)
{
    auto binaryFunction = parentFunction->as<NodeFunctionBinary>();

    const NodeFunctionPtr& leftChild = binaryFunction->getLeft();
    const NodeFunctionPtr& rightChild = binaryFunction->getRight();
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
            if (!(child->instanceOf<NodeFunctionFieldAccess>() || child->instanceOf<NodeFunctionConstantValue>()))
            {
                bool replaced = replaceCommutativeFunctions(child->as<FunctionNode>(), originalFunction, updatedFunction);
                if (replaced)
                {
                    return true;
                }
            }
        }
    }
    return false;
}

std::string AttributeSortRule::fetchLeftMostConstantValueOrFieldName(NodeFunctionPtr function)
{
    NodeFunctionPtr startPoint = std::move(function);
    while (!(startPoint->instanceOf<NodeFunctionFieldAccess>() || startPoint->instanceOf<NodeFunctionConstantValue>()))
    {
        startPoint = startPoint->getChildren()[0]->as<FunctionNode>();
    }

    if (startPoint->instanceOf<NodeFunctionFieldAccess>())
    {
        return startPoint->template as<NodeFunctionFieldAccess>()->getFieldName();
    }
    const ValueTypePtr& constantValue = startPoint->as<NodeFunctionConstantValue>()->getConstantValue();
    if (auto basicValueType = std::dynamic_pointer_cast<BasicValue>(constantValue); basicValueType)
    {
        return basicValueType->value;
    }

    if (auto arrayValueType = std::dynamic_pointer_cast<ArrayValue>(constantValue); arrayValueType)
    {
        return std::accumulate(arrayValueType->values.begin(), arrayValueType->values.end(), std::string());
    }

    NES_THROW_RUNTIME_ERROR("AttributeSortRule not equipped for handling value type!");
}

} /// namespace NES::Optimizer
