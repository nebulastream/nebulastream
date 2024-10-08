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
#include <Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Expressions/ConstantValueExpressionNode.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Expressions/FieldAssignmentExpressionNode.hpp>
#include <Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
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
        auto updatedPredicate = sortAttributesInExpression(predicate);
        auto updatedFilter = LogicalOperatorFactory::createFilterOperator(updatedPredicate);
        updatedFilter->setInputSchema(filterOperator->getInputSchema()->copy());
        NES::Util::as_if<LogicalOperator>(updatedFilter)
            ->setOutputSchema(NES::Util::as_if<LogicalOperator>(filterOperator)->getOutputSchema()->copy());
        filterOperator->replace(updatedFilter);
    }

    auto mapOperators = queryPlan->getOperatorByType<LogicalMapOperator>();
    for (auto const& mapOperator : mapOperators)
    {
        auto mapExpression = mapOperator->getMapExpression();
        auto updatedMapExpression = NES::Util::as<FieldAssignmentExpressionNode>(sortAttributesInExpression(mapExpression));
        auto updatedMap = LogicalOperatorFactory::createMapOperator(updatedMapExpression);
        updatedMap->setInputSchema(mapOperator->getInputSchema()->copy());
        NES::Util::as_if<LogicalOperator>(updatedMap)
            ->setOutputSchema(NES::Util::as_if<LogicalOperator>(mapOperator)->getOutputSchema()->copy());
        mapOperator->replace(updatedMap);
    }
    return queryPlan;
}

NES::ExpressionNodePtr AttributeSortRule::sortAttributesInExpression(NES::ExpressionNodePtr expression)
{
    NES_DEBUG("Sorting attributed for input expression {}", expression->toString());
    if (NES::Util::instanceOf<NES::LogicalExpressionNode>(expression))
    {
        return sortAttributesInLogicalExpressions(expression);
    }
    if (NES::Util::instanceOf<NES::ArithmeticalExpressionNode>(expression))
    {
        return sortAttributesInArithmeticalExpressions(expression);
    }
    else if (NES::Util::instanceOf<NES::FieldAssignmentExpressionNode>(expression))
    {
        auto fieldAssignmentExpressionNode = NES::Util::as<NES::FieldAssignmentExpressionNode>(expression);
        auto assignment = fieldAssignmentExpressionNode->getAssignment();
        auto updatedAssignment = sortAttributesInExpression(assignment);
        auto field = fieldAssignmentExpressionNode->getField();
        return NES::FieldAssignmentExpressionNode::create(field, updatedAssignment);
    }
    else if (
        NES::Util::instanceOf<NES::ConstantValueExpressionNode>(expression)
        || NES::Util::instanceOf<NES::FieldAccessExpressionNode>(expression))
    {
        return expression;
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression implemented for the expression: " + expression->toString());
    return nullptr;
}

ExpressionNodePtr AttributeSortRule::sortAttributesInArithmeticalExpressions(ExpressionNodePtr expression)
{
    NES_DEBUG("Create Z3 expression for arithmetical expression {}", expression->toString());
    if (NES::Util::instanceOf<NES::AddExpressionNode>(expression))
    {
        auto addExpressionNode = NES::Util::as<NES::AddExpressionNode>(expression);

        auto sortedLeft = sortAttributesInExpression(addExpressionNode->getLeft());
        auto sortedRight = sortAttributesInExpression(addExpressionNode->getRight());

        auto leftCommutativeFields = fetchCommutativeFields<NES::AddExpressionNode>(sortedLeft);
        auto rightCommutativeFields = fetchCommutativeFields<NES::AddExpressionNode>(sortedRight);

        std::vector<ExpressionNodePtr> allCommutativeFields;
        allCommutativeFields.insert(allCommutativeFields.end(), leftCommutativeFields.begin(), leftCommutativeFields.end());
        allCommutativeFields.insert(allCommutativeFields.end(), rightCommutativeFields.begin(), rightCommutativeFields.end());

        std::vector<ExpressionNodePtr> sortedCommutativeFields;
        sortedCommutativeFields.reserve(allCommutativeFields.size());
        for (const auto& commutativeField : allCommutativeFields)
        {
            sortedCommutativeFields.push_back(commutativeField->copy());
        }

        std::sort(
            sortedCommutativeFields.begin(),
            sortedCommutativeFields.end(),
            [](const NES::ExpressionNodePtr& lhsField, const NES::ExpressionNodePtr& rhsField)
            {
                std::string leftValue;
                std::string rightValue;

                if (NES::Util::instanceOf<NES::ConstantValueExpressionNode>(lhsField))
                {
                    auto constantValue = NES::Util::as<NES::ConstantValueExpressionNode>(lhsField)->getConstantValue();
                    auto basicValueType = std::dynamic_pointer_cast<BasicValue>(constantValue);
                    leftValue = basicValueType->value;
                }
                else
                {
                    leftValue = NES::Util::as<NES::FieldAccessExpressionNode>(lhsField)->getFieldName();
                }

                if (NES::Util::instanceOf<ConstantValueExpressionNode>(rhsField))
                {
                    auto constantValue = NES::Util::as<ConstantValueExpressionNode>(rhsField)->getConstantValue();
                    auto basicValueType = std::dynamic_pointer_cast<BasicValue>(constantValue);
                    rightValue = basicValueType->value;
                }
                else
                {
                    rightValue = NES::Util::as<NES::FieldAccessExpressionNode>(rhsField)->getFieldName();
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
            else if (!(NES::Util::instanceOf<FieldAccessExpressionNode>(sortedLeft)
                       || NES::Util::instanceOf<ConstantValueExpressionNode>(sortedLeft)))
            {
                bool replaced = replaceCommutativeExpressions(sortedLeft, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }

            if (sortedRight.get() == originalField.get())
            {
                sortedRight = updatedField;
            }
            else if (!(NES::Util::instanceOf<FieldAccessExpressionNode>(sortedRight)
                       || NES::Util::instanceOf<ConstantValueExpressionNode>(sortedRight)))
            {
                bool replaced = replaceCommutativeExpressions(sortedRight, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }
        }

        if (!NES::Util::instanceOf<AddExpressionNode>(sortedLeft) || !NES::Util::instanceOf<AddExpressionNode>(sortedRight))
        {
            auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
            auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
            int compared = leftSortedFieldName.compare(rightSortedFieldName);
            if (compared > 0)
            {
                return AddExpressionNode::create(sortedRight, sortedLeft);
            }
        }

        return AddExpressionNode::create(sortedLeft, sortedRight);
    }
    if (NES::Util::instanceOf<SubExpressionNode>(expression))
    {
        auto subExpressionNode = NES::Util::as<SubExpressionNode>(expression);
        auto left = subExpressionNode->getLeft();
        auto right = subExpressionNode->getRight();
        sortAttributesInExpression(left);
        sortAttributesInExpression(right);
        return expression;
    }
    else if (NES::Util::instanceOf<MulExpressionNode>(expression))
    {
        auto mulExpressionNode = NES::Util::as<MulExpressionNode>(expression);
        auto left = mulExpressionNode->getLeft();
        auto right = mulExpressionNode->getRight();

        auto sortedLeft = sortAttributesInExpression(left);
        auto sortedRight = sortAttributesInExpression(right);

        auto leftCommutativeFields = fetchCommutativeFields<MulExpressionNode>(sortedLeft);
        auto rightCommutativeFields = fetchCommutativeFields<MulExpressionNode>(sortedRight);

        std::vector<ExpressionNodePtr> allCommutativeFields;
        allCommutativeFields.insert(allCommutativeFields.end(), leftCommutativeFields.begin(), leftCommutativeFields.end());
        allCommutativeFields.insert(allCommutativeFields.end(), rightCommutativeFields.begin(), rightCommutativeFields.end());

        std::vector<ExpressionNodePtr> sortedCommutativeFields;
        sortedCommutativeFields.reserve(allCommutativeFields.size());
        for (const auto& commutativeField : allCommutativeFields)
        {
            sortedCommutativeFields.push_back(commutativeField->copy());
        }

        std::sort(
            sortedCommutativeFields.begin(),
            sortedCommutativeFields.end(),
            [](const ExpressionNodePtr& lhsField, const ExpressionNodePtr& rhsField)
            {
                std::string leftValue;
                std::string rightValue;

                if (NES::Util::instanceOf<ConstantValueExpressionNode>(lhsField))
                {
                    auto constantValue = NES::Util::as<ConstantValueExpressionNode>(lhsField)->getConstantValue();
                    auto basicValueType = std::dynamic_pointer_cast<BasicValue>(constantValue);
                    leftValue = basicValueType->value;
                }
                else
                {
                    leftValue = NES::Util::as<FieldAccessExpressionNode>(lhsField)->getFieldName();
                }

                if (NES::Util::instanceOf<ConstantValueExpressionNode>(rhsField))
                {
                    auto constantValue = NES::Util::as<ConstantValueExpressionNode>(rhsField)->getConstantValue();
                    auto basicValueType = std::dynamic_pointer_cast<BasicValue>(constantValue);
                    rightValue = basicValueType->value;
                }
                else
                {
                    rightValue = NES::Util::as<FieldAccessExpressionNode>(rhsField)->getFieldName();
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
            else if (!(NES::Util::instanceOf<FieldAccessExpressionNode>(sortedLeft)
                       || NES::Util::instanceOf<ConstantValueExpressionNode>(sortedLeft)))
            {
                bool replaced = replaceCommutativeExpressions(sortedLeft, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }

            if (sortedRight.get() == originalField.get())
            {
                sortedRight = updatedField;
            }
            else if (!(NES::Util::instanceOf<FieldAccessExpressionNode>(sortedRight)
                       || NES::Util::instanceOf<ConstantValueExpressionNode>(sortedRight)))
            {
                bool replaced = replaceCommutativeExpressions(sortedRight, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }
        }

        if (!NES::Util::instanceOf<MulExpressionNode>(sortedLeft) || !NES::Util::instanceOf<MulExpressionNode>(sortedRight))
        {
            auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
            auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
            int compared = leftSortedFieldName.compare(rightSortedFieldName);
            if (compared > 0)
            {
                return MulExpressionNode::create(sortedRight, sortedLeft);
            }
        }

        return MulExpressionNode::create(sortedLeft, sortedRight);
    }
    else if (NES::Util::instanceOf<DivExpressionNode>(expression))
    {
        auto divExpressionNode = NES::Util::as<DivExpressionNode>(expression);
        auto left = divExpressionNode->getLeft();
        auto right = divExpressionNode->getRight();
        sortAttributesInExpression(left);
        sortAttributesInExpression(right);
        return expression;
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression implemented for the arithmetical expression node: " + expression->toString());
    return nullptr;
}

ExpressionNodePtr AttributeSortRule::sortAttributesInLogicalExpressions(const ExpressionNodePtr& expression)
{
    NES_DEBUG("Create Z3 expression node for logical expression {}", expression->toString());
    if (NES::Util::instanceOf<AndExpressionNode>(expression))
    {
        auto andExpressionNode = NES::Util::as<AndExpressionNode>(expression);
        auto left = andExpressionNode->getLeft();
        auto right = andExpressionNode->getRight();
        auto sortedLeft = sortAttributesInExpression(left);
        auto sortedRight = sortAttributesInExpression(right);

        auto leftCommutativeFields = fetchCommutativeFields<AndExpressionNode>(sortedLeft);
        auto rightCommutativeFields = fetchCommutativeFields<AndExpressionNode>(sortedRight);

        std::vector<ExpressionNodePtr> allCommutativeFields;
        allCommutativeFields.insert(allCommutativeFields.end(), leftCommutativeFields.begin(), leftCommutativeFields.end());
        allCommutativeFields.insert(allCommutativeFields.end(), rightCommutativeFields.begin(), rightCommutativeFields.end());

        std::vector<ExpressionNodePtr> sortedCommutativeFields;
        sortedCommutativeFields.reserve(allCommutativeFields.size());
        for (const auto& commutativeField : allCommutativeFields)
        {
            sortedCommutativeFields.push_back(commutativeField->copy());
        }

        std::sort(
            sortedCommutativeFields.begin(),
            sortedCommutativeFields.end(),
            [](const ExpressionNodePtr& lhsField, const ExpressionNodePtr& rhsField)
            {
                std::string leftValue;
                std::string rightValue;

                if (NES::Util::instanceOf<ConstantValueExpressionNode>(lhsField))
                {
                    auto constantValue = NES::Util::as<ConstantValueExpressionNode>(lhsField)->getConstantValue();
                    auto basicValueType = std::dynamic_pointer_cast<BasicValue>(constantValue);
                    leftValue = basicValueType->value;
                }
                else
                {
                    leftValue = NES::Util::as<FieldAccessExpressionNode>(lhsField)->getFieldName();
                }

                if (NES::Util::instanceOf<ConstantValueExpressionNode>(rhsField))
                {
                    auto constantValue = NES::Util::as<ConstantValueExpressionNode>(rhsField)->getConstantValue();
                    auto basicValueType = std::dynamic_pointer_cast<BasicValue>(constantValue);
                    rightValue = basicValueType->value;
                }
                else
                {
                    rightValue = NES::Util::as<FieldAccessExpressionNode>(rhsField)->getFieldName();
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
            else if (!(NES::Util::instanceOf<FieldAccessExpressionNode>(sortedLeft)
                       || NES::Util::instanceOf<ConstantValueExpressionNode>(sortedLeft)))
            {
                bool replaced = replaceCommutativeExpressions(sortedLeft, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }

            if (sortedRight.get() == originalField.get())
            {
                sortedRight = updatedField;
            }
            else if (!(NES::Util::instanceOf<FieldAccessExpressionNode>(sortedRight)
                       || NES::Util::instanceOf<ConstantValueExpressionNode>(sortedRight)))
            {
                bool replaced = replaceCommutativeExpressions(sortedRight, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }
        }

        if (!NES::Util::instanceOf<AndExpressionNode>(sortedLeft) || !NES::Util::instanceOf<AndExpressionNode>(sortedRight))
        {
            auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
            auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
            int compared = leftSortedFieldName.compare(rightSortedFieldName);
            if (compared > 0)
            {
                return AndExpressionNode::create(sortedRight, sortedLeft);
            }
        }
        return AndExpressionNode::create(sortedLeft, sortedRight);
    }
    if (NES::Util::instanceOf<OrExpressionNode>(expression))
    {
        auto orExpressionNode = NES::Util::as<OrExpressionNode>(expression);
        auto left = orExpressionNode->getLeft();
        auto right = orExpressionNode->getRight();
        auto sortedLeft = sortAttributesInExpression(left);
        auto sortedRight = sortAttributesInExpression(right);

        auto leftCommutativeFields = fetchCommutativeFields<OrExpressionNode>(sortedLeft);
        auto rightCommutativeFields = fetchCommutativeFields<OrExpressionNode>(sortedRight);

        std::vector<ExpressionNodePtr> allCommutativeFields;
        allCommutativeFields.insert(allCommutativeFields.end(), leftCommutativeFields.begin(), leftCommutativeFields.end());
        allCommutativeFields.insert(allCommutativeFields.end(), rightCommutativeFields.begin(), rightCommutativeFields.end());

        std::vector<ExpressionNodePtr> sortedCommutativeFields;
        sortedCommutativeFields.reserve(allCommutativeFields.size());
        for (const auto& commutativeField : allCommutativeFields)
        {
            sortedCommutativeFields.push_back(commutativeField->copy());
        }

        std::sort(
            sortedCommutativeFields.begin(),
            sortedCommutativeFields.end(),
            [](const ExpressionNodePtr& lhsField, const ExpressionNodePtr& rhsField)
            {
                std::string leftValue;
                std::string rightValue;

                if (NES::Util::instanceOf<ConstantValueExpressionNode>(lhsField))
                {
                    auto constantValue = NES::Util::as<ConstantValueExpressionNode>(lhsField)->getConstantValue();
                    auto basicValueType = std::dynamic_pointer_cast<BasicValue>(constantValue);
                    leftValue = basicValueType->value;
                }
                else
                {
                    leftValue = NES::Util::as<FieldAccessExpressionNode>(lhsField)->getFieldName();
                }

                if (NES::Util::instanceOf<ConstantValueExpressionNode>(rhsField))
                {
                    auto constantValue = NES::Util::as<ConstantValueExpressionNode>(rhsField)->getConstantValue();
                    auto basicValueType = std::dynamic_pointer_cast<BasicValue>(constantValue);
                    rightValue = basicValueType->value;
                }
                else
                {
                    rightValue = NES::Util::as<FieldAccessExpressionNode>(rhsField)->getFieldName();
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
            else if (!(NES::Util::instanceOf<FieldAccessExpressionNode>(sortedLeft)
                       || NES::Util::instanceOf<ConstantValueExpressionNode>(sortedLeft)))
            {
                bool replaced = replaceCommutativeExpressions(sortedLeft, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }

            if (sortedRight.get() == originalField.get())
            {
                sortedRight = updatedField;
            }
            else if (!(NES::Util::instanceOf<FieldAccessExpressionNode>(sortedRight)
                       || NES::Util::instanceOf<ConstantValueExpressionNode>(sortedRight)))
            {
                bool replaced = replaceCommutativeExpressions(sortedRight, originalField, updatedField);
                if (replaced)
                {
                    continue;
                }
            }
        }

        if (!NES::Util::instanceOf<OrExpressionNode>(sortedLeft) || !NES::Util::instanceOf<OrExpressionNode>(sortedRight))
        {
            auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
            auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
            int compared = leftSortedFieldName.compare(rightSortedFieldName);
            if (compared > 0)
            {
                return OrExpressionNode::create(sortedRight, sortedLeft);
            }
        }
        return OrExpressionNode::create(sortedLeft, sortedRight);
    }
    else if (NES::Util::instanceOf<LessExpressionNode>(expression))
    {
        auto lessExpressionNode = NES::Util::as<LessExpressionNode>(expression);
        auto left = lessExpressionNode->getLeft();
        auto right = lessExpressionNode->getRight();

        auto sortedLeft = sortAttributesInExpression(left);
        auto sortedRight = sortAttributesInExpression(right);

        auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
        auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
        int compared = leftSortedFieldName.compare(rightSortedFieldName);
        if (compared > 0)
        {
            return GreaterExpressionNode::create(sortedRight, sortedLeft);
        }
        return LessExpressionNode::create(sortedLeft, sortedRight);
    }
    else if (NES::Util::instanceOf<LessEqualsExpressionNode>(expression))
    {
        auto lessEqualsExpressionNode = NES::Util::as<LessEqualsExpressionNode>(expression);
        auto left = lessEqualsExpressionNode->getLeft();
        auto right = lessEqualsExpressionNode->getRight();
        auto sortedLeft = sortAttributesInExpression(left);
        auto sortedRight = sortAttributesInExpression(right);

        auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
        auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
        int compared = leftSortedFieldName.compare(rightSortedFieldName);
        if (compared > 0)
        {
            return GreaterEqualsExpressionNode::create(sortedRight, sortedLeft);
        }
        return LessEqualsExpressionNode::create(sortedLeft, sortedRight);
    }
    else if (NES::Util::instanceOf<GreaterExpressionNode>(expression))
    {
        auto greaterExpressionNode = NES::Util::as<GreaterExpressionNode>(expression);
        auto left = greaterExpressionNode->getLeft();
        auto right = greaterExpressionNode->getRight();

        auto sortedLeft = sortAttributesInExpression(left);
        auto sortedRight = sortAttributesInExpression(right);

        auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
        auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
        int compared = leftSortedFieldName.compare(rightSortedFieldName);
        if (compared > 0)
        {
            return LessExpressionNode::create(sortedRight, sortedLeft);
        }
        return GreaterExpressionNode::create(sortedLeft, sortedRight);
    }
    else if (NES::Util::instanceOf<GreaterEqualsExpressionNode>(expression))
    {
        auto greaterEqualsExpressionNode = NES::Util::as<GreaterEqualsExpressionNode>(expression);
        auto left = greaterEqualsExpressionNode->getLeft();
        auto right = greaterEqualsExpressionNode->getRight();

        auto sortedLeft = sortAttributesInExpression(left);
        auto sortedRight = sortAttributesInExpression(right);

        auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
        auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
        int compared = leftSortedFieldName.compare(rightSortedFieldName);
        if (compared > 0)
        {
            return LessEqualsExpressionNode::create(sortedRight, sortedLeft);
        }
        return GreaterEqualsExpressionNode::create(sortedLeft, sortedRight);
    }
    else if (NES::Util::instanceOf<EqualsExpressionNode>(expression))
    {
        auto equalsExpressionNode = NES::Util::as<EqualsExpressionNode>(expression);
        auto left = equalsExpressionNode->getLeft();
        auto right = equalsExpressionNode->getRight();
        auto sortedLeft = sortAttributesInExpression(left);
        auto sortedRight = sortAttributesInExpression(right);

        auto leftSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedLeft);
        auto rightSortedFieldName = fetchLeftMostConstantValueOrFieldName(sortedRight);
        int compared = leftSortedFieldName.compare(rightSortedFieldName);
        if (compared > 0)
        {
            return EqualsExpressionNode::create(sortedRight, sortedLeft);
        }
        return EqualsExpressionNode::create(sortedLeft, sortedRight);
    }
    else if (NES::Util::instanceOf<NegateExpressionNode>(expression))
    {
        auto negateExpressionNode = NES::Util::as<NegateExpressionNode>(expression);
        auto childExpression = negateExpressionNode->child();
        auto updatedChildExpression = sortAttributesInExpression(childExpression);
        return NegateExpressionNode::create(updatedChildExpression);
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression possible for the logical expression node: " + expression->toString());
    return nullptr;
}

bool AttributeSortRule::replaceCommutativeExpressions(
    const ExpressionNodePtr& parentExpression, const ExpressionNodePtr& originalExpression, const ExpressionNodePtr& updatedExpression)
{
    auto binaryExpression = NES::Util::as<BinaryExpressionNode>(parentExpression);

    const ExpressionNodePtr& leftChild = binaryExpression->getLeft();
    const ExpressionNodePtr& rightChild = binaryExpression->getRight();
    if (leftChild.get() == originalExpression.get())
    {
        binaryExpression->removeChildren();
        binaryExpression->setChildren(updatedExpression, rightChild);
        return true;
    }
    if (rightChild.get() == originalExpression.get())
    {
        binaryExpression->removeChildren();
        binaryExpression->setChildren(leftChild, updatedExpression);
        return true;
    }
    else
    {
        auto children = parentExpression->getChildren();
        for (const auto& child : children)
        {
            if (!(NES::Util::instanceOf<FieldAccessExpressionNode>(child) || NES::Util::instanceOf<ConstantValueExpressionNode>(child)))
            {
                bool replaced = replaceCommutativeExpressions(NES::Util::as<ExpressionNode>(child), originalExpression, updatedExpression);
                if (replaced)
                {
                    return true;
                }
            }
        }
    }
    return false;
}

std::string AttributeSortRule::fetchLeftMostConstantValueOrFieldName(ExpressionNodePtr expression)
{
    ExpressionNodePtr startPoint = std::move(expression);
    while (
        !(NES::Util::instanceOf<FieldAccessExpressionNode>(startPoint) || NES::Util::instanceOf<ConstantValueExpressionNode>(startPoint)))
    {
        startPoint = NES::Util::as<ExpressionNode>(startPoint->getChildren()[0]);
    }

    if (NES::Util::instanceOf<FieldAccessExpressionNode>(startPoint))
    {
        return NES::Util::as<FieldAccessExpressionNode>(startPoint)->getFieldName();
    }
    const ValueTypePtr& constantValue = NES::Util::as<ConstantValueExpressionNode>(startPoint)->getConstantValue();
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
