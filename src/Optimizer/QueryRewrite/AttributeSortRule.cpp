/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/ArithmeticalExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LogicalExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Optimizer/QueryRewrite/AttributeSortRule.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES {

AttributeSortRulePtr AttributeSortRule::create() { return std::make_shared<AttributeSortRule>(); }

QueryPlanPtr AttributeSortRule::apply(NES::QueryPlanPtr queryPlan) {

    auto filterOperators = queryPlan->getOperatorByType<FilterLogicalOperatorNode>();
    for (auto& filter : filterOperators) {
        auto predicate = filter->getPredicate();
        sortAttributesInExpressions(predicate);
    }

    auto mapOperators = queryPlan->getOperatorByType<MapLogicalOperatorNode>();
    for (auto& map : mapOperators) {
        auto mapExpression = map->getMapExpression();
        sortAttributesInExpressions(mapExpression);
    }

    return queryPlan;
}

void AttributeSortRule::sortAttributesInExpressions(ExpressionNodePtr expression) {
    NES_DEBUG("Creating Z3 expression for input expression " << expression->toString());
    if (expression->instanceOf<LogicalExpressionNode>()) {
        sortAttributesInLogicalExpressions(expression);
        return;
    } else if (expression->instanceOf<ArithmeticalExpressionNode>()) {
        sortAttributesInArithmeticalExpressions(expression);
        return;
    } else if (expression->instanceOf<ConstantValueExpressionNode>()) {
        auto constantValueExpression = expression->as<ConstantValueExpressionNode>();
        auto value = constantValueExpression->getConstantValue();
        return;
    } else if (expression->instanceOf<FieldAccessExpressionNode>()) {
        auto fieldAccessExpression = expression->as<FieldAccessExpressionNode>();
        std::string fieldName = fieldAccessExpression->getFieldName();
        DataTypePtr fieldType = fieldAccessExpression->getStamp();
        return;
    } else if (expression->instanceOf<FieldAssignmentExpressionNode>()) {
        auto fieldAssignmentExpressionNode = expression->as<FieldAssignmentExpressionNode>();
        sortAttributesInExpressions(fieldAssignmentExpressionNode->getAssignment());
        return;
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression implemented for the expression: " + expression->toString());
}

void AttributeSortRule::sortAttributesInArithmeticalExpressions(ExpressionNodePtr expression) {
    NES_DEBUG("Create Z3 expression for arithmetical expression " << expression->toString());
    if (expression->instanceOf<AddExpressionNode>()) {
        auto addExpressionNode = expression->as<AddExpressionNode>();

        auto left = addExpressionNode->getLeft();
        auto right = addExpressionNode->getRight();

        sortAttributesInExpressions(left);
        sortAttributesInExpressions(right);

        auto leftCommutativeFields = fetchCommutativeFields<AddExpressionNode>(left);
        auto rightCommutativeFields = fetchCommutativeFields<AddExpressionNode>(right);

        std::vector<FieldAccessExpressionNodePtr> allCommutativeFields;
        allCommutativeFields.insert(allCommutativeFields.end(), leftCommutativeFields.begin(), leftCommutativeFields.end());
        allCommutativeFields.insert(allCommutativeFields.end(), rightCommutativeFields.begin(), rightCommutativeFields.end());

        std::vector<FieldAccessExpressionNodePtr> sortedCommutativeFields;
        for (auto& commutativeField : allCommutativeFields) {
            sortedCommutativeFields.push_back(commutativeField->copy()->as<FieldAccessExpressionNode>());
        }

        std::sort(sortedCommutativeFields.begin(), sortedCommutativeFields.end(),
                  [](const FieldAccessExpressionNodePtr& lhsField, const FieldAccessExpressionNodePtr& rhsField) {
                      return lhsField->getFieldName().compare(rhsField->getFieldName()) < 0;
                  });

        for (uint i = 0; i < sortedCommutativeFields.size(); i++) {
            auto& originalField = allCommutativeFields[i];
            auto& updatedField = sortedCommutativeFields[i];
            originalField->setFieldName(updatedField->getFieldName());
            originalField->setStamp(updatedField->getStamp());
            NES_INFO(originalField->toString());
        }
        return;
        //
        //        if (left->instanceOf<FieldAccessExpressionNode>() && right->instanceOf<FieldAccessExpressionNode>()) {
        //            auto leftFieldAccessExpression = left->as<FieldAccessExpressionNode>();
        //            std::string leftFieldName = leftFieldAccessExpression->getFieldName();
        //            auto rightFieldAccessExpression = right->as<FieldAccessExpressionNode>();
        //            std::string rightFieldName = rightFieldAccessExpression->getFieldName();
        //            int compared = leftFieldName.compare(rightFieldName);
        //            if (compared > 0) {
        //                addExpressionNode->removeChildren();
        //                addExpressionNode->setChildren(right, left);
        //            }
        //            return;
        //        }

        //fetch all commutative fields from the expression
        //make copy of all extracted field expressions
        //Sort them by alphabets and assign them to respective pointer refer by index.

        return;
    } else if (expression->instanceOf<SubExpressionNode>()) {
        auto subExpressionNode = expression->as<SubExpressionNode>();
        auto left = subExpressionNode->getLeft();
        auto right = subExpressionNode->getRight();
        sortAttributesInExpressions(left);
        sortAttributesInExpressions(right);
        return;
    } else if (expression->instanceOf<MulExpressionNode>()) {
        auto mulExpressionNode = expression->as<MulExpressionNode>();
        auto left = mulExpressionNode->getLeft();
        auto right = mulExpressionNode->getRight();

        sortAttributesInExpressions(left);
        sortAttributesInExpressions(right);

        auto leftCommutativeFields = fetchCommutativeFields<AddExpressionNode>(left);
        auto rightCommutativeFields = fetchCommutativeFields<AddExpressionNode>(right);

        std::vector<FieldAccessExpressionNodePtr> allCommutativeFields;
        allCommutativeFields.insert(allCommutativeFields.end(), leftCommutativeFields.begin(), leftCommutativeFields.end());
        allCommutativeFields.insert(allCommutativeFields.end(), rightCommutativeFields.begin(), rightCommutativeFields.end());

        std::vector<FieldAccessExpressionNodePtr> sortedCommutativeFields;
        for (auto& commutativeField : allCommutativeFields) {
            sortedCommutativeFields.push_back(commutativeField->copy()->as<FieldAccessExpressionNode>());
        }

        std::sort(sortedCommutativeFields.begin(), sortedCommutativeFields.end(),
                  [](const FieldAccessExpressionNodePtr& lhsField, const FieldAccessExpressionNodePtr& rhsField) {
                    return lhsField->getFieldName().compare(rhsField->getFieldName()) < 0;
                  });

        for (uint i = 0; i < sortedCommutativeFields.size(); i++) {
            auto& originalField = allCommutativeFields[i];
            auto& updatedField = sortedCommutativeFields[i];
            originalField->setFieldName(updatedField->getFieldName());
            originalField->setStamp(updatedField->getStamp());
            NES_INFO(originalField->toString());
        }

        if(!left->instanceOf<MulExpressionNode>() || !right->instanceOf<MulExpressionNode>()){

        }

        return;
    } else if (expression->instanceOf<DivExpressionNode>()) {
        auto divExpressionNode = expression->as<DivExpressionNode>();
        auto left = divExpressionNode->getLeft();
        auto right = divExpressionNode->getRight();
        sortAttributesInExpressions(left);
        sortAttributesInExpressions(right);
        return;
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression implemented for the arithmetical expression node: "
                            + expression->toString());
}

void AttributeSortRule::sortAttributesInLogicalExpressions(ExpressionNodePtr expression) {
    NES_DEBUG("Create Z3 expression node for logical expression " << expression->toString());
    if (expression->instanceOf<AndExpressionNode>()) {
        auto andExpressionNode = expression->as<AndExpressionNode>();
        auto left = andExpressionNode->getLeft();
        auto right = andExpressionNode->getRight();
        return;
    } else if (expression->instanceOf<OrExpressionNode>()) {
        auto orExpressionNode = expression->as<OrExpressionNode>();
        auto left = orExpressionNode->getLeft();
        auto right = orExpressionNode->getRight();
        return;
    } else if (expression->instanceOf<LessExpressionNode>()) {
        auto lessExpressionNode = expression->as<LessExpressionNode>();
        auto left = lessExpressionNode->getLeft();
        auto right = lessExpressionNode->getRight();
        return;
    } else if (expression->instanceOf<LessEqualsExpressionNode>()) {
        auto lessEqualsExpressionNode = expression->as<LessEqualsExpressionNode>();
        auto left = lessEqualsExpressionNode->getLeft();
        auto right = lessEqualsExpressionNode->getRight();
        return;
    } else if (expression->instanceOf<GreaterExpressionNode>()) {
        auto greaterExpressionNode = expression->as<GreaterExpressionNode>();
        auto left = greaterExpressionNode->getLeft();
        auto right = greaterExpressionNode->getRight();
        return;
    } else if (expression->instanceOf<GreaterEqualsExpressionNode>()) {
        auto greaterEqualsExpressionNode = expression->as<GreaterEqualsExpressionNode>();
        auto left = greaterEqualsExpressionNode->getLeft();
        auto right = greaterEqualsExpressionNode->getRight();
        return;
    } else if (expression->instanceOf<EqualsExpressionNode>()) {
        auto equalsExpressionNode = expression->as<EqualsExpressionNode>();
        auto left = equalsExpressionNode->getLeft();
        auto right = equalsExpressionNode->getRight();
        return;
    } else if (expression->instanceOf<NegateExpressionNode>()) {
        auto negateExpressionNode = expression->as<NegateExpressionNode>();
        auto expr = negateExpressionNode->child();
        return;
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression possible for the logical expression node: " + expression->toString());
}

}// namespace NES
