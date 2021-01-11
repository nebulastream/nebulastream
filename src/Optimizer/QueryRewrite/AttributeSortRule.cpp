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
        sortAttributesInOperator(filter);
    }

    auto mapOperators = queryPlan->getOperatorByType<MapLogicalOperatorNode>();
    for (auto& map : mapOperators) {
        sortAttributesInOperator(map);
    }

    auto projectOperators = queryPlan->getOperatorByType<ProjectionLogicalOperatorNode>();
    for (auto& project : projectOperators) {
        sortAttributesInOperator(project);
    }
    return queryPlan;
}

void AttributeSortRule::sortAttributesInOperator(OperatorNodePtr logicalOperator) {
    if (logicalOperator->instanceOf<FilterLogicalOperatorNode>()) {
        auto filter = logicalOperator->as<FilterLogicalOperatorNode>();
        auto predicate = filter->getPredicate();
        sortAttributesInExpressions(predicate);
    }
}

void AttributeSortRule::sortAttributesInExpressions(ExpressionNodePtr expression) {
    NES_DEBUG("Creating Z3 expression for input expression " << expression->toString());
    if (expression->instanceOf<LogicalExpressionNode>()) {
    } else if (expression->instanceOf<ArithmeticalExpressionNode>()) {
    } else if (expression->instanceOf<ConstantValueExpressionNode>()) {
        auto constantValueExpression = expression->as<ConstantValueExpressionNode>();
        auto value = constantValueExpression->getConstantValue();

    } else if (expression->instanceOf<FieldAssignmentExpressionNode>()) {
        auto fieldAccessExpression = expression->as<FieldAccessExpressionNode>();
        std::string fieldName = fieldAccessExpression->getFieldName();
        DataTypePtr fieldType = fieldAccessExpression->getStamp();

    } else if (expression->instanceOf<FieldAssignmentExpressionNode>()) {
        auto fieldAssignmentExpressionNode = expression->as<FieldAssignmentExpressionNode>();
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression implemented for the expression: " + expression->toString());
}

void AttributeSortRule::sortAttributesInArithmeticalExpressions(ExpressionNodePtr expression) {
    NES_DEBUG("Create Z3 expression for arithmetical expression " << expression->toString());
    if (expression->instanceOf<AddExpressionNode>()) {
        auto addExpressionNode = expression->as<AddExpressionNode>();
        auto left = addExpressionNode->getLeft();
        auto right = addExpressionNode->getRight();

    } else if (expression->instanceOf<SubExpressionNode>()) {
        auto subExpressionNode = expression->as<SubExpressionNode>();
        auto left = subExpressionNode->getLeft();
        auto right = subExpressionNode->getRight();

    } else if (expression->instanceOf<MulExpressionNode>()) {
        auto mulExpressionNode = expression->as<MulExpressionNode>();
        auto left = mulExpressionNode->getLeft();
        auto right = mulExpressionNode->getRight();

    } else if (expression->instanceOf<DivExpressionNode>()) {
        auto divExpressionNode = expression->as<DivExpressionNode>();
        auto left = divExpressionNode->getLeft();
        auto right = divExpressionNode->getRight();
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

    } else if (expression->instanceOf<OrExpressionNode>()) {
        auto orExpressionNode = expression->as<OrExpressionNode>();
        auto left = orExpressionNode->getLeft();
        auto right = orExpressionNode->getRight();

    } else if (expression->instanceOf<LessExpressionNode>()) {
        auto lessExpressionNode = expression->as<LessExpressionNode>();
        auto left = lessExpressionNode->getLeft();
        auto right = lessExpressionNode->getRight();

    } else if (expression->instanceOf<LessEqualsExpressionNode>()) {
        auto lessEqualsExpressionNode = expression->as<LessEqualsExpressionNode>();
        auto left = lessEqualsExpressionNode->getLeft();
        auto right = lessEqualsExpressionNode->getRight();

    } else if (expression->instanceOf<GreaterExpressionNode>()) {
        auto greaterExpressionNode = expression->as<GreaterExpressionNode>();
        auto left = greaterExpressionNode->getLeft();
        auto right = greaterExpressionNode->getRight();

    } else if (expression->instanceOf<GreaterEqualsExpressionNode>()) {
        auto greaterEqualsExpressionNode = expression->as<GreaterEqualsExpressionNode>();
        auto left = greaterEqualsExpressionNode->getLeft();
        auto right = greaterEqualsExpressionNode->getRight();

    } else if (expression->instanceOf<EqualsExpressionNode>()) {
        auto equalsExpressionNode = expression->as<EqualsExpressionNode>();
        auto left = equalsExpressionNode->getLeft();
        auto right = equalsExpressionNode->getRight();

    } else if (expression->instanceOf<NegateExpressionNode>()) {
        auto negateExpressionNode = expression->as<NegateExpressionNode>();
        auto expr = negateExpressionNode->child();
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression possible for the logical expression node: " + expression->toString());
}

}// namespace NES
