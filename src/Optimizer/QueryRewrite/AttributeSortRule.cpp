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
#include <Nodes/Expressions/LogicalExpressions/LogicalExpressionNode.hpp>
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
        return createForLogicalExpressions(expression);
    } else if (expression->instanceOf<ArithmeticalExpressionNode>()) {
        return createForArithmeticalExpressions(expression);
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
        auto left = createForExpression(addExpressionNode->getLeft(), context);
        auto right = createForExpression(addExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFieldMap = left->getFieldMap();
        leftFieldMap.merge(right->getFieldMap());

        Z3_ast array[] = {*left->getExpr(), *right->getExpr()};
        auto expr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_add(*context, 2, array)));
        return Z3ExprAndFieldMap::create(expr, leftFieldMap);
    } else if (expression->instanceOf<SubExpressionNode>()) {
        auto subExpressionNode = expression->as<SubExpressionNode>();
        auto left = createForExpression(subExpressionNode->getLeft(), context);
        auto right = createForExpression(subExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFieldMap = left->getFieldMap();
        leftFieldMap.merge(right->getFieldMap());


    } else if (expression->instanceOf<MulExpressionNode>()) {
        auto mulExpressionNode = expression->as<MulExpressionNode>();
        auto left = createForExpression(mulExpressionNode->getLeft(), context);
        auto right = createForExpression(mulExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFieldMap = left->getFieldMap();
        leftFieldMap.merge(right->getFieldMap());


    } else if (expression->instanceOf<DivExpressionNode>()) {
        auto divExpressionNode = expression->as<DivExpressionNode>();
        auto left = createForExpression(divExpressionNode->getLeft(), context);
        auto right = createForExpression(divExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFieldMap = left->getFieldMap();
        leftFieldMap.merge(right->getFieldMap());


    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression implemented for the arithmetical expression node: "
                            + expression->toString());
}

void AttributeSortRule::sortAttributesInLogicalExpressions(ExpressionNodePtr expression) {
    NES_DEBUG("Create Z3 expression node for logical expression " << expression->toString());
    if (expression->instanceOf<AndExpressionNode>()) {
        auto andExpressionNode = expression->as<AndExpressionNode>();
        auto left = createForExpression(andExpressionNode->getLeft(), context);
        auto right = createForExpression(andExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFieldMap = left->getFieldMap();
        leftFieldMap.merge(right->getFieldMap());

        Z3_ast array[] = {*left->getExpr(), *right->getExpr()};
        auto expr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_and(*context, 2, array)));
        return Z3ExprAndFieldMap::create(expr, leftFieldMap);
    } else if (expression->instanceOf<OrExpressionNode>()) {
        auto orExpressionNode = expression->as<OrExpressionNode>();
        auto left = createForExpression(orExpressionNode->getLeft(), context);
        auto right = createForExpression(orExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFieldMap = left->getFieldMap();
        leftFieldMap.merge(right->getFieldMap());

        Z3_ast array[] = {*left->getExpr(), *right->getExpr()};
        auto expr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_or(*context, 2, array)));
        return Z3ExprAndFieldMap::create(expr, leftFieldMap);
    } else if (expression->instanceOf<LessExpressionNode>()) {
        auto lessExpressionNode = expression->as<LessExpressionNode>();
        auto left = createForExpression(lessExpressionNode->getLeft(), context);
        auto right = createForExpression(lessExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFieldMap = left->getFieldMap();
        leftFieldMap.merge(right->getFieldMap());

        auto expr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_lt(*context, *left->getExpr(), *right->getExpr())));
        return Z3ExprAndFieldMap::create(expr, leftFieldMap);
    } else if (expression->instanceOf<LessEqualsExpressionNode>()) {
        auto lessEqualsExpressionNode = expression->as<LessEqualsExpressionNode>();
        auto left = createForExpression(lessEqualsExpressionNode->getLeft(), context);
        auto right = createForExpression(lessEqualsExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFieldMap = left->getFieldMap();
        leftFieldMap.merge(right->getFieldMap());

        auto expr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_le(*context, *left->getExpr(), *right->getExpr())));
        return Z3ExprAndFieldMap::create(expr, leftFieldMap);
    } else if (expression->instanceOf<GreaterExpressionNode>()) {
        auto greaterExpressionNode = expression->as<GreaterExpressionNode>();
        auto left = createForExpression(greaterExpressionNode->getLeft(), context);
        auto right = createForExpression(greaterExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFieldMap = left->getFieldMap();
        leftFieldMap.merge(right->getFieldMap());

        auto expr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_gt(*context, *left->getExpr(), *right->getExpr())));
        return Z3ExprAndFieldMap::create(expr, leftFieldMap);
    } else if (expression->instanceOf<GreaterEqualsExpressionNode>()) {
        auto greaterEqualsExpressionNode = expression->as<GreaterEqualsExpressionNode>();
        auto left = createForExpression(greaterEqualsExpressionNode->getLeft(), context);
        auto right = createForExpression(greaterEqualsExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFieldMap = left->getFieldMap();
        leftFieldMap.merge(right->getFieldMap());

        auto expr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_ge(*context, *left->getExpr(), *right->getExpr())));
        return Z3ExprAndFieldMap::create(expr, leftFieldMap);
    } else if (expression->instanceOf<EqualsExpressionNode>()) {
        auto equalsExpressionNode = expression->as<EqualsExpressionNode>();
        auto left = createForExpression(equalsExpressionNode->getLeft(), context);
        auto right = createForExpression(equalsExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFiledMap = left->getFieldMap();
        leftFiledMap.merge(right->getFieldMap());

        auto expr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_eq(*context, *left->getExpr(), *right->getExpr())));
        return Z3ExprAndFieldMap::create(expr, leftFiledMap);
    } else if (expression->instanceOf<NegateExpressionNode>()) {
        auto equalsExpressionNode = expression->as<NegateExpressionNode>();
        auto expr = createForExpression(equalsExpressionNode->child(), context);
        auto updatedExpr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_not(*context, *expr->getExpr())));
        return Z3ExprAndFieldMap::create(updatedExpr, expr->getFieldMap());
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression possible for the logical expression node: " + expression->toString());
}

}// namespace NES
