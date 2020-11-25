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
#include <Nodes/Expressions/ExpressionNode.hpp>
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
#include <Optimizer/Utils/DataTypeToZ3ExprUtil.hpp>
#include <Optimizer/Utils/ExpressionToZ3ExprUtil.hpp>
#include <z3++.h>

namespace NES {

z3::ExprPtr ExpressionToZ3ExprUtil::createForExpression(ExpressionNodePtr expression,
                                                        std::map<std::string, std::vector<z3::ExprPtr>> cols,
                                                        z3::context& context) {
    NES_DEBUG("Creating Z3 expression for input expression " << expression->toString());
    if (expression->instanceOf<LogicalExpressionNode>()) {
        return createForLogicalExpressions(expression, cols, context);
    } else if (expression->instanceOf<ArithmeticalExpressionNode>()) {
        return createForArithmeticalExpressions(expression, cols, context);
    } else if (expression->instanceOf<ConstantValueExpressionNode>()) {
        auto constantValueExpression = expression->as<ConstantValueExpressionNode>();
        auto value = constantValueExpression->getConstantValue();
        return DataTypeToZ3ExprUtil::createForDataValue(value, context);
    } else if (expression->instanceOf<FieldAccessExpressionNode>()) {
        auto fieldAccessExpression = expression->as<FieldAccessExpressionNode>();
        std::string fieldName = fieldAccessExpression->getFieldName();
        DataTypePtr fieldType = fieldAccessExpression->getStamp();
        return DataTypeToZ3ExprUtil::createForField(fieldName, fieldType, context);
    } else if (expression->instanceOf<FieldAssignmentExpressionNode>()) {
        auto fieldAssignmentExpressionNode = expression->as<FieldAssignmentExpressionNode>();
        return createForExpression(fieldAssignmentExpressionNode->getAssignment(), cols, context);
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression implemented for the expression: " + expression->toString());
}

z3::ExprPtr ExpressionToZ3ExprUtil::createForArithmeticalExpressions(ExpressionNodePtr expression,
                                                                     std::map<std::string, std::vector<z3::ExprPtr>> cols,
                                                                     z3::context& context) {
    NES_DEBUG("Create Z3 expression for arithmetical expression " << expression->toString());
    if (expression->instanceOf<AddExpressionNode>()) {
        auto addExpressionNode = expression->as<AddExpressionNode>();
        auto left = createForExpression(addExpressionNode->getLeft(), cols, context);
        auto right = createForExpression(addExpressionNode->getRight(), cols, context);
        Z3_ast array[] = {*left, *right};
        return std::make_shared<z3::expr>(to_expr(context, Z3_mk_add(context, 2, array)));
    } else if (expression->instanceOf<SubExpressionNode>()) {
        auto subExpressionNode = expression->as<SubExpressionNode>();
        auto left = createForExpression(subExpressionNode->getLeft(), cols, context);
        auto right = createForExpression(subExpressionNode->getRight(), cols, context);
        Z3_ast array[] = {*left, *right};
        return std::make_shared<z3::expr>(to_expr(context, Z3_mk_sub(context, 2, array)));
    } else if (expression->instanceOf<MulExpressionNode>()) {
        auto mulExpressionNode = expression->as<MulExpressionNode>();
        auto left = createForExpression(mulExpressionNode->getLeft(), cols, context);
        auto right = createForExpression(mulExpressionNode->getRight(), cols, context);
        Z3_ast array[] = {*left, *right};
        return std::make_shared<z3::expr>(to_expr(context, Z3_mk_mul(context, 2, array)));
    } else if (expression->instanceOf<DivExpressionNode>()) {
        auto divExpressionNode = expression->as<DivExpressionNode>();
        auto left = createForExpression(divExpressionNode->getLeft(), cols, context);
        auto right = createForExpression(divExpressionNode->getRight(), cols, context);
        return std::make_shared<z3::expr>(to_expr(context, Z3_mk_div(context, *left, *right)));
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression implemented for the arithmetical expression node: "
                            + expression->toString());
}

z3::ExprPtr ExpressionToZ3ExprUtil::createForLogicalExpressions(ExpressionNodePtr expression,
                                                                std::map<std::string, std::vector<z3::ExprPtr>> cols,
                                                                z3::context& context) {
    NES_DEBUG("Create Z3 expression node for logical expression " << expression->toString());
    if (expression->instanceOf<AndExpressionNode>()) {
        auto andExpressionNode = expression->as<AndExpressionNode>();
        auto left = createForExpression(andExpressionNode->getLeft(), cols, context);
        auto right = createForExpression(andExpressionNode->getRight(), cols, context);
        Z3_ast array[] = {*left, *right};
        return std::make_shared<z3::expr>(to_expr(context, Z3_mk_and(context, 2, array)));
    } else if (expression->instanceOf<OrExpressionNode>()) {
        auto orExpressionNode = expression->as<OrExpressionNode>();
        auto left = createForExpression(orExpressionNode->getLeft(), cols, context);
        auto right = createForExpression(orExpressionNode->getRight(), cols, context);
        Z3_ast array[] = {*left, *right};
        return std::make_shared<z3::expr>(to_expr(context, Z3_mk_or(context, 2, array)));
    } else if (expression->instanceOf<LessExpressionNode>()) {
        auto lessExpressionNode = expression->as<LessExpressionNode>();
        auto left = createForExpression(lessExpressionNode->getLeft(), cols, context);
        auto right = createForExpression(lessExpressionNode->getRight(), cols, context);
        return std::make_shared<z3::expr>(to_expr(context, Z3_mk_lt(context, *left, *right)));
    } else if (expression->instanceOf<LessEqualsExpressionNode>()) {
        auto lessEqualsExpressionNode = expression->as<LessEqualsExpressionNode>();
        auto left = createForExpression(lessEqualsExpressionNode->getLeft(), cols, context);
        auto right = createForExpression(lessEqualsExpressionNode->getRight(), cols, context);
        return std::make_shared<z3::expr>(to_expr(context, Z3_mk_le(context, *left, *right)));
    } else if (expression->instanceOf<GreaterExpressionNode>()) {
        auto greaterExpressionNode = expression->as<GreaterExpressionNode>();
        auto left = createForExpression(greaterExpressionNode->getLeft(), cols, context);
        auto right = createForExpression(greaterExpressionNode->getRight(), cols, context);
        return std::make_shared<z3::expr>(to_expr(context, Z3_mk_gt(context, *left, *right)));
    } else if (expression->instanceOf<GreaterEqualsExpressionNode>()) {
        auto greaterEqualsExpressionNode = expression->as<GreaterEqualsExpressionNode>();
        auto left = createForExpression(greaterEqualsExpressionNode->getLeft(), cols, context);
        auto right = createForExpression(greaterEqualsExpressionNode->getRight(), cols, context);
        return std::make_shared<z3::expr>(to_expr(context, Z3_mk_ge(context, *left, *right)));
    } else if (expression->instanceOf<EqualsExpressionNode>()) {
        auto equalsExpressionNode = expression->as<EqualsExpressionNode>();
        auto left = createForExpression(equalsExpressionNode->getLeft(), cols, context);
        auto right = createForExpression(equalsExpressionNode->getRight(), cols, context);
        return std::make_shared<z3::expr>(to_expr(context, Z3_mk_eq(context, *left, *right)));
    } else if (expression->instanceOf<NegateExpressionNode>()) {
        auto equalsExpressionNode = expression->as<NegateExpressionNode>();
        auto expr = createForExpression(equalsExpressionNode->child(), cols, context);
        return std::make_shared<z3::expr>(to_expr(context, Z3_mk_not(context, *expr)));
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression possible for the logical expression node: " + expression->toString());
}

}// namespace NES
