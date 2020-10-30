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
#include <Optimizer/Utils/DataTypeToFOL.hpp>
#include <Optimizer/Utils/ExpressionToFOLUtil.hpp>
#include <z3++.h>

namespace NES {

z3::expr ExpressionToFOLUtil::serializeExpression(ExpressionNodePtr expression, z3::context& context) {

    NES_DEBUG("ExpressionSerializationUtil:: serialize expression " << expression->toString());
    // serialize expression node depending on its type.
    if (expression->instanceOf<LogicalExpressionNode>()) {
        // serialize logical expression
        return serializeLogicalExpressions(expression, context);
    } else if (expression->instanceOf<ArithmeticalExpressionNode>()) {
        // serialize arithmetical expressions
        return serializeArithmeticalExpressions(expression, context);
    } else if (expression->instanceOf<ConstantValueExpressionNode>()) {
        // serialize constant value expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize constant value expression node.");
        auto constantValueExpression = expression->as<ConstantValueExpressionNode>();
        auto value = constantValueExpression->getConstantValue();
        // serialize value
        //        auto serializedConstantValue = SerializableExpression_ConstantValueExpression();
        //        DataTypeSerializationUtil::serializeDataValue(value, serializedConstantValue.mutable_value());
        //        serializedExpression->mutable_details()->PackFrom(serializedConstantValue);
        return DataTypeToFOL::serializeDataValue(value, context);
    } else if (expression->instanceOf<FieldAccessExpressionNode>()) {
        // serialize field access expression node
        NES_TRACE("ExpressionSerializationUtil:: serialize field access expression node.");
        auto fieldAccessExpression = expression->as<FieldAccessExpressionNode>();
        //        auto serializedFieldAccessExpression = SerializableExpression_FieldAccessExpression();
        //        serializedFieldAccessExpression.set_fieldname(fieldAccessExpression->getFieldName());
        //        serializedExpression->mutable_details()->PackFrom(serializedFieldAccessExpression);
    } else if (expression->instanceOf<FieldAssignmentExpressionNode>()) {
        // serialize field assignment expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize field assignment expression node.");
        auto fieldAssignmentExpressionNode = expression->as<FieldAssignmentExpressionNode>();
        std::string fieldName = fieldAssignmentExpressionNode->getField()->getFieldName();
        DataTypePtr fieldType = fieldAssignmentExpressionNode->getField()->getStamp();
        auto filedExpr = DataTypeToFOL::serializeDataType(fieldName, fieldType, context);
        auto valueExpr = serializeExpression(fieldAssignmentExpressionNode->getAssignment(), context);
        return to_expr(context, Z3_mk_eq(context, filedExpr, valueExpr));
    }

    NES_THROW_RUNTIME_ERROR("ExpressionSerializationUtil: could not serialize this expression: " + expression->toString());
    //    DataTypeToFOL::serializeDataType(expression->getStamp());
    //    NES_DEBUG("ExpressionSerializationUtil:: serialize expression node to " << serializedExpression->mutable_details()->type_url());
}

z3::expr ExpressionToFOLUtil::serializeArithmeticalExpressions(ExpressionNodePtr expression, z3::context& context) {
    NES_DEBUG("ExpressionSerializationUtil:: serialize arithmetical expression " << expression->toString());
    if (expression->instanceOf<AddExpressionNode>()) {
        // serialize add expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize ADD arithmetical expression to SerializableExpression_AddExpression");
        auto addExpressionNode = expression->as<AddExpressionNode>();
        auto left = serializeExpression(addExpressionNode->getLeft(), context);
        auto right = serializeExpression(addExpressionNode->getRight(), context);
        Z3_ast array[] = {left, right};
        return to_expr(context, Z3_mk_add(context, 2, array));
    } else if (expression->instanceOf<SubExpressionNode>()) {
        // serialize sub expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize SUB arithmetical expression to SerializableExpression_SubExpression");
        auto subExpressionNode = expression->as<SubExpressionNode>();
        auto left = serializeExpression(subExpressionNode->getLeft(), context);
        auto right = serializeExpression(subExpressionNode->getRight(), context);
        Z3_ast array[] = {left, right};
        return to_expr(context, Z3_mk_sub(context, 2, array));
    } else if (expression->instanceOf<MulExpressionNode>()) {
        // serialize mul expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize MUL arithmetical expression to SerializableExpression_MulExpression");
        auto mulExpressionNode = expression->as<MulExpressionNode>();
        auto left = serializeExpression(mulExpressionNode->getLeft(), context);
        auto right = serializeExpression(mulExpressionNode->getRight(), context);
        Z3_ast array[] = {left, right};
        return to_expr(context, Z3_mk_mul(context, 2, array));
    } else if (expression->instanceOf<DivExpressionNode>()) {
        // serialize div expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize DIV arithmetical expression to SerializableExpression_DivExpression");
        auto divExpressionNode = expression->as<DivExpressionNode>();
        auto left = serializeExpression(divExpressionNode->getLeft(), context);
        auto right = serializeExpression(divExpressionNode->getRight(), context);
        return to_expr(context, Z3_mk_div(context, left, right));
    } else {
        NES_FATAL_ERROR("TranslateToLegacyPhase: No serialization implemented for this arithmetical expression node: " << expression->toString());
        NES_NOT_IMPLEMENTED();
    }
}

z3::expr ExpressionToFOLUtil::serializeLogicalExpressions(ExpressionNodePtr expression, z3::context& context) {
    NES_DEBUG("ExpressionSerializationUtil:: serialize logical expression " << expression->toString());
    if (expression->instanceOf<AndExpressionNode>()) {
        NES_TRACE("ExpressionSerializationUtil:: serialize AND logical expression to SerializableExpression_AndExpression");
        auto andExpressionNode = expression->as<AndExpressionNode>();
        auto left = serializeExpression(andExpressionNode->getLeft(), context);
        auto right = serializeExpression(andExpressionNode->getRight(), context);
        Z3_ast array[] = {left, right};
        return to_expr(context, Z3_mk_and(context, 2, array));
    } else if (expression->instanceOf<OrExpressionNode>()) {
        NES_TRACE("ExpressionSerializationUtil:: serialize OR logical expression to SerializableExpression_OrExpression");
        auto orExpressionNode = expression->as<OrExpressionNode>();
        auto left = serializeExpression(orExpressionNode->getLeft(), context);
        auto right = serializeExpression(orExpressionNode->getRight(), context);
        Z3_ast array[] = {left, right};
        return to_expr(context, Z3_mk_or(context, 2, array));
    } else if (expression->instanceOf<LessExpressionNode>()) {
        // serialize less expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize Less logical expression to SerializableExpression_LessExpression");
        auto lessExpressionNode = expression->as<LessExpressionNode>();
        auto left = serializeExpression(lessExpressionNode->getLeft(), context);
        auto right = serializeExpression(lessExpressionNode->getRight(), context);
        return to_expr(context, Z3_mk_lt(context, left, right));
    } else if (expression->instanceOf<LessEqualsExpressionNode>()) {
        // serialize less equals expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize Less Equals logical expression to SerializableExpression_LessEqualsExpression");
        auto lessEqualsExpressionNode = expression->as<LessEqualsExpressionNode>();
        auto left = serializeExpression(lessEqualsExpressionNode->getLeft(), context);
        auto right = serializeExpression(lessEqualsExpressionNode->getRight(), context);
        return to_expr(context, Z3_mk_le(context, left, right));
    } else if (expression->instanceOf<GreaterExpressionNode>()) {
        // serialize greater expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize Greater logical expression to SerializableExpression_GreaterExpression");
        auto greaterExpressionNode = expression->as<GreaterExpressionNode>();
        auto left = serializeExpression(greaterExpressionNode->getLeft(), context);
        auto right = serializeExpression(greaterExpressionNode->getRight(), context);
        return to_expr(context, Z3_mk_gt(context, left, right));
    } else if (expression->instanceOf<GreaterEqualsExpressionNode>()) {
        // serialize greater equals expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize Greater Equals logical expression to SerializableExpression_GreaterEqualsExpression");
        auto greaterEqualsExpressionNode = expression->as<GreaterEqualsExpressionNode>();
        auto left = serializeExpression(greaterEqualsExpressionNode->getLeft(), context);
        auto right = serializeExpression(greaterEqualsExpressionNode->getRight(), context);
        return to_expr(context, Z3_mk_ge(context, left, right));
    } else if (expression->instanceOf<EqualsExpressionNode>()) {
        // serialize equals expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize Equals logical expression to SerializableExpression_EqualsExpression");
        auto equalsExpressionNode = expression->as<EqualsExpressionNode>();
        auto left = serializeExpression(equalsExpressionNode->getLeft(), context);
        auto right = serializeExpression(equalsExpressionNode->getRight(), context);
        return to_expr(context, Z3_mk_eq(context, left, right));
    } else if (expression->instanceOf<NegateExpressionNode>()) {
        // serialize negate expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize negate logical expression to SerializableExpression_NegateExpression");
        auto equalsExpressionNode = expression->as<NegateExpressionNode>();
        auto expr = serializeExpression(equalsExpressionNode->child(), context);
        return to_expr(context, Z3_mk_not(context, expr));
    } else {
        NES_FATAL_ERROR("ExpressionSerializationUtil: No serialization implemented for this logical expression node: " << expression->toString());
        NES_NOT_IMPLEMENTED();
    }
}

}// namespace NES
