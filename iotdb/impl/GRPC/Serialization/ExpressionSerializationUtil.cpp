#include <API/UserAPIExpression.hpp>
#include <GRPC/Serialization/DataTypeSerializationUtil.hpp>
#include <GRPC/Serialization/ExpressionSerializationUtil.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
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
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <QueryCompiler/DataTypes/ValueType.hpp>
#include <SerializableOperator.pb.h>
namespace NES {

SerializableExpression* ExpressionSerializationUtil::serializeExpression(ExpressionNodePtr expression, SerializableExpression* serializedExpression) {
    // serialize expression node depending on its type.
    if (expression->instanceOf<LogicalExpressionNode>()) {
        // serialize logical expression
        serializeLogicalExpressions(expression, serializedExpression);
    } else if (expression->instanceOf<ArithmeticalExpressionNode>()) {
        // serialize arithmetical expressions
        serializeArithmeticalExpressions(expression, serializedExpression);
    } else if (expression->instanceOf<ConstantValueExpressionNode>()) {
        // serialize constant value expression node.
        auto constantValueExpression = expression->as<ConstantValueExpressionNode>();
        auto value = constantValueExpression->getConstantValue();
        // serialize value
        auto serializedConstantValue = SerializableExpression_ConstantValueExpression();
        DataTypeSerializationUtil::serializeDataValue(value, serializedConstantValue.mutable_value());
        serializedExpression->mutable_details()->PackFrom(serializedConstantValue);
    } else if (expression->instanceOf<FieldAccessExpressionNode>()) {
        // serialize field access expression node
        auto fieldAccessExpression = expression->as<FieldAccessExpressionNode>();
        auto serializedFieldAccessExpression = SerializableExpression_FieldAccessExpression();
        serializedFieldAccessExpression.set_fieldname(fieldAccessExpression->getFieldName());
        serializedExpression->mutable_details()->PackFrom(serializedFieldAccessExpression);
    } else if (expression->instanceOf<FieldAssignmentExpressionNode>()) {
        // serialize field assignment expression node.
        auto fieldAssignmentExpressionNode = expression->as<FieldAssignmentExpressionNode>();
        auto serializedFieldAssignmentExpression = SerializableExpression_FieldAssignmentExpression();
        auto serializedFieldAccessExpression = serializedFieldAssignmentExpression.mutable_field();
        serializedFieldAccessExpression->set_fieldname(fieldAssignmentExpressionNode->getField()->getFieldName());
        // serialize assignment expression
        serializeExpression(fieldAssignmentExpressionNode->getAssignment(), serializedFieldAssignmentExpression.mutable_assignment());
        serializedExpression->mutable_details()->PackFrom(serializedFieldAssignmentExpression);
    }
    DataTypeSerializationUtil::serializeDataType(expression->getStamp(), serializedExpression->mutable_stamp());
    return serializedExpression;
}

ExpressionNodePtr ExpressionSerializationUtil::deserializeExpression(SerializableExpression* serializedExpression) {

    // de-serialize expression
    // 1. check if the serialized expression is a logical expression
    auto expressionNodePtr = deserializeLogicalExpressions(serializedExpression);
    // 2. if the expression was not de-serialized then try if its a arithmetical expression
    if (!expressionNodePtr) {
        expressionNodePtr = deserializeArithmeticalExpressions(serializedExpression);
    }
    // 3. if the expression was not de-serialized try remaining expression types
    if (!expressionNodePtr) {
        if (serializedExpression->details().Is<SerializableExpression_ConstantValueExpression>()) {
            // de-serialize constant value expression node.
            auto serializedConstantValue = SerializableExpression_ConstantValueExpression();
            serializedExpression->details().UnpackTo(&serializedConstantValue);
            auto valueType = DataTypeSerializationUtil::deserializeDataValue(serializedConstantValue.release_value());
            expressionNodePtr = ConstantValueExpressionNode::create(valueType);
        } else if (serializedExpression->details().Is<SerializableExpression_FieldAccessExpression>()) {
            // de-serialize field read expression node.
            SerializableExpression_FieldAccessExpression serializedFieldAccessExpression;
            serializedExpression->details().UnpackTo(&serializedFieldAccessExpression);
            auto name = serializedFieldAccessExpression.fieldname();
            expressionNodePtr = FieldAccessExpressionNode::create(name);
        } else if (serializedExpression->details().Is<SerializableExpression_FieldAssignmentExpression>()) {
            // de-serialize field read expression node.
            SerializableExpression_FieldAssignmentExpression serializedFieldAccessExpression;
            serializedExpression->details().UnpackTo(&serializedFieldAccessExpression);
            auto field = serializedFieldAccessExpression.mutable_field();
            auto fieldAccessNode = FieldAccessExpressionNode::create(field->fieldname());
            auto fieldAssignmentExpression = deserializeExpression(serializedFieldAccessExpression.mutable_assignment());
            expressionNodePtr = FieldAssignmentExpressionNode::create(fieldAccessNode->as<FieldAccessExpressionNode>(), fieldAssignmentExpression);
        }
    }
    // deserialize expression stamp
    auto stamp = DataTypeSerializationUtil::deserializeDataType(serializedExpression->mutable_stamp());
    expressionNodePtr->setStamp(stamp);

    return expressionNodePtr;
}

void ExpressionSerializationUtil::serializeArithmeticalExpressions(ExpressionNodePtr expression, SerializableExpression* serializedExpression) {
    if (expression->instanceOf<AddExpressionNode>()) {
        // serialize add expression node.
        auto addExpressionNode = expression->as<AddExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_AddExpression();
        serializeExpression(addExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(addExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<SubExpressionNode>()) {
        // serialize sub expression node.
        auto subExpressionNode = expression->as<SubExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_SubExpression();
        serializeExpression(subExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(subExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<MulExpressionNode>()) {
        // serialize mul expression node.
        auto mulExpressionNode = expression->as<MulExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_MulExpression();
        serializeExpression(mulExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(mulExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<DivExpressionNode>()) {
        // serialize div expression node.
        auto divExpressionNode = expression->as<DivExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_DivExpression();
        serializeExpression(divExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(divExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else {
        NES_FATAL_ERROR(
            "TranslateToLegacyPhase: No serialization implemented for this arithmetical expression node: "
            << expression->toString());
        NES_NOT_IMPLEMENTED();
    }
}

void ExpressionSerializationUtil::serializeLogicalExpressions(ExpressionNodePtr expression, SerializableExpression* serializedExpression) {

    if (expression->instanceOf<AndExpressionNode>()) {
        // serialize and expression node.
        auto andExpressionNode = expression->as<AndExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_AndExpression();
        serializeExpression(andExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(andExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<OrExpressionNode>()) {
        // serialize or expression node.
        auto orExpressionNode = expression->as<OrExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_OrExpression();
        serializeExpression(orExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(orExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<LessExpressionNode>()) {
        // serialize less expression node.
        auto lessExpressionNode = expression->as<LessExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_LessExpression();
        serializeExpression(lessExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(lessExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<LessEqualsExpressionNode>()) {
        // serialize less equals expression node.
        auto lessEqualsExpressionNode = expression->as<LessEqualsExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_LessEqualsExpression();
        serializeExpression(lessEqualsExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(lessEqualsExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<GreaterExpressionNode>()) {
        // serialize greater expression node.
        auto greaterExpressionNode = expression->as<GreaterExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_GreaterExpression();
        serializeExpression(greaterExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(greaterExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<GreaterEqualsExpressionNode>()) {
        // serialize greater equals expression node.
        auto greaterEqualsExpressionNode = expression->as<GreaterEqualsExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_GreaterEqualsExpression();
        serializeExpression(greaterEqualsExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(greaterEqualsExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<EqualsExpressionNode>()) {
        // serialize equals expression node.
        auto equalsExpressionNode = expression->as<EqualsExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_EqualsExpression();
        serializeExpression(equalsExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(equalsExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<NegateExpressionNode>()) {
        // serialize negate expression node.
        auto equalsExpressionNode = expression->as<NegateExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_NegateExpression();
        serializeExpression(equalsExpressionNode->child(), serializedExpressionNode.mutable_child());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else {
        NES_FATAL_ERROR(
            "ExpressionSerializationUtil: No serialization implemented for this logical expression node: "
            << expression->toString());
        NES_NOT_IMPLEMENTED();
    }
}

ExpressionNodePtr ExpressionSerializationUtil::deserializeArithmeticalExpressions(SerializableExpression* serializedExpression) {
    if (serializedExpression->details().Is<SerializableExpression_AddExpression>()) {
        // de-serialize ADD expression node.
        auto serializedExpressionNode = SerializableExpression_AddExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return AddExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_SubExpression>()) {
        // de-serialize SUB expression node.
        auto serializedExpressionNode = SerializableExpression_SubExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return SubExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_MulExpression>()) {
        // de-serialize MUL expression node.
        auto serializedExpressionNode = SerializableExpression_MulExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return MulExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_DivExpression>()) {
        // de-serialize DIV expression node.
        auto serializedExpressionNode = SerializableExpression_DivExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return DivExpressionNode::create(left, right);
    }
    return nullptr;
}

ExpressionNodePtr ExpressionSerializationUtil::deserializeLogicalExpressions(SerializableExpression* serializedExpression) {

    if (serializedExpression->details().Is<SerializableExpression_AndExpression>()) {
        // de-serialize and expression node.
        auto serializedExpressionNode = SerializableExpression_AndExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return AndExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_OrExpression>()) {
        // de-serialize or expression node.
        auto serializedExpressionNode = SerializableExpression_OrExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return OrExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_LessExpression>()) {
        // de-serialize less expression node.
        auto serializedExpressionNode = SerializableExpression_LessExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return LessExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_LessEqualsExpression>()) {
        // de-serialize less equals expression node.
        auto serializedExpressionNode = SerializableExpression_LessEqualsExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return LessEqualsExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_GreaterExpression>()) {
        // de-serialize greater expression node.
        auto serializedExpressionNode = SerializableExpression_GreaterExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return GreaterExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_GreaterEqualsExpression>()) {
        // de-serialize greater equals expression node.
        auto serializedExpressionNode = SerializableExpression_GreaterEqualsExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return GreaterEqualsExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_EqualsExpression>()) {
        // de-serialize equals expression node.
        auto serializedExpressionNode = SerializableExpression_EqualsExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return EqualsExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_NegateExpression>()) {
        // de-serialize negate expression node.
        auto serializedExpressionNode = SerializableExpression_NegateExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto child = deserializeExpression(serializedExpressionNode.release_child());
        return NegateExpressionNode::create(child);
    }
    return nullptr;
}

}// namespace NES