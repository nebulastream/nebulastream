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
    if (expression->instanceOf<LogicalExpressionNode>()) {
        // Translate logical expressions to the legacy representation
        serializeLogicalExpressions(expression, serializedExpression);
    } else if (expression->instanceOf<ArithmeticalExpressionNode>()) {
        // Translate arithmetical expressions to the legacy representation
        serializeArithmeticalExpressions(expression, serializedExpression);
    } else if (expression->instanceOf<ConstantValueExpressionNode>()) {
        // Translate constant value expression node.
        auto constantValueExpression = expression->as<ConstantValueExpressionNode>();
        auto value = constantValueExpression->getConstantValue();
        auto serializedConstantValue = SerializableExpression_ConstantValueExpression();
        // todo serialize constant value
        serializedExpression->mutable_details()->PackFrom(serializedConstantValue);
    } else if (expression->instanceOf<FieldAccessExpressionNode>()) {
        // Translate field read expression node.
        auto fieldAccessExpression = expression->as<FieldAccessExpressionNode>();
        auto serializedFieldAccessExpression = SerializableExpression_FieldAccessExpression();
        // todo add field type
        serializedFieldAccessExpression.set_fieldname(fieldAccessExpression->getFieldName());
        serializedExpression->mutable_details()->PackFrom(serializedFieldAccessExpression);
    } else if (expression->instanceOf<FieldAssignmentExpressionNode>()) {
        // Translate field read expression node.
        auto fieldAssignmentExpressionNode = expression->as<FieldAssignmentExpressionNode>();
        auto serializedFieldAssignmentExpression = SerializableExpression_FieldAssignmentExpression();
        // todo add field type
        auto serializedFieldAccessExpression = serializedFieldAssignmentExpression.mutable_field();
        serializedFieldAccessExpression->set_fieldname(fieldAssignmentExpressionNode->getField()->getFieldName());
        serializeExpression(fieldAssignmentExpressionNode->getAssignment(), serializedFieldAssignmentExpression.mutable_assignment());
        serializedExpression->mutable_details()->PackFrom(serializedFieldAssignmentExpression);
    }
    DataTypeSerializationUtil::serializeDataType(expression->getStamp(), serializedExpression->mutable_stamp());
    return serializedExpression;
}

ExpressionNodePtr ExpressionSerializationUtil::deserializeExpression(SerializableExpression* serializedExpression) {

    auto expressionNodePtr = deserializeLogicalExpressions(serializedExpression);
    if (!expressionNodePtr) {
        expressionNodePtr = deserializeArithmeticalExpressions(serializedExpression);
    }
    if (!expressionNodePtr) {
        if (serializedExpression->details().Is<SerializableExpression_ConstantValueExpression>()) {
            // Translate constant value expression node.
            auto serializedConstantValue = SerializableExpression_ConstantValueExpression();
            serializedExpression->details().UnpackTo(&serializedConstantValue);
            auto type = DataTypeSerializationUtil::deserializeDataType(serializedConstantValue.release_type());
            // todo deserialize constant value
            expressionNodePtr = ConstantValueExpressionNode::create(type->getDefaultInitValue());
        } else if (serializedExpression->details().Is<SerializableExpression_FieldAccessExpression>()) {
            // Translate field read expression node.
            SerializableExpression_FieldAccessExpression serializedFieldAccessExpression;
            serializedExpression->details().UnpackTo(&serializedFieldAccessExpression);
            auto name = serializedFieldAccessExpression.fieldname();
            expressionNodePtr = FieldAccessExpressionNode::create(name);
        } else if (serializedExpression->details().Is<SerializableExpression_FieldAssignmentExpression>()) {
            // Translate field read expression node.
            SerializableExpression_FieldAssignmentExpression serializedFieldAccessExpression;
            serializedExpression->details().UnpackTo(&serializedFieldAccessExpression);
            auto field = serializedFieldAccessExpression.mutable_field();
            // todo add field type
            auto fieldAccessNode = FieldAccessExpressionNode::create(field->fieldname());
            auto fieldAssignmentExpression = deserializeExpression(serializedFieldAccessExpression.mutable_assignment());
            expressionNodePtr = FieldAssignmentExpressionNode::create(fieldAccessNode->as<FieldAccessExpressionNode>(), fieldAssignmentExpression);
        }
    }
    auto stamp = DataTypeSerializationUtil::deserializeDataType(serializedExpression->mutable_stamp());
    expressionNodePtr->setStamp(stamp);
    return expressionNodePtr;
}

void ExpressionSerializationUtil::serializeArithmeticalExpressions(ExpressionNodePtr expression, SerializableExpression* serializedExpression) {
    if (expression->instanceOf<AddExpressionNode>()) {
        // Translate add expression node.
        auto addExpressionNode = expression->as<AddExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_AddExpression();
        serializeExpression(addExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(addExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<SubExpressionNode>()) {
        // Translate sub expression node.
        auto subExpressionNode = expression->as<SubExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_SubExpression();
        serializeExpression(subExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(subExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<MulExpressionNode>()) {
        // Translate mul expression node.
        auto mulExpressionNode = expression->as<MulExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_MulExpression();
        serializeExpression(mulExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(mulExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<DivExpressionNode>()) {
        // Translate div expression node.
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
        // Translate and expression node.
        auto andExpressionNode = expression->as<AndExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_AndExpression();
        serializeExpression(andExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(andExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<OrExpressionNode>()) {
        // Translate or expression node.
        auto orExpressionNode = expression->as<OrExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_OrExpression();
        serializeExpression(orExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(orExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<LessExpressionNode>()) {
        // Translate less expression node.
        auto lessExpressionNode = expression->as<LessExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_LessExpression();
        serializeExpression(lessExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(lessExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<LessEqualsExpressionNode>()) {
        // Translate less equals expression node.
        auto lessEqualsExpressionNode = expression->as<LessEqualsExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_LessEqualsExpression();
        serializeExpression(lessEqualsExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(lessEqualsExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<GreaterExpressionNode>()) {
        // Translate greater expression node.
        auto greaterExpressionNode = expression->as<GreaterExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_GreaterExpression();
        serializeExpression(greaterExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(greaterExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<GreaterEqualsExpressionNode>()) {
        // Translate greater equals expression node.
        auto greaterEqualsExpressionNode = expression->as<GreaterEqualsExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_GreaterEqualsExpression();
        serializeExpression(greaterEqualsExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(greaterEqualsExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<EqualsExpressionNode>()) {
        // Translate equals expression node.
        auto equalsExpressionNode = expression->as<EqualsExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_EqualsExpression();
        serializeExpression(equalsExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(equalsExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<NegateExpressionNode>()) {
        // Translate negate expression node.
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
        // Translate and expression node.
        auto serializedExpressionNode = SerializableExpression_AddExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return AddExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_SubExpression>()) {
        // Translate or expression node.
        auto serializedExpressionNode = SerializableExpression_SubExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return SubExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_MulExpression>()) {
        // Translate or expression node.
        auto serializedExpressionNode = SerializableExpression_MulExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return MulExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_DivExpression>()) {
        // Translate or expression node.
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
        // Translate and expression node.
        auto serializedExpressionNode = SerializableExpression_AndExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return AndExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_OrExpression>()) {
        // Translate or expression node.
        auto serializedExpressionNode = SerializableExpression_OrExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return OrExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_LessExpression>()) {
        // Translate less expression node.
        auto serializedExpressionNode = SerializableExpression_LessExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return LessExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_LessEqualsExpression>()) {
        // Translate less equals expression node.
        auto serializedExpressionNode = SerializableExpression_LessEqualsExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return LessEqualsExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_GreaterExpression>()) {
        // Translate greater expression node.
        auto serializedExpressionNode = SerializableExpression_GreaterExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return GreaterExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_GreaterEqualsExpression>()) {
        // Translate greater equals expression node.
        auto serializedExpressionNode = SerializableExpression_GreaterEqualsExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return GreaterEqualsExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_EqualsExpression>()) {
        // Translate equals expression node.
        auto serializedExpressionNode = SerializableExpression_EqualsExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return EqualsExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_NegateExpression>()) {
        // Translate negate expression node.
        auto serializedExpressionNode = SerializableExpression_NegateExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto child = deserializeExpression(serializedExpressionNode.release_child());
        return NegateExpressionNode::create(child);
    }
    return nullptr;
}

}// namespace NES