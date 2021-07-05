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

#include <GRPC/Serialization/DataTypeSerializationUtil.hpp>
#include <GRPC/Serialization/ExpressionSerializationUtil.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AbsExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/CeilExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/ExpExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/FloorExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/Log10ExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/LogExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/ModExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/PowExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/RoundExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/SqrtExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/FieldRenameExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <SerializableExpression.pb.h>
namespace NES {

SerializableExpression* ExpressionSerializationUtil::serializeExpression(const ExpressionNodePtr& expression,
                                                                         SerializableExpression* serializedExpression) {
    NES_DEBUG("ExpressionSerializationUtil:: serialize expression " << expression->toString());
    // serialize expression node depending on its type.
    if (expression->instanceOf<LogicalExpressionNode>()) {
        // serialize logical expression
        serializeLogicalExpressions(expression, serializedExpression);
    } else if (expression->instanceOf<ArithmeticalExpressionNode>()) {
        // serialize arithmetical expressions
        serializeArithmeticalExpressions(expression, serializedExpression);
    } else if (expression->instanceOf<ConstantValueExpressionNode>()) {
        // serialize constant value expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize constant value expression node.");
        auto constantValueExpression = expression->as<ConstantValueExpressionNode>();
        auto value = constantValueExpression->getConstantValue();
        // serialize value
        auto serializedConstantValue = SerializableExpression_ConstantValueExpression();
        DataTypeSerializationUtil::serializeDataValue(value, serializedConstantValue.mutable_value());
        serializedExpression->mutable_details()->PackFrom(serializedConstantValue);
    } else if (expression->instanceOf<FieldAccessExpressionNode>()) {
        // serialize field access expression node
        NES_TRACE("ExpressionSerializationUtil:: serialize field access expression node.");
        auto fieldAccessExpression = expression->as<FieldAccessExpressionNode>();
        auto serializedFieldAccessExpression = SerializableExpression_FieldAccessExpression();
        serializedFieldAccessExpression.set_fieldname(fieldAccessExpression->getFieldName());
        serializedExpression->mutable_details()->PackFrom(serializedFieldAccessExpression);
    } else if (expression->instanceOf<FieldRenameExpressionNode>()) {
        // serialize field rename expression node
        NES_TRACE("ExpressionSerializationUtil:: serialize field rename expression node.");
        auto fieldRenameExpression = expression->as<FieldRenameExpressionNode>();
        auto serializedFieldRenameExpression = SerializableExpression_FieldRenameExpression();
        serializeExpression(fieldRenameExpression->getOriginalField(),
                            serializedFieldRenameExpression.mutable_originalfieldaccessexpression());
        serializedFieldRenameExpression.set_newfieldname(fieldRenameExpression->getNewFieldName());
        serializedExpression->mutable_details()->PackFrom(serializedFieldRenameExpression);
    } else if (expression->instanceOf<FieldAssignmentExpressionNode>()) {
        // serialize field assignment expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize field assignment expression node.");
        auto fieldAssignmentExpressionNode = expression->as<FieldAssignmentExpressionNode>();
        auto serializedFieldAssignmentExpression = SerializableExpression_FieldAssignmentExpression();
        auto* serializedFieldAccessExpression = serializedFieldAssignmentExpression.mutable_field();
        serializedFieldAccessExpression->set_fieldname(fieldAssignmentExpressionNode->getField()->getFieldName());
        DataTypeSerializationUtil::serializeDataType(fieldAssignmentExpressionNode->getField()->getStamp(),
                                                     serializedFieldAccessExpression->mutable_type());
        // serialize assignment expression
        serializeExpression(fieldAssignmentExpressionNode->getAssignment(),
                            serializedFieldAssignmentExpression.mutable_assignment());
        serializedExpression->mutable_details()->PackFrom(serializedFieldAssignmentExpression);
    } else {
        NES_FATAL_ERROR("ExpressionSerializationUtil: could not serialize this expression: " << expression->toString());
    }
    DataTypeSerializationUtil::serializeDataType(expression->getStamp(), serializedExpression->mutable_stamp());
    NES_DEBUG("ExpressionSerializationUtil:: serialize expression node to "
              << serializedExpression->mutable_details()->type_url());
    return serializedExpression;
}

ExpressionNodePtr ExpressionSerializationUtil::deserializeExpression(SerializableExpression* serializedExpression) {
    NES_DEBUG("ExpressionSerializationUtil:: deserialize expression " << serializedExpression->details().type_url());
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
            NES_TRACE("ExpressionSerializationUtil:: de-serialize expression as Constant Value expression node.");
            auto serializedConstantValue = SerializableExpression_ConstantValueExpression();
            serializedExpression->details().UnpackTo(&serializedConstantValue);
            auto valueType = DataTypeSerializationUtil::deserializeDataValue(serializedConstantValue.release_value());
            expressionNodePtr = ConstantValueExpressionNode::create(valueType);
        } else if (serializedExpression->details().Is<SerializableExpression_FieldAccessExpression>()) {
            // de-serialize field access expression node.
            NES_TRACE("ExpressionSerializationUtil:: de-serialize expression as FieldAccess expression node.");
            SerializableExpression_FieldAccessExpression serializedFieldAccessExpression;
            serializedExpression->details().UnpackTo(&serializedFieldAccessExpression);
            auto name = serializedFieldAccessExpression.fieldname();
            expressionNodePtr = FieldAccessExpressionNode::create(name);
        } else if (serializedExpression->details().Is<SerializableExpression_FieldRenameExpression>()) {
            // de-serialize field rename expression node.
            NES_TRACE("ExpressionSerializationUtil:: de-serialize expression as Field Rename expression node.");
            SerializableExpression_FieldRenameExpression serializedFieldRenameExpression;
            serializedExpression->details().UnpackTo(&serializedFieldRenameExpression);
            auto originalFieldAccessExpression =
                deserializeExpression(serializedFieldRenameExpression.mutable_originalfieldaccessexpression());
            if (!originalFieldAccessExpression->instanceOf<FieldAccessExpressionNode>()) {
                NES_FATAL_ERROR("ExpressionSerializationUtil: the original field access expression "
                                "should be of type FieldAccessExpressionNode, but was a "
                                << originalFieldAccessExpression->toString());
            }
            auto newFieldName = serializedFieldRenameExpression.newfieldname();
            expressionNodePtr =
                FieldRenameExpressionNode::create(originalFieldAccessExpression->as<FieldAccessExpressionNode>(), newFieldName);
        } else if (serializedExpression->details().Is<SerializableExpression_FieldAssignmentExpression>()) {
            // de-serialize field read expression node.
            NES_TRACE("ExpressionSerializationUtil:: de-serialize expression as FieldAssignment expression node.");
            SerializableExpression_FieldAssignmentExpression serializedFieldAccessExpression;
            serializedExpression->details().UnpackTo(&serializedFieldAccessExpression);
            auto* field = serializedFieldAccessExpression.mutable_field();
            auto fieldStamp = DataTypeSerializationUtil::deserializeDataType(field->mutable_type());
            auto fieldAccessNode = FieldAccessExpressionNode::create(fieldStamp, field->fieldname());
            auto fieldAssignmentExpression = deserializeExpression(serializedFieldAccessExpression.mutable_assignment());
            expressionNodePtr = FieldAssignmentExpressionNode::create(fieldAccessNode->as<FieldAccessExpressionNode>(),
                                                                      fieldAssignmentExpression);
        } else {
            NES_FATAL_ERROR("ExpressionSerializationUtil: could not de-serialize this expression");
        }
    }

    if (!expressionNodePtr) {
        NES_FATAL_ERROR(
            "ExpressionSerializationUtil:: fatal error during de-serialization. The expression node must not be null");
    }
    // deserialize expression stamp
    auto stamp = DataTypeSerializationUtil::deserializeDataType(serializedExpression->mutable_stamp());
    expressionNodePtr->setStamp(stamp);
    NES_DEBUG(
        "ExpressionSerializationUtil:: deserialized expression node to the following node: " << expressionNodePtr->toString());
    return expressionNodePtr;
}

void ExpressionSerializationUtil::serializeArithmeticalExpressions(const ExpressionNodePtr& expression,
                                                                   SerializableExpression* serializedExpression) {
    NES_DEBUG("ExpressionSerializationUtil:: serialize arithmetical expression " << expression->toString());
    if (expression->instanceOf<AddExpressionNode>()) {
        // serialize add expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize ADD arithmetical expression to SerializableExpression_AddExpression");
        auto addExpressionNode = expression->as<AddExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_AddExpression();
        serializeExpression(addExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(addExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<SubExpressionNode>()) {
        // serialize sub expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize SUB arithmetical expression to SerializableExpression_SubExpression");
        auto subExpressionNode = expression->as<SubExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_SubExpression();
        serializeExpression(subExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(subExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<MulExpressionNode>()) {
        // serialize mul expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize MUL arithmetical expression to SerializableExpression_MulExpression");
        auto mulExpressionNode = expression->as<MulExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_MulExpression();
        serializeExpression(mulExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(mulExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<DivExpressionNode>()) {
        // serialize div expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize DIV arithmetical expression to SerializableExpression_DivExpression");
        auto divExpressionNode = expression->as<DivExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_DivExpression();
        serializeExpression(divExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(divExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<ModExpressionNode>()) {
        // serialize mod expression node.
        NES_TRACE(
            "ExpressionSerializationUtil:: serialize MODULO arithmetical expression to SerializableExpression_PowExpression");
        auto modExpressionNode = expression->as<ModExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_ModExpression();
        serializeExpression(modExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(modExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<PowExpressionNode>()) {
        // serialize pow expression node.
        NES_TRACE(
            "ExpressionSerializationUtil:: serialize POWER arithmetical expression to SerializableExpression_PowExpression");
        auto powExpressionNode = expression->as<PowExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_PowExpression();
        serializeExpression(powExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(powExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<AbsExpressionNode>()) {
        // serialize abs expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize ABS arithmetical expression to SerializableExpression_AbsExpression");
        auto absExpressionNode = expression->as<AbsExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_AbsExpression();
        serializeExpression(absExpressionNode->child(), serializedExpressionNode.mutable_child());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<CeilExpressionNode>()) {
        // serialize ceil expression node.
        NES_TRACE(
            "ExpressionSerializationUtil:: serialize CEIL arithmetical expression to SerializableExpression_CeilExpression");
        auto ceilExpressionNode = expression->as<CeilExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_CeilExpression();
        serializeExpression(ceilExpressionNode->child(), serializedExpressionNode.mutable_child());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<ExpExpressionNode>()) {
        // serialize exp expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize EXP arithmetical expression to SerializableExpression_ExpExpression");
        auto expExpressionNode = expression->as<ExpExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_ExpExpression();
        serializeExpression(expExpressionNode->child(), serializedExpressionNode.mutable_child());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<FloorExpressionNode>()) {
        // serialize floor expression node.
        NES_TRACE(
            "ExpressionSerializationUtil:: serialize FLOOR arithmetical expression to SerializableExpression_FloorExpression");
        auto floorExpressionNode = expression->as<FloorExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_FloorExpression();
        serializeExpression(floorExpressionNode->child(), serializedExpressionNode.mutable_child());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<LogExpressionNode>()) {
        // serialize log expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize LOG arithmetical expression to SerializableExpression_LogExpression");
        auto logExpressionNode = expression->as<LogExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_LogExpression();
        serializeExpression(logExpressionNode->child(), serializedExpressionNode.mutable_child());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<Log10ExpressionNode>()) {
        // serialize log10 expression node.
        NES_TRACE(
            "ExpressionSerializationUtil:: serialize LOG10 arithmetical expression to SerializableExpression_Log10Expression");
        auto log10ExpressionNode = expression->as<Log10ExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_Log10Expression();
        serializeExpression(log10ExpressionNode->child(), serializedExpressionNode.mutable_child());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<RoundExpressionNode>()) {
        // serialize round expression node.
        NES_TRACE(
            "ExpressionSerializationUtil:: serialize ROUND arithmetical expression to SerializableExpression_RoundExpression");
        auto roundExpressionNode = expression->as<RoundExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_RoundExpression();
        serializeExpression(roundExpressionNode->child(), serializedExpressionNode.mutable_child());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<SqrtExpressionNode>()) {
        // serialize sqrt expression node.
        NES_TRACE(
            "ExpressionSerializationUtil:: serialize SQRT arithmetical expression to SerializableExpression_SqrtExpression");
        auto sqrtExpressionNode = expression->as<SqrtExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_SqrtExpression();
        serializeExpression(sqrtExpressionNode->child(), serializedExpressionNode.mutable_child());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else {
        NES_FATAL_ERROR("TranslateToLegacyPhase: No serialization implemented for this arithmetical expression node: "
                        << expression->toString());
        NES_NOT_IMPLEMENTED();
    }
}

void ExpressionSerializationUtil::serializeLogicalExpressions(const ExpressionNodePtr& expression,
                                                              SerializableExpression* serializedExpression) {
    NES_DEBUG("ExpressionSerializationUtil:: serialize logical expression " << expression->toString());
    if (expression->instanceOf<AndExpressionNode>()) {
        // serialize and expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize AND logical expression to SerializableExpression_AndExpression");
        auto andExpressionNode = expression->as<AndExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_AndExpression();
        serializeExpression(andExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(andExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<OrExpressionNode>()) {
        // serialize or expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize OR logical expression to SerializableExpression_OrExpression");
        auto orExpressionNode = expression->as<OrExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_OrExpression();
        serializeExpression(orExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(orExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<LessExpressionNode>()) {
        // serialize less expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize Less logical expression to SerializableExpression_LessExpression");
        auto lessExpressionNode = expression->as<LessExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_LessExpression();
        serializeExpression(lessExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(lessExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<LessEqualsExpressionNode>()) {
        // serialize less equals expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize Less Equals logical expression to "
                  "SerializableExpression_LessEqualsExpression");
        auto lessEqualsExpressionNode = expression->as<LessEqualsExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_LessEqualsExpression();
        serializeExpression(lessEqualsExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(lessEqualsExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<GreaterExpressionNode>()) {
        // serialize greater expression node.
        NES_TRACE(
            "ExpressionSerializationUtil:: serialize Greater logical expression to SerializableExpression_GreaterExpression");
        auto greaterExpressionNode = expression->as<GreaterExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_GreaterExpression();
        serializeExpression(greaterExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(greaterExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<GreaterEqualsExpressionNode>()) {
        // serialize greater equals expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize Greater Equals logical expression to "
                  "SerializableExpression_GreaterEqualsExpression");
        auto greaterEqualsExpressionNode = expression->as<GreaterEqualsExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_GreaterEqualsExpression();
        serializeExpression(greaterEqualsExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(greaterEqualsExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<EqualsExpressionNode>()) {
        // serialize equals expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize Equals logical expression to SerializableExpression_EqualsExpression");
        auto equalsExpressionNode = expression->as<EqualsExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_EqualsExpression();
        serializeExpression(equalsExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(equalsExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else if (expression->instanceOf<NegateExpressionNode>()) {
        // serialize negate expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize negate logical expression to SerializableExpression_NegateExpression");
        auto equalsExpressionNode = expression->as<NegateExpressionNode>();
        auto serializedExpressionNode = SerializableExpression_NegateExpression();
        serializeExpression(equalsExpressionNode->child(), serializedExpressionNode.mutable_child());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    } else {
        NES_FATAL_ERROR("ExpressionSerializationUtil: No serialization implemented for this logical expression node: "
                        << expression->toString());
        NES_NOT_IMPLEMENTED();
    }
}

ExpressionNodePtr ExpressionSerializationUtil::deserializeArithmeticalExpressions(SerializableExpression* serializedExpression) {
    if (serializedExpression->details().Is<SerializableExpression_AddExpression>()) {
        // de-serialize ADD expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as Add expression node.");
        auto serializedExpressionNode = SerializableExpression_AddExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return AddExpressionNode::create(left, right);
    }
    if (serializedExpression->details().Is<SerializableExpression_SubExpression>()) {
        // de-serialize SUB expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as SUB expression node.");
        auto serializedExpressionNode = SerializableExpression_SubExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return SubExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_MulExpression>()) {
        // de-serialize MUL expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as MUL expression node.");
        auto serializedExpressionNode = SerializableExpression_MulExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return MulExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_DivExpression>()) {
        // de-serialize DIV expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as DIV expression node.");
        auto serializedExpressionNode = SerializableExpression_DivExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return DivExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_ModExpression>()) {
        // de-serialize MODULO expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as MODULO expression node.");
        auto serializedExpressionNode = SerializableExpression_ModExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return ModExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_PowExpression>()) {
        // de-serialize POWER expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as POWER expression node.");
        auto serializedExpressionNode = SerializableExpression_PowExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return PowExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_AbsExpression>()) {
        // de-serialize ABS expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as ABS expression node.");
        auto serializedExpressionNode = SerializableExpression_AbsExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto child = deserializeExpression(serializedExpressionNode.release_child());
        return AbsExpressionNode::create(child);
    } else if (serializedExpression->details().Is<SerializableExpression_CeilExpression>()) {
        // de-serialize CEIL expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as CEIL expression node.");
        auto serializedExpressionNode = SerializableExpression_CeilExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto child = deserializeExpression(serializedExpressionNode.release_child());
        return CeilExpressionNode::create(child);
    } else if (serializedExpression->details().Is<SerializableExpression_ExpExpression>()) {
        // de-serialize EXP expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as EXP expression node.");
        auto serializedExpressionNode = SerializableExpression_ExpExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto child = deserializeExpression(serializedExpressionNode.release_child());
        return ExpExpressionNode::create(child);
    } else if (serializedExpression->details().Is<SerializableExpression_FloorExpression>()) {
        // de-serialize FLOOR expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as FLOOR expression node.");
        auto serializedExpressionNode = SerializableExpression_FloorExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto child = deserializeExpression(serializedExpressionNode.release_child());
        return FloorExpressionNode::create(child);
    } else if (serializedExpression->details().Is<SerializableExpression_LogExpression>()) {
        // de-serialize LOG expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as LOG expression node.");
        auto serializedExpressionNode = SerializableExpression_LogExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto child = deserializeExpression(serializedExpressionNode.release_child());
        return LogExpressionNode::create(child);
    } else if (serializedExpression->details().Is<SerializableExpression_Log10Expression>()) {
        // de-serialize LOG10 expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as LOG10 expression node.");
        auto serializedExpressionNode = SerializableExpression_Log10Expression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto child = deserializeExpression(serializedExpressionNode.release_child());
        return Log10ExpressionNode::create(child);
    } else if (serializedExpression->details().Is<SerializableExpression_RoundExpression>()) {
        // de-serialize ROUND expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as ROUND expression node.");
        auto serializedExpressionNode = SerializableExpression_RoundExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto child = deserializeExpression(serializedExpressionNode.release_child());
        return RoundExpressionNode::create(child);
    } else if (serializedExpression->details().Is<SerializableExpression_SqrtExpression>()) {
        // de-serialize SQRT expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as SQRT expression node.");
        auto serializedExpressionNode = SerializableExpression_SqrtExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto child = deserializeExpression(serializedExpressionNode.release_child());
        return SqrtExpressionNode::create(child);
    }
    return nullptr;
}

ExpressionNodePtr ExpressionSerializationUtil::deserializeLogicalExpressions(SerializableExpression* serializedExpression) {
    NES_DEBUG("ExpressionSerializationUtil:: de-serialize logical expression" << serializedExpression->details().type_url());
    if (serializedExpression->details().Is<SerializableExpression_AndExpression>()) {
        NES_TRACE("ExpressionSerializationUtil:: de-serialize logical expression as AND expression node.");
        // de-serialize and expression node.
        auto serializedExpressionNode = SerializableExpression_AndExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return AndExpressionNode::create(left, right);
    }
    if (serializedExpression->details().Is<SerializableExpression_OrExpression>()) {
        // de-serialize or expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize logical expression as OR expression node.");
        auto serializedExpressionNode = SerializableExpression_OrExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return OrExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_LessExpression>()) {
        // de-serialize less expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize logical expression as LESS expression node.");
        auto serializedExpressionNode = SerializableExpression_LessExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return LessExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_LessEqualsExpression>()) {
        // de-serialize less equals expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize logical expression as LESS Equals expression node.");
        auto serializedExpressionNode = SerializableExpression_LessEqualsExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return LessEqualsExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_GreaterExpression>()) {
        // de-serialize greater expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize logical expression as Greater expression node.");
        auto serializedExpressionNode = SerializableExpression_GreaterExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return GreaterExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_GreaterEqualsExpression>()) {
        // de-serialize greater equals expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize logical expression as GreaterEquals expression node.");
        auto serializedExpressionNode = SerializableExpression_GreaterEqualsExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return GreaterEqualsExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_EqualsExpression>()) {
        // de-serialize equals expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize logical expression as Equals expression node.");
        auto serializedExpressionNode = SerializableExpression_EqualsExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.release_left());
        auto right = deserializeExpression(serializedExpressionNode.release_right());
        return EqualsExpressionNode::create(left, right);
    } else if (serializedExpression->details().Is<SerializableExpression_NegateExpression>()) {
        // de-serialize negate expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize logical expression as Negate expression node.");
        auto serializedExpressionNode = SerializableExpression_NegateExpression();
        serializedExpression->details().UnpackTo(&serializedExpressionNode);
        auto child = deserializeExpression(serializedExpressionNode.release_child());
        return NegateExpressionNode::create(child);
    }
    return nullptr;
}

}// namespace NES