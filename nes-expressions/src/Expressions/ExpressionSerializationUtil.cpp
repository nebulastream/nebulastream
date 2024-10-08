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

#include <Expressions/ArithmeticalExpressions/AbsExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/CeilExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/ExpExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/FloorExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/ModExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/PowExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/RoundExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/SqrtExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Expressions/CaseExpressionNode.hpp>
#include <Expressions/ConstantValueExpressionNode.hpp>
#include <Expressions/ExpressionNode.hpp>
#include <Expressions/ExpressionSerializationUtil.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Expressions/FieldAssignmentExpressionNode.hpp>
#include <Expressions/FieldRenameExpressionNode.hpp>
#include <Expressions/Functions/FunctionExpressionNode.hpp>
#include <Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Expressions/WhenExpressionNode.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <SerializableExpression.pb.h>


namespace NES
{

SerializableExpression*
ExpressionSerializationUtil::serializeExpression(const ExpressionNodePtr& expression, SerializableExpression* serializedExpression)
{
    NES_DEBUG("ExpressionSerializationUtil:: serialize expression {}", expression->toString());
    /// serialize expression node depending on its type.
    if (NES::Util::instanceOf<LogicalExpressionNode>(expression))
    {
        /// serialize logical expression
        serializeLogicalExpressions(expression, serializedExpression);
    }
    else if (NES::Util::instanceOf<ArithmeticalExpressionNode>(expression))
    {
        /// serialize arithmetical expressions
        serializeArithmeticalExpressions(expression, serializedExpression);
    }
    else if (NES::Util::instanceOf<ConstantValueExpressionNode>(expression))
    {
        /// serialize constant value expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize constant value expression node.");
        auto constantValueExpression = NES::Util::as<ConstantValueExpressionNode>(expression);
        auto value = constantValueExpression->getConstantValue();
        /// serialize value
        auto serializedConstantValue = SerializableExpression_ConstantValueExpression();
        DataTypeSerializationUtil::serializeDataValue(value, serializedConstantValue.mutable_value());
        serializedExpression->mutable_details()->PackFrom(serializedConstantValue);
    }
    else if (NES::Util::instanceOf<FieldAccessExpressionNode>(expression))
    {
        /// serialize field access expression node
        NES_TRACE("ExpressionSerializationUtil:: serialize field access expression node.");
        auto fieldAccessExpression = NES::Util::as<FieldAccessExpressionNode>(expression);
        auto serializedFieldAccessExpression = SerializableExpression_FieldAccessExpression();
        serializedFieldAccessExpression.set_fieldname(fieldAccessExpression->getFieldName());
        serializedExpression->mutable_details()->PackFrom(serializedFieldAccessExpression);
    }
    else if (NES::Util::instanceOf<FieldRenameExpressionNode>(expression))
    {
        /// serialize field rename expression node
        NES_TRACE("ExpressionSerializationUtil:: serialize field rename expression node.");
        auto fieldRenameExpression = NES::Util::as<FieldRenameExpressionNode>(expression);
        auto serializedFieldRenameExpression = SerializableExpression_FieldRenameExpression();
        serializeExpression(
            fieldRenameExpression->getOriginalField(), serializedFieldRenameExpression.mutable_originalfieldaccessexpression());
        serializedFieldRenameExpression.set_newfieldname(fieldRenameExpression->getNewFieldName());
        serializedExpression->mutable_details()->PackFrom(serializedFieldRenameExpression);
    }
    else if (NES::Util::instanceOf<FieldAssignmentExpressionNode>(expression))
    {
        /// serialize field assignment expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize field assignment expression node.");
        auto fieldAssignmentExpressionNode = NES::Util::as<FieldAssignmentExpressionNode>(expression);
        auto serializedFieldAssignmentExpression = SerializableExpression_FieldAssignmentExpression();
        auto* serializedFieldAccessExpression = serializedFieldAssignmentExpression.mutable_field();
        serializedFieldAccessExpression->set_fieldname(fieldAssignmentExpressionNode->getField()->getFieldName());
        DataTypeSerializationUtil::serializeDataType(
            fieldAssignmentExpressionNode->getField()->getStamp(), serializedFieldAccessExpression->mutable_type());
        /// serialize assignment expression
        serializeExpression(fieldAssignmentExpressionNode->getAssignment(), serializedFieldAssignmentExpression.mutable_assignment());
        serializedExpression->mutable_details()->PackFrom(serializedFieldAssignmentExpression);
    }
    else if (NES::Util::instanceOf<WhenExpressionNode>(expression))
    {
        /// serialize when expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize when expression {}.", expression->toString());
        auto whenExpressionNode = NES::Util::as<WhenExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_WhenExpression();
        serializeExpression(whenExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(whenExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else if (NES::Util::instanceOf<CaseExpressionNode>(expression))
    {
        /// serialize case expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize case expression {}.", expression->toString());
        auto caseExpressionNode = NES::Util::as<CaseExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_CaseExpression();
        for (const auto& elem : caseExpressionNode->getWhenChildren())
        {
            serializeExpression(elem, serializedExpressionNode.add_left());
        }
        serializeExpression(caseExpressionNode->getDefaultExp(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else if (NES::Util::instanceOf<FunctionExpression>(expression))
    {
        /// serialize negate expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize function logical expression to SerializableExpression_FunctionExpression");
        auto functionExpressionNode = NES::Util::as<FunctionExpression>(expression);
        auto serializedExpressionNode = SerializableExpression_FunctionExpression();
        serializedExpressionNode.set_functionname(functionExpressionNode->getFunctionName());
        for (const auto& child : functionExpressionNode->getChildren())
        {
            auto argument = serializedExpressionNode.add_arguments();
            serializeExpression(NES::Util::as<ExpressionNode>(child), argument);
        }
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else
    {
        NES_FATAL_ERROR("ExpressionSerializationUtil: could not serialize this expression: {}", expression->toString());
    }

    DataTypeSerializationUtil::serializeDataType(expression->getStamp(), serializedExpression->mutable_stamp());
    NES_DEBUG("ExpressionSerializationUtil:: serialize expression node to {}", serializedExpression->mutable_details()->type_url());
    return serializedExpression;
}

ExpressionNodePtr ExpressionSerializationUtil::deserializeExpression(const SerializableExpression& serializedExpression)
{
    NES_DEBUG("ExpressionSerializationUtil:: deserialize expression {}", serializedExpression.details().type_url());
    /// de-serialize expression
    /// 1. check if the serialized expression is a logical expression
    auto expressionNodePtr = deserializeLogicalExpressions(serializedExpression);
    /// 2. if the expression was not de-serialized then try if it's an arithmetical expression
    if (!expressionNodePtr)
    {
        expressionNodePtr = deserializeArithmeticalExpressions(serializedExpression);
    }
    /// 3. if the expression was not de-serialized try remaining expression types
    if (!expressionNodePtr)
    {
        if (serializedExpression.details().Is<SerializableExpression_ConstantValueExpression>())
        {
            /// de-serialize constant value expression node.
            NES_TRACE("ExpressionSerializationUtil:: de-serialize expression as Constant Value expression node.");
            auto serializedConstantValue = SerializableExpression_ConstantValueExpression();
            serializedExpression.details().UnpackTo(&serializedConstantValue);
            auto valueType = DataTypeSerializationUtil::deserializeDataValue(serializedConstantValue.value());
            expressionNodePtr = ConstantValueExpressionNode::create(valueType);
        }
        else if (serializedExpression.details().Is<SerializableExpression_FieldAccessExpression>())
        {
            /// de-serialize field access expression node.
            NES_TRACE("ExpressionSerializationUtil:: de-serialize expression as FieldAccess expression node.");
            SerializableExpression_FieldAccessExpression serializedFieldAccessExpression;
            serializedExpression.details().UnpackTo(&serializedFieldAccessExpression);
            const auto& name = serializedFieldAccessExpression.fieldname();
            expressionNodePtr = FieldAccessExpressionNode::create(name);
        }
        else if (serializedExpression.details().Is<SerializableExpression_FieldRenameExpression>())
        {
            /// de-serialize field rename expression node.
            NES_TRACE("ExpressionSerializationUtil:: de-serialize expression as Field Rename expression node.");
            SerializableExpression_FieldRenameExpression serializedFieldRenameExpression;
            serializedExpression.details().UnpackTo(&serializedFieldRenameExpression);
            auto originalFieldAccessExpression = deserializeExpression(serializedFieldRenameExpression.originalfieldaccessexpression());
            if (!NES::Util::instanceOf<FieldAccessExpressionNode>(originalFieldAccessExpression))
            {
                NES_FATAL_ERROR(
                    "ExpressionSerializationUtil: the original field access expression "
                    "should be of type FieldAccessExpressionNode, but was a {}",
                    originalFieldAccessExpression->toString());
            }
            const auto& newFieldName = serializedFieldRenameExpression.newfieldname();
            expressionNodePtr
                = FieldRenameExpressionNode::create(NES::Util::as<FieldAccessExpressionNode>(originalFieldAccessExpression), newFieldName);
        }
        else if (serializedExpression.details().Is<SerializableExpression_FieldAssignmentExpression>())
        {
            /// de-serialize field read expression node.
            NES_TRACE("ExpressionSerializationUtil:: de-serialize expression as FieldAssignment expression node.");
            SerializableExpression_FieldAssignmentExpression serializedFieldAccessExpression;
            serializedExpression.details().UnpackTo(&serializedFieldAccessExpression);
            const auto* field = serializedFieldAccessExpression.mutable_field();
            auto fieldStamp = DataTypeSerializationUtil::deserializeDataType(field->type());
            auto fieldAccessNode = FieldAccessExpressionNode::create(fieldStamp, field->fieldname());
            auto fieldAssignmentExpression = deserializeExpression(serializedFieldAccessExpression.assignment());
            expressionNodePtr = FieldAssignmentExpressionNode::create(
                NES::Util::as<FieldAccessExpressionNode>(fieldAccessNode), fieldAssignmentExpression);
        }
        else if (serializedExpression.details().Is<SerializableExpression_FunctionExpression>())
        {
            /// de-serialize field read expression node.
            NES_TRACE("ExpressionSerializationUtil:: de-serialize expression as function expression node.");
            SerializableExpression_FunctionExpression functionExpression;
            serializedExpression.details().UnpackTo(&functionExpression);
            const auto& functionName = functionExpression.functionname();
            std::vector<ExpressionNodePtr> arguments;
            for (const auto& arg : functionExpression.arguments())
            {
                arguments.emplace_back(deserializeExpression(arg));
            }
            auto resultStamp = DataTypeSerializationUtil::deserializeDataType(serializedExpression.stamp());
            auto functionExpressionNode = FunctionExpression::create(resultStamp, functionName, arguments);
            expressionNodePtr = functionExpressionNode;
        }
        else if (serializedExpression.details().Is<SerializableExpression_WhenExpression>())
        {
            /// de-serialize WHEN expression node.
            NES_TRACE("ExpressionSerializationUtil:: de-serialize expression as When expression node.");
            auto serializedExpressionNode = SerializableExpression_WhenExpression();
            serializedExpression.details().UnpackTo(&serializedExpressionNode);
            auto left = deserializeExpression(serializedExpressionNode.left());
            auto right = deserializeExpression(serializedExpressionNode.right());
            return WhenExpressionNode::create(left, right);
        }
        else if (serializedExpression.details().Is<SerializableExpression_CaseExpression>())
        {
            /// de-serialize CASE expression node.
            NES_TRACE("ExpressionSerializationUtil:: de-serialize expression as Case expression node.");
            auto serializedExpressionNode = SerializableExpression_CaseExpression();
            serializedExpression.details().UnpackTo(&serializedExpressionNode);
            std::vector<ExpressionNodePtr> leftExps;

            ///todo: deserialization might be possible more efficiently
            for (int i = 0; i < serializedExpressionNode.left_size(); i++)
            {
                const auto& leftNode = serializedExpressionNode.left(i);
                leftExps.push_back(deserializeExpression(leftNode));
            }
            auto right = deserializeExpression(serializedExpressionNode.right());
            return CaseExpressionNode::create(leftExps, right);
        }
        else
        {
            NES_FATAL_ERROR("ExpressionSerializationUtil: could not de-serialize this expression");
        }
    }

    if (!expressionNodePtr)
    {
        NES_FATAL_ERROR("ExpressionSerializationUtil:: fatal error during de-serialization. The expression node must not be null");
    }

    /// deserialize expression stamp
    auto stamp = DataTypeSerializationUtil::deserializeDataType(serializedExpression.stamp());
    expressionNodePtr->setStamp(stamp);
    NES_DEBUG("ExpressionSerializationUtil:: deserialized expression node to the following node: {}", expressionNodePtr->toString());
    return expressionNodePtr;
}

void ExpressionSerializationUtil::serializeArithmeticalExpressions(
    const ExpressionNodePtr& expression, SerializableExpression* serializedExpression)
{
    NES_DEBUG("ExpressionSerializationUtil:: serialize arithmetical expression {}", expression->toString());
    if (NES::Util::instanceOf<AddExpressionNode>(expression))
    {
        /// serialize add expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize ADD arithmetical expression to SerializableExpression_AddExpression");
        auto addExpressionNode = NES::Util::as<AddExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_AddExpression();
        serializeExpression(addExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(addExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else if (NES::Util::instanceOf<SubExpressionNode>(expression))
    {
        /// serialize sub expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize SUB arithmetical expression to SerializableExpression_SubExpression");
        auto subExpressionNode = NES::Util::as<SubExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_SubExpression();
        serializeExpression(subExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(subExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else if (NES::Util::instanceOf<MulExpressionNode>(expression))
    {
        /// serialize mul expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize MUL arithmetical expression to SerializableExpression_MulExpression");
        auto mulExpressionNode = NES::Util::as<MulExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_MulExpression();
        serializeExpression(mulExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(mulExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else if (NES::Util::instanceOf<DivExpressionNode>(expression))
    {
        /// serialize div expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize DIV arithmetical expression to SerializableExpression_DivExpression");
        auto divExpressionNode = NES::Util::as<DivExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_DivExpression();
        serializeExpression(divExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(divExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else if (NES::Util::instanceOf<ModExpressionNode>(expression))
    {
        /// serialize mod expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize MODULO arithmetical expression to SerializableExpression_PowExpression");
        auto modExpressionNode = NES::Util::as<ModExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_ModExpression();
        serializeExpression(modExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(modExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else if (NES::Util::instanceOf<PowExpressionNode>(expression))
    {
        /// serialize pow expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize POWER arithmetical expression to SerializableExpression_PowExpression");
        auto powExpressionNode = NES::Util::as<PowExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_PowExpression();
        serializeExpression(powExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(powExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else if (NES::Util::instanceOf<AbsExpressionNode>(expression))
    {
        /// serialize abs expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize ABS arithmetical expression to SerializableExpression_AbsExpression");
        auto absExpressionNode = NES::Util::as<AbsExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_AbsExpression();
        serializeExpression(absExpressionNode->child(), serializedExpressionNode.mutable_child());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else if (NES::Util::instanceOf<CeilExpressionNode>(expression))
    {
        /// serialize ceil expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize CEIL arithmetical expression to SerializableExpression_CeilExpression");
        auto ceilExpressionNode = NES::Util::as<CeilExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_CeilExpression();
        serializeExpression(ceilExpressionNode->child(), serializedExpressionNode.mutable_child());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else if (NES::Util::instanceOf<ExpExpressionNode>(expression))
    {
        /// serialize exp expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize EXP arithmetical expression to SerializableExpression_ExpExpression");
        auto expExpressionNode = NES::Util::as<ExpExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_ExpExpression();
        serializeExpression(expExpressionNode->child(), serializedExpressionNode.mutable_child());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else if (NES::Util::instanceOf<FloorExpressionNode>(expression))
    {
        /// serialize floor expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize FLOOR arithmetical expression to SerializableExpression_FloorExpression");
        auto floorExpressionNode = NES::Util::as<FloorExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_FloorExpression();
        serializeExpression(floorExpressionNode->child(), serializedExpressionNode.mutable_child());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else if (NES::Util::instanceOf<RoundExpressionNode>(expression))
    {
        /// serialize round expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize ROUND arithmetical expression to SerializableExpression_RoundExpression");
        auto roundExpressionNode = NES::Util::as<RoundExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_RoundExpression();
        serializeExpression(roundExpressionNode->child(), serializedExpressionNode.mutable_child());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else if (NES::Util::instanceOf<SqrtExpressionNode>(expression))
    {
        /// serialize sqrt expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize SQRT arithmetical expression to SerializableExpression_SqrtExpression");
        auto sqrtExpressionNode = NES::Util::as<SqrtExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_SqrtExpression();
        serializeExpression(sqrtExpressionNode->child(), serializedExpressionNode.mutable_child());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else
    {
        NES_FATAL_ERROR(
            "TranslateToLegacyPhase: No serialization implemented for this arithmetical expression node: {}", expression->toString());
        NES_NOT_IMPLEMENTED();
    }
}

void ExpressionSerializationUtil::serializeLogicalExpressions(
    const ExpressionNodePtr& expression, SerializableExpression* serializedExpression)
{
    NES_DEBUG("ExpressionSerializationUtil:: serialize logical expression {}", expression->toString());
    if (NES::Util::instanceOf<AndExpressionNode>(expression))
    {
        /// serialize and expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize AND logical expression to SerializableExpression_AndExpression");
        auto andExpressionNode = NES::Util::as<AndExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_AndExpression();
        serializeExpression(andExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(andExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else if (NES::Util::instanceOf<OrExpressionNode>(expression))
    {
        /// serialize or expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize OR logical expression to SerializableExpression_OrExpression");
        auto orExpressionNode = NES::Util::as<OrExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_OrExpression();
        serializeExpression(orExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(orExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else if (NES::Util::instanceOf<LessExpressionNode>(expression))
    {
        /// serialize less expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize Less logical expression to SerializableExpression_LessExpression");
        auto lessExpressionNode = NES::Util::as<LessExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_LessExpression();
        serializeExpression(lessExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(lessExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else if (NES::Util::instanceOf<LessEqualsExpressionNode>(expression))
    {
        /// serialize less equals expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize Less Equals logical expression to "
                  "SerializableExpression_LessEqualsExpression");
        auto lessEqualsExpressionNode = NES::Util::as<LessEqualsExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_LessEqualsExpression();
        serializeExpression(lessEqualsExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(lessEqualsExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else if (NES::Util::instanceOf<GreaterExpressionNode>(expression))
    {
        /// serialize greater expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize Greater logical expression to SerializableExpression_GreaterExpression");
        auto greaterExpressionNode = NES::Util::as<GreaterExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_GreaterExpression();
        serializeExpression(greaterExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(greaterExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else if (NES::Util::instanceOf<GreaterEqualsExpressionNode>(expression))
    {
        /// serialize greater equals expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize Greater Equals logical expression to "
                  "SerializableExpression_GreaterEqualsExpression");
        auto greaterEqualsExpressionNode = NES::Util::as<GreaterEqualsExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_GreaterEqualsExpression();
        serializeExpression(greaterEqualsExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(greaterEqualsExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else if (NES::Util::instanceOf<EqualsExpressionNode>(expression))
    {
        /// serialize equals expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize Equals logical expression to SerializableExpression_EqualsExpression");
        auto equalsExpressionNode = NES::Util::as<EqualsExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_EqualsExpression();
        serializeExpression(equalsExpressionNode->getLeft(), serializedExpressionNode.mutable_left());
        serializeExpression(equalsExpressionNode->getRight(), serializedExpressionNode.mutable_right());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else if (NES::Util::instanceOf<NegateExpressionNode>(expression))
    {
        /// serialize negate expression node.
        NES_TRACE("ExpressionSerializationUtil:: serialize negate logical expression to SerializableExpression_NegateExpression");
        auto equalsExpressionNode = NES::Util::as<NegateExpressionNode>(expression);
        auto serializedExpressionNode = SerializableExpression_NegateExpression();
        serializeExpression(equalsExpressionNode->child(), serializedExpressionNode.mutable_child());
        serializedExpression->mutable_details()->PackFrom(serializedExpressionNode);
    }
    else
    {
        NES_FATAL_ERROR(
            "ExpressionSerializationUtil: No serialization implemented for this logical expression node: {}", expression->toString());
        NES_NOT_IMPLEMENTED();
    }
}

ExpressionNodePtr ExpressionSerializationUtil::deserializeArithmeticalExpressions(const SerializableExpression& serializedExpression)
{
    if (serializedExpression.details().Is<SerializableExpression_AddExpression>())
    {
        /// de-serialize ADD expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as Add expression node.");
        auto serializedExpressionNode = SerializableExpression_AddExpression();
        serializedExpression.details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.left());
        auto right = deserializeExpression(serializedExpressionNode.right());
        return AddExpressionNode::create(left, right);
    }
    if (serializedExpression.details().Is<SerializableExpression_SubExpression>())
    {
        /// de-serialize SUB expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as SUB expression node.");
        auto serializedExpressionNode = SerializableExpression_SubExpression();
        serializedExpression.details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.left());
        auto right = deserializeExpression(serializedExpressionNode.right());
        return SubExpressionNode::create(left, right);
    }
    else if (serializedExpression.details().Is<SerializableExpression_MulExpression>())
    {
        /// de-serialize MUL expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as MUL expression node.");
        auto serializedExpressionNode = SerializableExpression_MulExpression();
        serializedExpression.details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.left());
        auto right = deserializeExpression(serializedExpressionNode.right());
        return MulExpressionNode::create(left, right);
    }
    else if (serializedExpression.details().Is<SerializableExpression_DivExpression>())
    {
        /// de-serialize DIV expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as DIV expression node.");
        auto serializedExpressionNode = SerializableExpression_DivExpression();
        serializedExpression.details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.left());
        auto right = deserializeExpression(serializedExpressionNode.right());
        return DivExpressionNode::create(left, right);
    }
    else if (serializedExpression.details().Is<SerializableExpression_ModExpression>())
    {
        /// de-serialize MODULO expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as MODULO expression node.");
        auto serializedExpressionNode = SerializableExpression_ModExpression();
        serializedExpression.details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.left());
        auto right = deserializeExpression(serializedExpressionNode.right());
        return ModExpressionNode::create(left, right);
    }
    else if (serializedExpression.details().Is<SerializableExpression_PowExpression>())
    {
        /// de-serialize POWER expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as POWER expression node.");
        auto serializedExpressionNode = SerializableExpression_PowExpression();
        serializedExpression.details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.left());
        auto right = deserializeExpression(serializedExpressionNode.right());
        return PowExpressionNode::create(left, right);
    }
    else if (serializedExpression.details().Is<SerializableExpression_AbsExpression>())
    {
        /// de-serialize ABS expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as ABS expression node.");
        auto serializedExpressionNode = SerializableExpression_AbsExpression();
        serializedExpression.details().UnpackTo(&serializedExpressionNode);
        auto child = deserializeExpression(serializedExpressionNode.child());
        return AbsExpressionNode::create(child);
    }
    else if (serializedExpression.details().Is<SerializableExpression_CeilExpression>())
    {
        /// de-serialize CEIL expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as CEIL expression node.");
        auto serializedExpressionNode = SerializableExpression_CeilExpression();
        serializedExpression.details().UnpackTo(&serializedExpressionNode);
        auto child = deserializeExpression(serializedExpressionNode.child());
        return CeilExpressionNode::create(child);
    }
    else if (serializedExpression.details().Is<SerializableExpression_ExpExpression>())
    {
        /// de-serialize EXP expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as EXP expression node.");
        auto serializedExpressionNode = SerializableExpression_ExpExpression();
        serializedExpression.details().UnpackTo(&serializedExpressionNode);
        auto child = deserializeExpression(serializedExpressionNode.child());
        return ExpExpressionNode::create(child);
    }
    else if (serializedExpression.details().Is<SerializableExpression_FloorExpression>())
    {
        /// de-serialize FLOOR expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as FLOOR expression node.");
        auto serializedExpressionNode = SerializableExpression_FloorExpression();
        serializedExpression.details().UnpackTo(&serializedExpressionNode);
        auto child = deserializeExpression(serializedExpressionNode.child());
        return FloorExpressionNode::create(child);
    }
    else if (serializedExpression.details().Is<SerializableExpression_RoundExpression>())
    {
        /// de-serialize ROUND expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as ROUND expression node.");
        auto serializedExpressionNode = SerializableExpression_RoundExpression();
        serializedExpression.details().UnpackTo(&serializedExpressionNode);
        auto child = deserializeExpression(serializedExpressionNode.child());
        return RoundExpressionNode::create(child);
    }
    else if (serializedExpression.details().Is<SerializableExpression_SqrtExpression>())
    {
        /// de-serialize SQRT expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize arithmetical expression as SQRT expression node.");
        auto serializedExpressionNode = SerializableExpression_SqrtExpression();
        serializedExpression.details().UnpackTo(&serializedExpressionNode);
        auto child = deserializeExpression(serializedExpressionNode.child());
        return SqrtExpressionNode::create(child);
    }
    return nullptr;
}

ExpressionNodePtr ExpressionSerializationUtil::deserializeLogicalExpressions(const SerializableExpression& serializedExpression)
{
    NES_DEBUG("ExpressionSerializationUtil:: de-serialize logical expression {}", serializedExpression.details().type_url());
    if (serializedExpression.details().Is<SerializableExpression_AndExpression>())
    {
        NES_TRACE("ExpressionSerializationUtil:: de-serialize logical expression as AND expression node.");
        /// de-serialize and expression node.
        auto serializedExpressionNode = SerializableExpression_AndExpression();
        serializedExpression.details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.left());
        auto right = deserializeExpression(serializedExpressionNode.right());
        return AndExpressionNode::create(left, right);
    }
    if (serializedExpression.details().Is<SerializableExpression_OrExpression>())
    {
        /// de-serialize or expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize logical expression as OR expression node.");
        auto serializedExpressionNode = SerializableExpression_OrExpression();
        serializedExpression.details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.left());
        auto right = deserializeExpression(serializedExpressionNode.right());
        return OrExpressionNode::create(left, right);
    }
    else if (serializedExpression.details().Is<SerializableExpression_LessExpression>())
    {
        /// de-serialize less expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize logical expression as LESS expression node.");
        auto serializedExpressionNode = SerializableExpression_LessExpression();
        serializedExpression.details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.left());
        auto right = deserializeExpression(serializedExpressionNode.right());
        return LessExpressionNode::create(left, right);
    }
    else if (serializedExpression.details().Is<SerializableExpression_LessEqualsExpression>())
    {
        /// de-serialize less equals expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize logical expression as LESS Equals expression node.");
        auto serializedExpressionNode = SerializableExpression_LessEqualsExpression();
        serializedExpression.details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.left());
        auto right = deserializeExpression(serializedExpressionNode.right());
        return LessEqualsExpressionNode::create(left, right);
    }
    else if (serializedExpression.details().Is<SerializableExpression_GreaterExpression>())
    {
        /// de-serialize greater expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize logical expression as Greater expression node.");
        auto serializedExpressionNode = SerializableExpression_GreaterExpression();
        serializedExpression.details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.left());
        auto right = deserializeExpression(serializedExpressionNode.right());
        return GreaterExpressionNode::create(left, right);
    }
    else if (serializedExpression.details().Is<SerializableExpression_GreaterEqualsExpression>())
    {
        /// de-serialize greater equals expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize logical expression as GreaterEquals expression node.");
        auto serializedExpressionNode = SerializableExpression_GreaterEqualsExpression();
        serializedExpression.details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.left());
        auto right = deserializeExpression(serializedExpressionNode.right());
        return GreaterEqualsExpressionNode::create(left, right);
    }
    else if (serializedExpression.details().Is<SerializableExpression_EqualsExpression>())
    {
        /// de-serialize equals expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize logical expression as Equals expression node.");
        auto serializedExpressionNode = SerializableExpression_EqualsExpression();
        serializedExpression.details().UnpackTo(&serializedExpressionNode);
        auto left = deserializeExpression(serializedExpressionNode.left());
        auto right = deserializeExpression(serializedExpressionNode.right());
        return EqualsExpressionNode::create(left, right);
    }
    else if (serializedExpression.details().Is<SerializableExpression_NegateExpression>())
    {
        /// de-serialize negate expression node.
        NES_TRACE("ExpressionSerializationUtil:: de-serialize logical expression as Negate expression node.");
        auto serializedExpressionNode = SerializableExpression_NegateExpression();
        serializedExpression.details().UnpackTo(&serializedExpressionNode);
        auto child = deserializeExpression(serializedExpressionNode.child());
        return NegateExpressionNode::create(child);
    }
    return nullptr;
}

} /// namespace NES
