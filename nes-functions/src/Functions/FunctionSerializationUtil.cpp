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

#include <Functions/ArithmeticalFunctions/AbsFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/AddFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/CeilFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/DivFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/ExpFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/FloorFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/ModFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/MulFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/PowFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/RoundFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/SqrtFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/SubFunctionNode.hpp>
#include <Functions/CaseFunctionNode.hpp>
#include <Functions/ConstantValueFunctionNode.hpp>
#include <Functions/FunctionNode.hpp>
#include <Functions/FunctionSerializationUtil.hpp>
#include <Functions/FieldAccessFunctionNode.hpp>
#include <Functions/FieldAssignmentFunctionNode.hpp>
#include <Functions/FieldRenameFunctionNode.hpp>
#include <Functions/Functions/FunctionFunctionNode.hpp>
#include <Functions/LogicalFunctions/AndFunctionNode.hpp>
#include <Functions/LogicalFunctions/EqualsFunctionNode.hpp>
#include <Functions/LogicalFunctions/GreaterEqualsFunctionNode.hpp>
#include <Functions/LogicalFunctions/GreaterFunctionNode.hpp>
#include <Functions/LogicalFunctions/LessEqualsFunctionNode.hpp>
#include <Functions/LogicalFunctions/LessFunctionNode.hpp>
#include <Functions/LogicalFunctions/NegateFunctionNode.hpp>
#include <Functions/LogicalFunctions/OrFunctionNode.hpp>
#include <Functions/WhenFunctionNode.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <SerializableFunction.pb.h>

namespace NES
{

SerializableFunction*
FunctionSerializationUtil::serializeFunction(const FunctionNodePtr& function, SerializableFunction* serializedFunction)
{
    NES_DEBUG("FunctionSerializationUtil:: serialize function {}", function->toString());
    /// serialize function node depending on its type.
    if (function->instanceOf<LogicalFunctionNode>())
    {
        /// serialize logical function
        serializeLogicalFunctions(function, serializedFunction);
    }
    else if (function->instanceOf<ArithmeticalFunctionNode>())
    {
        /// serialize arithmetical functions
        serializeArithmeticalFunctions(function, serializedFunction);
    }
    else if (function->instanceOf<ConstantValueFunctionNode>())
    {
        /// serialize constant value function node.
        NES_TRACE("FunctionSerializationUtil:: serialize constant value function node.");
        auto constantValueFunction = function->as<ConstantValueFunctionNode>();
        auto value = constantValueFunction->getConstantValue();
        /// serialize value
        auto serializedConstantValue = SerializableFunction_ConstantValueFunction();
        DataTypeSerializationUtil::serializeDataValue(value, serializedConstantValue.mutable_value());
        serializedFunction->mutable_details()->PackFrom(serializedConstantValue);
    }
    else if (function->instanceOf<FieldAccessFunctionNode>())
    {
        /// serialize field access function node
        NES_TRACE("FunctionSerializationUtil:: serialize field access function node.");
        auto fieldAccessFunction = function->as<FieldAccessFunctionNode>();
        auto serializedFieldAccessFunction = SerializableFunction_FieldAccessFunction();
        serializedFieldAccessFunction.set_fieldname(fieldAccessFunction->getFieldName());
        serializedFunction->mutable_details()->PackFrom(serializedFieldAccessFunction);
    }
    else if (function->instanceOf<FieldRenameFunctionNode>())
    {
        /// serialize field rename function node
        NES_TRACE("FunctionSerializationUtil:: serialize field rename function node.");
        auto fieldRenameFunction = function->as<FieldRenameFunctionNode>();
        auto serializedFieldRenameFunction = SerializableFunction_FieldRenameFunction();
        serializeFunction(
            fieldRenameFunction->getOriginalField(), serializedFieldRenameFunction.mutable_originalfieldaccessfunction());
        serializedFieldRenameFunction.set_newfieldname(fieldRenameFunction->getNewFieldName());
        serializedFunction->mutable_details()->PackFrom(serializedFieldRenameFunction);
    }
    else if (function->instanceOf<FieldAssignmentFunctionNode>())
    {
        /// serialize field assignment function node.
        NES_TRACE("FunctionSerializationUtil:: serialize field assignment function node.");
        auto fieldAssignmentFunctionNode = function->as<FieldAssignmentFunctionNode>();
        auto serializedFieldAssignmentFunction = SerializableFunction_FieldAssignmentFunction();
        auto* serializedFieldAccessFunction = serializedFieldAssignmentFunction.mutable_field();
        serializedFieldAccessFunction->set_fieldname(fieldAssignmentFunctionNode->getField()->getFieldName());
        DataTypeSerializationUtil::serializeDataType(
            fieldAssignmentFunctionNode->getField()->getStamp(), serializedFieldAccessFunction->mutable_type());
        /// serialize assignment function
        serializeFunction(fieldAssignmentFunctionNode->getAssignment(), serializedFieldAssignmentFunction.mutable_assignment());
        serializedFunction->mutable_details()->PackFrom(serializedFieldAssignmentFunction);
    }
    else if (function->instanceOf<WhenFunctionNode>())
    {
        /// serialize when function node.
        NES_TRACE("FunctionSerializationUtil:: serialize when function {}.", function->toString());
        auto whenFunctionNode = function->as<WhenFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_WhenFunction();
        serializeFunction(whenFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(whenFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<CaseFunctionNode>())
    {
        /// serialize case function node.
        NES_TRACE("FunctionSerializationUtil:: serialize case function {}.", function->toString());
        auto caseFunctionNode = function->as<CaseFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_CaseFunction();
        for (const auto& elem : caseFunctionNode->getWhenChildren())
        {
            serializeFunction(elem, serializedFunctionNode.add_left());
        }
        serializeFunction(caseFunctionNode->getDefaultExp(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<FunctionFunction>())
    {
        /// serialize negate function node.
        NES_TRACE("FunctionSerializationUtil:: serialize function logical function to SerializableFunction_FunctionFunction");
        auto functionFunctionNode = function->as<FunctionFunction>();
        auto serializedFunctionNode = SerializableFunction_FunctionFunction();
        serializedFunctionNode.set_functionname(functionFunctionNode->getFunctionName());
        for (const auto& child : functionFunctionNode->getChildren())
        {
            auto argument = serializedFunctionNode.add_arguments();
            serializeFunction(child->as<FunctionNode>(), argument);
        }
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else
    {
        NES_FATAL_ERROR("FunctionSerializationUtil: could not serialize this function: {}", function->toString());
    }

    DataTypeSerializationUtil::serializeDataType(function->getStamp(), serializedFunction->mutable_stamp());
    NES_DEBUG("FunctionSerializationUtil:: serialize function node to {}", serializedFunction->mutable_details()->type_url());
    return serializedFunction;
}

FunctionNodePtr FunctionSerializationUtil::deserializeFunction(const SerializableFunction& serializedFunction)
{
    NES_DEBUG("FunctionSerializationUtil:: deserialize function {}", serializedFunction.details().type_url());
    /// de-serialize function
    /// 1. check if the serialized function is a logical function
    auto functionNodePtr = deserializeLogicalFunctions(serializedFunction);
    /// 2. if the function was not de-serialized then try if it's an arithmetical function
    if (!functionNodePtr)
    {
        functionNodePtr = deserializeArithmeticalFunctions(serializedFunction);
    }
    /// 3. if the function was not de-serialized try remaining function types
    if (!functionNodePtr)
    {
        if (serializedFunction.details().Is<SerializableFunction_ConstantValueFunction>())
        {
            /// de-serialize constant value function node.
            NES_TRACE("FunctionSerializationUtil:: de-serialize function as Constant Value function node.");
            auto serializedConstantValue = SerializableFunction_ConstantValueFunction();
            serializedFunction.details().UnpackTo(&serializedConstantValue);
            auto valueType = DataTypeSerializationUtil::deserializeDataValue(serializedConstantValue.value());
            functionNodePtr = ConstantValueFunctionNode::create(valueType);
        }
        else if (serializedFunction.details().Is<SerializableFunction_FieldAccessFunction>())
        {
            /// de-serialize field access function node.
            NES_TRACE("FunctionSerializationUtil:: de-serialize function as FieldAccess function node.");
            SerializableFunction_FieldAccessFunction serializedFieldAccessFunction;
            serializedFunction.details().UnpackTo(&serializedFieldAccessFunction);
            const auto& name = serializedFieldAccessFunction.fieldname();
            functionNodePtr = FieldAccessFunctionNode::create(name);
        }
        else if (serializedFunction.details().Is<SerializableFunction_FieldRenameFunction>())
        {
            /// de-serialize field rename function node.
            NES_TRACE("FunctionSerializationUtil:: de-serialize function as Field Rename function node.");
            SerializableFunction_FieldRenameFunction serializedFieldRenameFunction;
            serializedFunction.details().UnpackTo(&serializedFieldRenameFunction);
            auto originalFieldAccessFunction = deserializeFunction(serializedFieldRenameFunction.originalfieldaccessfunction());
            if (!originalFieldAccessFunction->instanceOf<FieldAccessFunctionNode>())
            {
                NES_FATAL_ERROR(
                    "FunctionSerializationUtil: the original field access function "
                    "should be of type FieldAccessFunctionNode, but was a {}",
                    originalFieldAccessFunction->toString());
            }
            const auto& newFieldName = serializedFieldRenameFunction.newfieldname();
            functionNodePtr
                = FieldRenameFunctionNode::create(originalFieldAccessFunction->as<FieldAccessFunctionNode>(), newFieldName);
        }
        else if (serializedFunction.details().Is<SerializableFunction_FieldAssignmentFunction>())
        {
            /// de-serialize field read function node.
            NES_TRACE("FunctionSerializationUtil:: de-serialize function as FieldAssignment function node.");
            SerializableFunction_FieldAssignmentFunction serializedFieldAccessFunction;
            serializedFunction.details().UnpackTo(&serializedFieldAccessFunction);
            const auto* field = serializedFieldAccessFunction.mutable_field();
            auto fieldStamp = DataTypeSerializationUtil::deserializeDataType(field->type());
            auto fieldAccessNode = FieldAccessFunctionNode::create(fieldStamp, field->fieldname());
            auto fieldAssignmentFunction = deserializeFunction(serializedFieldAccessFunction.assignment());
            functionNodePtr
                = FieldAssignmentFunctionNode::create(fieldAccessNode->as<FieldAccessFunctionNode>(), fieldAssignmentFunction);
        }
        else if (serializedFunction.details().Is<SerializableFunction_FunctionFunction>())
        {
            /// de-serialize field read function node.
            NES_TRACE("FunctionSerializationUtil:: de-serialize function as function function node.");
            SerializableFunction_FunctionFunction functionFunction;
            serializedFunction.details().UnpackTo(&functionFunction);
            const auto& functionName = functionFunction.functionname();
            std::vector<FunctionNodePtr> arguments;
            for (const auto& arg : functionFunction.arguments())
            {
                arguments.emplace_back(deserializeFunction(arg));
            }
            auto resultStamp = DataTypeSerializationUtil::deserializeDataType(serializedFunction.stamp());
            auto functionFunctionNode = FunctionFunction::create(resultStamp, functionName, arguments);
            functionNodePtr = functionFunctionNode;
        }
        else if (serializedFunction.details().Is<SerializableFunction_WhenFunction>())
        {
            /// de-serialize WHEN function node.
            NES_TRACE("FunctionSerializationUtil:: de-serialize function as When function node.");
            auto serializedFunctionNode = SerializableFunction_WhenFunction();
            serializedFunction.details().UnpackTo(&serializedFunctionNode);
            auto left = deserializeFunction(serializedFunctionNode.left());
            auto right = deserializeFunction(serializedFunctionNode.right());
            return WhenFunctionNode::create(left, right);
        }
        else if (serializedFunction.details().Is<SerializableFunction_CaseFunction>())
        {
            /// de-serialize CASE function node.
            NES_TRACE("FunctionSerializationUtil:: de-serialize function as Case function node.");
            auto serializedFunctionNode = SerializableFunction_CaseFunction();
            serializedFunction.details().UnpackTo(&serializedFunctionNode);
            std::vector<FunctionNodePtr> leftExps;

            ///todo: deserialization might be possible more efficiently
            for (int i = 0; i < serializedFunctionNode.left_size(); i++)
            {
                const auto& leftNode = serializedFunctionNode.left(i);
                leftExps.push_back(deserializeFunction(leftNode));
            }
            auto right = deserializeFunction(serializedFunctionNode.right());
            return CaseFunctionNode::create(leftExps, right);
        }
        else
        {
            NES_FATAL_ERROR("FunctionSerializationUtil: could not de-serialize this function");
        }
    }

    if (!functionNodePtr)
    {
        NES_FATAL_ERROR("FunctionSerializationUtil:: fatal error during de-serialization. The function node must not be null");
    }

    /// deserialize function stamp
    auto stamp = DataTypeSerializationUtil::deserializeDataType(serializedFunction.stamp());
    functionNodePtr->setStamp(stamp);
    NES_DEBUG("FunctionSerializationUtil:: deserialized function node to the following node: {}", functionNodePtr->toString());
    return functionNodePtr;
}

void FunctionSerializationUtil::serializeArithmeticalFunctions(
    const FunctionNodePtr& function, SerializableFunction* serializedFunction)
{
    NES_DEBUG("FunctionSerializationUtil:: serialize arithmetical function {}", function->toString());
    if (function->instanceOf<AddFunctionNode>())
    {
        /// serialize add function node.
        NES_TRACE("FunctionSerializationUtil:: serialize ADD arithmetical function to SerializableFunction_AddFunction");
        auto addFunctionNode = function->as<AddFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_AddFunction();
        serializeFunction(addFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(addFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<SubFunctionNode>())
    {
        /// serialize sub function node.
        NES_TRACE("FunctionSerializationUtil:: serialize SUB arithmetical function to SerializableFunction_SubFunction");
        auto subFunctionNode = function->as<SubFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_SubFunction();
        serializeFunction(subFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(subFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<MulFunctionNode>())
    {
        /// serialize mul function node.
        NES_TRACE("FunctionSerializationUtil:: serialize MUL arithmetical function to SerializableFunction_MulFunction");
        auto mulFunctionNode = function->as<MulFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_MulFunction();
        serializeFunction(mulFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(mulFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<DivFunctionNode>())
    {
        /// serialize div function node.
        NES_TRACE("FunctionSerializationUtil:: serialize DIV arithmetical function to SerializableFunction_DivFunction");
        auto divFunctionNode = function->as<DivFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_DivFunction();
        serializeFunction(divFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(divFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<ModFunctionNode>())
    {
        /// serialize mod function node.
        NES_TRACE("FunctionSerializationUtil:: serialize MODULO arithmetical function to SerializableFunction_PowFunction");
        auto modFunctionNode = function->as<ModFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_ModFunction();
        serializeFunction(modFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(modFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<PowFunctionNode>())
    {
        /// serialize pow function node.
        NES_TRACE("FunctionSerializationUtil:: serialize POWER arithmetical function to SerializableFunction_PowFunction");
        auto powFunctionNode = function->as<PowFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_PowFunction();
        serializeFunction(powFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(powFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<AbsFunctionNode>())
    {
        /// serialize abs function node.
        NES_TRACE("FunctionSerializationUtil:: serialize ABS arithmetical function to SerializableFunction_AbsFunction");
        auto absFunctionNode = function->as<AbsFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_AbsFunction();
        serializeFunction(absFunctionNode->child(), serializedFunctionNode.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<CeilFunctionNode>())
    {
        /// serialize ceil function node.
        NES_TRACE("FunctionSerializationUtil:: serialize CEIL arithmetical function to SerializableFunction_CeilFunction");
        auto ceilFunctionNode = function->as<CeilFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_CeilFunction();
        serializeFunction(ceilFunctionNode->child(), serializedFunctionNode.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<ExpFunctionNode>())
    {
        /// serialize exp function node.
        NES_TRACE("FunctionSerializationUtil:: serialize EXP arithmetical function to SerializableFunction_ExpFunction");
        auto expFunctionNode = function->as<ExpFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_ExpFunction();
        serializeFunction(expFunctionNode->child(), serializedFunctionNode.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<FloorFunctionNode>())
    {
        /// serialize floor function node.
        NES_TRACE("FunctionSerializationUtil:: serialize FLOOR arithmetical function to SerializableFunction_FloorFunction");
        auto floorFunctionNode = function->as<FloorFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_FloorFunction();
        serializeFunction(floorFunctionNode->child(), serializedFunctionNode.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<RoundFunctionNode>())
    {
        /// serialize round function node.
        NES_TRACE("FunctionSerializationUtil:: serialize ROUND arithmetical function to SerializableFunction_RoundFunction");
        auto roundFunctionNode = function->as<RoundFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_RoundFunction();
        serializeFunction(roundFunctionNode->child(), serializedFunctionNode.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<SqrtFunctionNode>())
    {
        /// serialize sqrt function node.
        NES_TRACE("FunctionSerializationUtil:: serialize SQRT arithmetical function to SerializableFunction_SqrtFunction");
        auto sqrtFunctionNode = function->as<SqrtFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_SqrtFunction();
        serializeFunction(sqrtFunctionNode->child(), serializedFunctionNode.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else
    {
        NES_FATAL_ERROR(
            "TranslateToLegacyPhase: No serialization implemented for this arithmetical function node: {}", function->toString());
        NES_NOT_IMPLEMENTED();
    }
}

void FunctionSerializationUtil::serializeLogicalFunctions(
    const FunctionNodePtr& function, SerializableFunction* serializedFunction)
{
    NES_DEBUG("FunctionSerializationUtil:: serialize logical function {}", function->toString());
    if (function->instanceOf<AndFunctionNode>())
    {
        /// serialize and function node.
        NES_TRACE("FunctionSerializationUtil:: serialize AND logical function to SerializableFunction_AndFunction");
        auto andFunctionNode = function->as<AndFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_AndFunction();
        serializeFunction(andFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(andFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<OrFunctionNode>())
    {
        /// serialize or function node.
        NES_TRACE("FunctionSerializationUtil:: serialize OR logical function to SerializableFunction_OrFunction");
        auto orFunctionNode = function->as<OrFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_OrFunction();
        serializeFunction(orFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(orFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<LessFunctionNode>())
    {
        /// serialize less function node.
        NES_TRACE("FunctionSerializationUtil:: serialize Less logical function to SerializableFunction_LessFunction");
        auto lessFunctionNode = function->as<LessFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_LessFunction();
        serializeFunction(lessFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(lessFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<LessEqualsFunctionNode>())
    {
        /// serialize less equals function node.
        NES_TRACE("FunctionSerializationUtil:: serialize Less Equals logical function to "
                  "SerializableFunction_LessEqualsFunction");
        auto lessEqualsFunctionNode = function->as<LessEqualsFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_LessEqualsFunction();
        serializeFunction(lessEqualsFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(lessEqualsFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<GreaterFunctionNode>())
    {
        /// serialize greater function node.
        NES_TRACE("FunctionSerializationUtil:: serialize Greater logical function to SerializableFunction_GreaterFunction");
        auto greaterFunctionNode = function->as<GreaterFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_GreaterFunction();
        serializeFunction(greaterFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(greaterFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<GreaterEqualsFunctionNode>())
    {
        /// serialize greater equals function node.
        NES_TRACE("FunctionSerializationUtil:: serialize Greater Equals logical function to "
                  "SerializableFunction_GreaterEqualsFunction");
        auto greaterEqualsFunctionNode = function->as<GreaterEqualsFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_GreaterEqualsFunction();
        serializeFunction(greaterEqualsFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(greaterEqualsFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<EqualsFunctionNode>())
    {
        /// serialize equals function node.
        NES_TRACE("FunctionSerializationUtil:: serialize Equals logical function to SerializableFunction_EqualsFunction");
        auto equalsFunctionNode = function->as<EqualsFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_EqualsFunction();
        serializeFunction(equalsFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(equalsFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<NegateFunctionNode>())
    {
        /// serialize negate function node.
        NES_TRACE("FunctionSerializationUtil:: serialize negate logical function to SerializableFunction_NegateFunction");
        auto equalsFunctionNode = function->as<NegateFunctionNode>();
        auto serializedFunctionNode = SerializableFunction_NegateFunction();
        serializeFunction(equalsFunctionNode->child(), serializedFunctionNode.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else
    {
        NES_FATAL_ERROR(
            "FunctionSerializationUtil: No serialization implemented for this logical function node: {}", function->toString());
        NES_NOT_IMPLEMENTED();
    }
}

FunctionNodePtr FunctionSerializationUtil::deserializeArithmeticalFunctions(const SerializableFunction& serializedFunction)
{
    if (serializedFunction.details().Is<SerializableFunction_AddFunction>())
    {
        /// de-serialize ADD function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as Add function node.");
        auto serializedFunctionNode = SerializableFunction_AddFunction();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return AddFunctionNode::create(left, right);
    }
    if (serializedFunction.details().Is<SerializableFunction_SubFunction>())
    {
        /// de-serialize SUB function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as SUB function node.");
        auto serializedFunctionNode = SerializableFunction_SubFunction();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return SubFunctionNode::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_MulFunction>())
    {
        /// de-serialize MUL function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as MUL function node.");
        auto serializedFunctionNode = SerializableFunction_MulFunction();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return MulFunctionNode::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_DivFunction>())
    {
        /// de-serialize DIV function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as DIV function node.");
        auto serializedFunctionNode = SerializableFunction_DivFunction();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return DivFunctionNode::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_ModFunction>())
    {
        /// de-serialize MODULO function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as MODULO function node.");
        auto serializedFunctionNode = SerializableFunction_ModFunction();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return ModFunctionNode::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_PowFunction>())
    {
        /// de-serialize POWER function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as POWER function node.");
        auto serializedFunctionNode = SerializableFunction_PowFunction();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return PowFunctionNode::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_AbsFunction>())
    {
        /// de-serialize ABS function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as ABS function node.");
        auto serializedFunctionNode = SerializableFunction_AbsFunction();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto child = deserializeFunction(serializedFunctionNode.child());
        return AbsFunctionNode::create(child);
    }
    else if (serializedFunction.details().Is<SerializableFunction_CeilFunction>())
    {
        /// de-serialize CEIL function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as CEIL function node.");
        auto serializedFunctionNode = SerializableFunction_CeilFunction();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto child = deserializeFunction(serializedFunctionNode.child());
        return CeilFunctionNode::create(child);
    }
    else if (serializedFunction.details().Is<SerializableFunction_ExpFunction>())
    {
        /// de-serialize EXP function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as EXP function node.");
        auto serializedFunctionNode = SerializableFunction_ExpFunction();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto child = deserializeFunction(serializedFunctionNode.child());
        return ExpFunctionNode::create(child);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FloorFunction>())
    {
        /// de-serialize FLOOR function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as FLOOR function node.");
        auto serializedFunctionNode = SerializableFunction_FloorFunction();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto child = deserializeFunction(serializedFunctionNode.child());
        return FloorFunctionNode::create(child);
    }
    else if (serializedFunction.details().Is<SerializableFunction_RoundFunction>())
    {
        /// de-serialize ROUND function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as ROUND function node.");
        auto serializedFunctionNode = SerializableFunction_RoundFunction();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto child = deserializeFunction(serializedFunctionNode.child());
        return RoundFunctionNode::create(child);
    }
    else if (serializedFunction.details().Is<SerializableFunction_SqrtFunction>())
    {
        /// de-serialize SQRT function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as SQRT function node.");
        auto serializedFunctionNode = SerializableFunction_SqrtFunction();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto child = deserializeFunction(serializedFunctionNode.child());
        return SqrtFunctionNode::create(child);
    }
    return nullptr;
}

FunctionNodePtr FunctionSerializationUtil::deserializeLogicalFunctions(const SerializableFunction& serializedFunction)
{
    NES_DEBUG("FunctionSerializationUtil:: de-serialize logical function {}", serializedFunction.details().type_url());
    if (serializedFunction.details().Is<SerializableFunction_AndFunction>())
    {
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as AND function node.");
        /// de-serialize and function node.
        auto serializedFunctionNode = SerializableFunction_AndFunction();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return AndFunctionNode::create(left, right);
    }
    if (serializedFunction.details().Is<SerializableFunction_OrFunction>())
    {
        /// de-serialize or function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as OR function node.");
        auto serializedFunctionNode = SerializableFunction_OrFunction();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return OrFunctionNode::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_LessFunction>())
    {
        /// de-serialize less function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as LESS function node.");
        auto serializedFunctionNode = SerializableFunction_LessFunction();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return LessFunctionNode::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_LessEqualsFunction>())
    {
        /// de-serialize less equals function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as LESS Equals function node.");
        auto serializedFunctionNode = SerializableFunction_LessEqualsFunction();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return LessEqualsFunctionNode::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_GreaterFunction>())
    {
        /// de-serialize greater function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as Greater function node.");
        auto serializedFunctionNode = SerializableFunction_GreaterFunction();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return GreaterFunctionNode::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_GreaterEqualsFunction>())
    {
        /// de-serialize greater equals function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as GreaterEquals function node.");
        auto serializedFunctionNode = SerializableFunction_GreaterEqualsFunction();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return GreaterEqualsFunctionNode::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_EqualsFunction>())
    {
        /// de-serialize equals function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as Equals function node.");
        auto serializedFunctionNode = SerializableFunction_EqualsFunction();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return EqualsFunctionNode::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_NegateFunction>())
    {
        /// de-serialize negate function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as Negate function node.");
        auto serializedFunctionNode = SerializableFunction_NegateFunction();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto child = deserializeFunction(serializedFunctionNode.child());
        return NegateFunctionNode::create(child);
    }
    return nullptr;
}

} /// namespace NES
