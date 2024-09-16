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

#include <Functions/ArithmeticalFunctions/NodeFunctionAbs.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionAdd.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionCeil.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionDiv.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionExp.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionFloor.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionMod.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionMul.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionPow.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionRound.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionSqrt.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionSub.hpp>
#include <Functions/FunctionSerializationUtil.hpp>
#include <Functions/LogicalFunctions/NodeFunctionAnd.hpp>
#include <Functions/LogicalFunctions/NodeFunctionEquals.hpp>
#include <Functions/LogicalFunctions/NodeFunctionGreater.hpp>
#include <Functions/LogicalFunctions/NodeFunctionGreaterEquals.hpp>
#include <Functions/LogicalFunctions/NodeFunctionLess.hpp>
#include <Functions/LogicalFunctions/NodeFunctionLessEquals.hpp>
#include <Functions/LogicalFunctions/NodeFunctionNegate.hpp>
#include <Functions/LogicalFunctions/NodeFunctionOr.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionCase.hpp>
#include <Functions/NodeFunctionConstantValue.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Functions/NodeFunctionFieldRename.hpp>
#include <Functions/NodeFunctionWhen.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <SerializableFunction.pb.h>

namespace NES
{

SerializableFunction*
FunctionSerializationUtil::serializeFunction(const NodeFunctionPtr& function, SerializableFunction* serializedFunction)
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
    else if (function->instanceOf<NodeFunctionConstantValue>())
    {
        /// serialize constant value function node.
        NES_TRACE("FunctionSerializationUtil:: serialize constant value function node.");
        auto constantValueFunction = function->as<NodeFunctionConstantValue>();
        auto value = constantValueFunction->getConstantValue();
        /// serialize value
        auto serializedConstantValue = SerializableFunction_FunctionConstantValue();
        DataTypeSerializationUtil::serializeDataValue(value, serializedConstantValue.mutable_value());
        serializedFunction->mutable_details()->PackFrom(serializedConstantValue);
    }
    else if (function->instanceOf<NodeFunctionFieldAccess>())
    {
        /// serialize field access function node
        NES_TRACE("FunctionSerializationUtil:: serialize field access function node.");
        auto fieldAccessFunction = function->as<NodeFunctionFieldAccess>();
        auto serializedFieldAccessFunction = SerializableFunction_FunctionFieldAccess();
        serializedFieldAccessFunction.set_fieldname(fieldAccessFunction->getFieldName());
        serializedFunction->mutable_details()->PackFrom(serializedFieldAccessFunction);
    }
    else if (function->instanceOf<NodeFunctionFieldRename>())
    {
        /// serialize field rename function node
        NES_TRACE("FunctionSerializationUtil:: serialize field rename function node.");
        auto fieldRenameFunction = function->as<NodeFunctionFieldRename>();
        auto serializedFieldRenameFunction = SerializableFunction_FunctionFieldRename();
        serializeFunction(fieldRenameFunction->getOriginalField(), serializedFieldRenameFunction.mutable_functionoriginalfieldaccess());
        serializedFieldRenameFunction.set_newfieldname(fieldRenameFunction->getNewFieldName());
        serializedFunction->mutable_details()->PackFrom(serializedFieldRenameFunction);
    }
    else if (function->instanceOf<NodeFunctionFieldAssignment>())
    {
        /// serialize field assignment function node.
        NES_TRACE("FunctionSerializationUtil:: serialize field assignment function node.");
        auto fieldAssignmentFunctionNode = function->as<NodeFunctionFieldAssignment>();
        auto serializedFieldAssignmentFunction = SerializableFunction_FunctionFieldAssignment();
        auto* serializedFieldAccessFunction = serializedFieldAssignmentFunction.mutable_field();
        serializedFieldAccessFunction->set_fieldname(fieldAssignmentFunctionNode->getField()->getFieldName());
        DataTypeSerializationUtil::serializeDataType(
            fieldAssignmentFunctionNode->getField()->getStamp(), serializedFieldAccessFunction->mutable_type());
        /// serialize assignment function
        serializeFunction(fieldAssignmentFunctionNode->getAssignment(), serializedFieldAssignmentFunction.mutable_assignment());
        serializedFunction->mutable_details()->PackFrom(serializedFieldAssignmentFunction);
    }
    else if (function->instanceOf<NodeFunctionWhen>())
    {
        /// serialize when function node.
        NES_TRACE("FunctionSerializationUtil:: serialize when function {}.", function->toString());
        auto whenFunctionNode = function->as<NodeFunctionWhen>();
        auto serializedFunctionNode = SerializableFunction_FunctionWhen();
        serializeFunction(whenFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(whenFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<NodeFunctionCase>())
    {
        /// serialize case function node.
        NES_TRACE("FunctionSerializationUtil:: serialize case function {}.", function->toString());
        auto caseFunctionNode = function->as<NodeFunctionCase>();
        auto serializedFunctionNode = SerializableFunction_FunctionCase();
        for (const auto& elem : caseFunctionNode->getWhenChildren())
        {
            serializeFunction(elem, serializedFunctionNode.add_left());
        }
        serializeFunction(caseFunctionNode->getDefaultExp(), serializedFunctionNode.mutable_right());
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

NodeFunctionPtr FunctionSerializationUtil::deserializeFunction(const SerializableFunction& serializedFunction)
{
    NES_DEBUG("FunctionSerializationUtil:: deserialize function {}", serializedFunction.details().type_url());
    /// de-serialize function
    /// 1. check if the serialized function is a logical function
    auto nodeFunctionPtr = deserializeLogicalFunctions(serializedFunction);
    /// 2. if the function was not de-serialized then try if it's an arithmetical function
    if (!nodeFunctionPtr)
    {
        nodeFunctionPtr = deserializeArithmeticalFunctions(serializedFunction);
    }
    /// 3. if the function was not de-serialized try remaining function types
    if (!nodeFunctionPtr)
    {
        if (serializedFunction.details().Is<SerializableFunction_FunctionConstantValue>())
        {
            /// de-serialize constant value function node.
            NES_TRACE("FunctionSerializationUtil:: de-serialize function as Constant Value function node.");
            auto serializedConstantValue = SerializableFunction_FunctionConstantValue();
            serializedFunction.details().UnpackTo(&serializedConstantValue);
            auto valueType = DataTypeSerializationUtil::deserializeDataValue(serializedConstantValue.value());
            nodeFunctionPtr = NodeFunctionConstantValue::create(valueType);
        }
        else if (serializedFunction.details().Is<SerializableFunction_FunctionFieldAccess>())
        {
            /// de-serialize field access function node.
            NES_TRACE("FunctionSerializationUtil:: de-serialize function as FieldAccess function node.");
            SerializableFunction_FunctionFieldAccess serializedFieldAccessFunction;
            serializedFunction.details().UnpackTo(&serializedFieldAccessFunction);
            const auto& name = serializedFieldAccessFunction.fieldname();
            nodeFunctionPtr = NodeFunctionFieldAccess::create(name);
        }
        else if (serializedFunction.details().Is<SerializableFunction_FunctionFieldRename>())
        {
            /// de-serialize field rename function node.
            NES_TRACE("FunctionSerializationUtil:: de-serialize function as Field Rename function node.");
            SerializableFunction_FunctionFieldRename serializedFieldRenameFunction;
            serializedFunction.details().UnpackTo(&serializedFieldRenameFunction);
            auto originalFieldAccessFunction = deserializeFunction(serializedFieldRenameFunction.functionoriginalfieldaccess());
            if (!originalFieldAccessFunction->instanceOf<NodeFunctionFieldAccess>())
            {
                NES_FATAL_ERROR(
                    "FunctionSerializationUtil: the original field access function "
                    "should be of type NodeFunctionFieldAccess, but was a {}",
                    originalFieldAccessFunction->toString());
            }
            const auto& newFieldName = serializedFieldRenameFunction.newfieldname();
            nodeFunctionPtr = NodeFunctionFieldRename::create(originalFieldAccessFunction->as<NodeFunctionFieldAccess>(), newFieldName);
        }
        else if (serializedFunction.details().Is<SerializableFunction_FunctionFieldAssignment>())
        {
            /// de-serialize field read function node.
            NES_TRACE("FunctionSerializationUtil:: de-serialize function as FieldAssignment function node.");
            SerializableFunction_FunctionFieldAssignment serializedFieldAccessFunction;
            serializedFunction.details().UnpackTo(&serializedFieldAccessFunction);
            const auto* field = serializedFieldAccessFunction.mutable_field();
            auto fieldStamp = DataTypeSerializationUtil::deserializeDataType(field->type());
            auto fieldAccessNode = NodeFunctionFieldAccess::create(fieldStamp, field->fieldname());
            auto fieldAssignmentFunction = deserializeFunction(serializedFieldAccessFunction.assignment());
            nodeFunctionPtr = NodeFunctionFieldAssignment::create(fieldAccessNode->as<NodeFunctionFieldAccess>(), fieldAssignmentFunction);
        }
        else if (serializedFunction.details().Is<SerializableFunction_FunctionWhen>())
        {
            /// de-serialize WHEN function node.
            NES_TRACE("FunctionSerializationUtil:: de-serialize function as When function node.");
            auto serializedFunctionNode = SerializableFunction_FunctionWhen();
            serializedFunction.details().UnpackTo(&serializedFunctionNode);
            auto left = deserializeFunction(serializedFunctionNode.left());
            auto right = deserializeFunction(serializedFunctionNode.right());
            return NodeFunctionWhen::create(left, right);
        }
        else if (serializedFunction.details().Is<SerializableFunction_FunctionCase>())
        {
            /// de-serialize CASE function node.
            NES_TRACE("FunctionSerializationUtil:: de-serialize function as Case function node.");
            auto serializedFunctionNode = SerializableFunction_FunctionCase();
            serializedFunction.details().UnpackTo(&serializedFunctionNode);
            std::vector<NodeFunctionPtr> leftExps;

            ///todo: deserialization might be possible more efficiently
            for (int i = 0; i < serializedFunctionNode.left_size(); i++)
            {
                const auto& leftNode = serializedFunctionNode.left(i);
                leftExps.push_back(deserializeFunction(leftNode));
            }
            auto right = deserializeFunction(serializedFunctionNode.right());
            return NodeFunctionCase::create(leftExps, right);
        }
        else
        {
            NES_FATAL_ERROR("FunctionSerializationUtil: could not de-serialize this function");
        }
    }

    if (!nodeFunctionPtr)
    {
        NES_FATAL_ERROR("FunctionSerializationUtil:: fatal error during de-serialization. The function node must not be null");
    }

    /// deserialize function stamp
    auto stamp = DataTypeSerializationUtil::deserializeDataType(serializedFunction.stamp());
    nodeFunctionPtr->setStamp(stamp);
    NES_DEBUG("FunctionSerializationUtil:: deserialized function node to the following node: {}", nodeFunctionPtr->toString());
    return nodeFunctionPtr;
}

void FunctionSerializationUtil::serializeArithmeticalFunctions(const NodeFunctionPtr& function, SerializableFunction* serializedFunction)
{
    NES_DEBUG("FunctionSerializationUtil:: serialize arithmetical function {}", function->toString());
    if (function->instanceOf<NodeFunctionAdd>())
    {
        /// serialize add function node.
        NES_TRACE("FunctionSerializationUtil:: serialize ADD arithmetical function to SerializableFunction_FunctionAdd");
        auto addFunctionNode = function->as<NodeFunctionAdd>();
        auto serializedFunctionNode = SerializableFunction_FunctionAdd();
        serializeFunction(addFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(addFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<NodeFunctionSub>())
    {
        /// serialize sub function node.
        NES_TRACE("FunctionSerializationUtil:: serialize SUB arithmetical function to SerializableFunction_FunctionSub");
        auto subFunctionNode = function->as<NodeFunctionSub>();
        auto serializedFunctionNode = SerializableFunction_FunctionSub();
        serializeFunction(subFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(subFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<NodeFunctionMul>())
    {
        /// serialize mul function node.
        NES_TRACE("FunctionSerializationUtil:: serialize MUL arithmetical function to SerializableFunction_FunctionMul");
        auto mulFunctionNode = function->as<NodeFunctionMul>();
        auto serializedFunctionNode = SerializableFunction_FunctionMul();
        serializeFunction(mulFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(mulFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<NodeFunctionDiv>())
    {
        /// serialize div function node.
        NES_TRACE("FunctionSerializationUtil:: serialize DIV arithmetical function to SerializableFunction_FunctionDiv");
        auto divFunctionNode = function->as<NodeFunctionDiv>();
        auto serializedFunctionNode = SerializableFunction_FunctionDiv();
        serializeFunction(divFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(divFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<NodeFunctionMod>())
    {
        /// serialize mod function node.
        NES_TRACE("FunctionSerializationUtil:: serialize MODULO arithmetical function to SerializableFunction_FunctionPow");
        auto modFunctionNode = function->as<NodeFunctionMod>();
        auto serializedFunctionNode = SerializableFunction_FunctionMod();
        serializeFunction(modFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(modFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<NodeFunctionPow>())
    {
        /// serialize pow function node.
        NES_TRACE("FunctionSerializationUtil:: serialize POWER arithmetical function to SerializableFunction_FunctionPow");
        auto powFunctionNode = function->as<NodeFunctionPow>();
        auto serializedFunctionNode = SerializableFunction_FunctionPow();
        serializeFunction(powFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(powFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<NodeFunctionAbs>())
    {
        /// serialize abs function node.
        NES_TRACE("FunctionSerializationUtil:: serialize ABS arithmetical function to SerializableFunction_FunctionAbs");
        auto absFunctionNode = function->as<NodeFunctionAbs>();
        auto serializedFunctionNode = SerializableFunction_FunctionAbs();
        serializeFunction(absFunctionNode->child(), serializedFunctionNode.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<NodeFunctionCeil>())
    {
        /// serialize ceil function node.
        NES_TRACE("FunctionSerializationUtil:: serialize CEIL arithmetical function to SerializableFunction_FunctionCeil");
        auto ceilFunctionNode = function->as<NodeFunctionCeil>();
        auto serializedFunctionNode = SerializableFunction_FunctionCeil();
        serializeFunction(ceilFunctionNode->child(), serializedFunctionNode.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<NodeFunctionExp>())
    {
        /// serialize exp function node.
        NES_TRACE("FunctionSerializationUtil:: serialize EXP arithmetical function to SerializableFunction_FunctionExp");
        auto expFunctionNode = function->as<NodeFunctionExp>();
        auto serializedFunctionNode = SerializableFunction_FunctionExp();
        serializeFunction(expFunctionNode->child(), serializedFunctionNode.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<NodeFunctionFloor>())
    {
        /// serialize floor function node.
        NES_TRACE("FunctionSerializationUtil:: serialize FLOOR arithmetical function to SerializableFunction_FunctionFloor");
        auto floorFunctionNode = function->as<NodeFunctionFloor>();
        auto serializedFunctionNode = SerializableFunction_FunctionFloor();
        serializeFunction(floorFunctionNode->child(), serializedFunctionNode.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<NodeFunctionRound>())
    {
        /// serialize round function node.
        NES_TRACE("FunctionSerializationUtil:: serialize ROUND arithmetical function to SerializableFunction_FunctionRound");
        auto roundFunctionNode = function->as<NodeFunctionRound>();
        auto serializedFunctionNode = SerializableFunction_FunctionRound();
        serializeFunction(roundFunctionNode->child(), serializedFunctionNode.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<NodeFunctionSqrt>())
    {
        /// serialize sqrt function node.
        NES_TRACE("FunctionSerializationUtil:: serialize SQRT arithmetical function to SerializableFunction_FunctionSqrt");
        auto sqrtFunctionNode = function->as<NodeFunctionSqrt>();
        auto serializedFunctionNode = SerializableFunction_FunctionSqrt();
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

void FunctionSerializationUtil::serializeLogicalFunctions(const NodeFunctionPtr& function, SerializableFunction* serializedFunction)
{
    NES_DEBUG("FunctionSerializationUtil:: serialize logical function {}", function->toString());
    if (function->instanceOf<NodeFunctionAnd>())
    {
        /// serialize and function node.
        NES_TRACE("FunctionSerializationUtil:: serialize AND logical function to SerializableFunction_FunctionAnd");
        auto andFunctionNode = function->as<NodeFunctionAnd>();
        auto serializedFunctionNode = SerializableFunction_FunctionAnd();
        serializeFunction(andFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(andFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<NodeFunctionOr>())
    {
        /// serialize or function node.
        NES_TRACE("FunctionSerializationUtil:: serialize OR logical function to SerializableFunction_FunctionOr");
        auto orFunctionNode = function->as<NodeFunctionOr>();
        auto serializedFunctionNode = SerializableFunction_FunctionOr();
        serializeFunction(orFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(orFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<NodeFunctionLess>())
    {
        /// serialize less function node.
        NES_TRACE("FunctionSerializationUtil:: serialize Less logical function to SerializableFunction_FunctionLess");
        auto lessFunctionNode = function->as<NodeFunctionLess>();
        auto serializedFunctionNode = SerializableFunction_FunctionLess();
        serializeFunction(lessFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(lessFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<NodeFunctionLessEquals>())
    {
        /// serialize less equals function node.
        NES_TRACE("FunctionSerializationUtil:: serialize Less Equals logical function to "
                  "SerializableFunction_FunctionLessEquals");
        auto lessNodeFunctionEquals = function->as<NodeFunctionLessEquals>();
        auto serializedFunctionNode = SerializableFunction_FunctionLessEquals();
        serializeFunction(lessNodeFunctionEquals->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(lessNodeFunctionEquals->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<NodeFunctionGreater>())
    {
        /// serialize greater function node.
        NES_TRACE("FunctionSerializationUtil:: serialize Greater logical function to SerializableFunction_FunctionGreater");
        auto greaterFunctionNode = function->as<NodeFunctionGreater>();
        auto serializedFunctionNode = SerializableFunction_FunctionGreater();
        serializeFunction(greaterFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(greaterFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<NodeFunctionGreaterEquals>())
    {
        /// serialize greater equals function node.
        NES_TRACE("FunctionSerializationUtil:: serialize Greater Equals logical function to "
                  "SerializableFunction_FunctionGreaterEquals");
        auto greaterNodeFunctionEquals = function->as<NodeFunctionGreaterEquals>();
        auto serializedFunctionNode = SerializableFunction_FunctionGreaterEquals();
        serializeFunction(greaterNodeFunctionEquals->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(greaterNodeFunctionEquals->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<NodeFunctionEquals>())
    {
        /// serialize equals function node.
        NES_TRACE("FunctionSerializationUtil:: serialize Equals logical function to SerializableFunction_FunctionEquals");
        auto equalsFunctionNode = function->as<NodeFunctionEquals>();
        auto serializedFunctionNode = SerializableFunction_FunctionEquals();
        serializeFunction(equalsFunctionNode->getLeft(), serializedFunctionNode.mutable_left());
        serializeFunction(equalsFunctionNode->getRight(), serializedFunctionNode.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else if (function->instanceOf<NodeFunctionNegate>())
    {
        /// serialize negate function node.
        NES_TRACE("FunctionSerializationUtil:: serialize negate logical function to SerializableFunction_FunctionNegate");
        auto equalsFunctionNode = function->as<NodeFunctionNegate>();
        auto serializedFunctionNode = SerializableFunction_FunctionNegate();
        serializeFunction(equalsFunctionNode->child(), serializedFunctionNode.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedFunctionNode);
    }
    else
    {
        NES_FATAL_ERROR("FunctionSerializationUtil: No serialization implemented for this logical function node: {}", function->toString());
        NES_NOT_IMPLEMENTED();
    }
}

NodeFunctionPtr FunctionSerializationUtil::deserializeArithmeticalFunctions(const SerializableFunction& serializedFunction)
{
    if (serializedFunction.details().Is<SerializableFunction_FunctionAdd>())
    {
        /// de-serialize ADD function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as Add function node.");
        auto serializedFunctionNode = SerializableFunction_FunctionAdd();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return NodeFunctionAdd::create(left, right);
    }
    if (serializedFunction.details().Is<SerializableFunction_FunctionSub>())
    {
        /// de-serialize SUB function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as SUB function node.");
        auto serializedFunctionNode = SerializableFunction_FunctionSub();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return NodeFunctionSub::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionMul>())
    {
        /// de-serialize MUL function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as MUL function node.");
        auto serializedFunctionNode = SerializableFunction_FunctionMul();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return NodeFunctionMul::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionDiv>())
    {
        /// de-serialize DIV function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as DIV function node.");
        auto serializedFunctionNode = SerializableFunction_FunctionDiv();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return NodeFunctionDiv::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionMod>())
    {
        /// de-serialize MODULO function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as MODULO function node.");
        auto serializedFunctionNode = SerializableFunction_FunctionMod();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return NodeFunctionMod::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionPow>())
    {
        /// de-serialize POWER function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as POWER function node.");
        auto serializedFunctionNode = SerializableFunction_FunctionPow();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return NodeFunctionPow::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionAbs>())
    {
        /// de-serialize ABS function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as ABS function node.");
        auto serializedFunctionNode = SerializableFunction_FunctionAbs();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto child = deserializeFunction(serializedFunctionNode.child());
        return NodeFunctionAbs::create(child);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionCeil>())
    {
        /// de-serialize CEIL function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as CEIL function node.");
        auto serializedFunctionNode = SerializableFunction_FunctionCeil();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto child = deserializeFunction(serializedFunctionNode.child());
        return NodeFunctionCeil::create(child);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionExp>())
    {
        /// de-serialize EXP function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as EXP function node.");
        auto serializedFunctionNode = SerializableFunction_FunctionExp();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto child = deserializeFunction(serializedFunctionNode.child());
        return NodeFunctionExp::create(child);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionFloor>())
    {
        /// de-serialize FLOOR function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as FLOOR function node.");
        auto serializedFunctionNode = SerializableFunction_FunctionFloor();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto child = deserializeFunction(serializedFunctionNode.child());
        return NodeFunctionFloor::create(child);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionRound>())
    {
        /// de-serialize ROUND function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as ROUND function node.");
        auto serializedFunctionNode = SerializableFunction_FunctionRound();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto child = deserializeFunction(serializedFunctionNode.child());
        return NodeFunctionRound::create(child);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionSqrt>())
    {
        /// de-serialize SQRT function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as SQRT function node.");
        auto serializedFunctionNode = SerializableFunction_FunctionSqrt();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto child = deserializeFunction(serializedFunctionNode.child());
        return NodeFunctionSqrt::create(child);
    }
    return nullptr;
}

NodeFunctionPtr FunctionSerializationUtil::deserializeLogicalFunctions(const SerializableFunction& serializedFunction)
{
    NES_DEBUG("FunctionSerializationUtil:: de-serialize logical function {}", serializedFunction.details().type_url());
    if (serializedFunction.details().Is<SerializableFunction_FunctionAnd>())
    {
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as AND function node.");
        /// de-serialize and function node.
        auto serializedFunctionNode = SerializableFunction_FunctionAnd();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return NodeFunctionAnd::create(left, right);
    }
    if (serializedFunction.details().Is<SerializableFunction_FunctionOr>())
    {
        /// de-serialize or function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as OR function node.");
        auto serializedFunctionNode = SerializableFunction_FunctionOr();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return NodeFunctionOr::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionLess>())
    {
        /// de-serialize less function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as LESS function node.");
        auto serializedFunctionNode = SerializableFunction_FunctionLess();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return NodeFunctionLess::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionLessEquals>())
    {
        /// de-serialize less equals function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as LESS Equals function node.");
        auto serializedFunctionNode = SerializableFunction_FunctionLessEquals();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return NodeFunctionLessEquals::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionGreater>())
    {
        /// de-serialize greater function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as Greater function node.");
        auto serializedFunctionNode = SerializableFunction_FunctionGreater();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return NodeFunctionGreater::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionGreaterEquals>())
    {
        /// de-serialize greater equals function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as GreaterEquals function node.");
        auto serializedFunctionNode = SerializableFunction_FunctionGreaterEquals();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return NodeFunctionGreaterEquals::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionEquals>())
    {
        /// de-serialize equals function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as Equals function node.");
        auto serializedFunctionNode = SerializableFunction_FunctionEquals();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto left = deserializeFunction(serializedFunctionNode.left());
        auto right = deserializeFunction(serializedFunctionNode.right());
        return NodeFunctionEquals::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionNegate>())
    {
        /// de-serialize negate function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as Negate function node.");
        auto serializedFunctionNode = SerializableFunction_FunctionNegate();
        serializedFunction.details().UnpackTo(&serializedFunctionNode);
        auto child = deserializeFunction(serializedFunctionNode.child());
        return NodeFunctionNegate::create(child);
    }
    return nullptr;
}

} /// namespace NES
