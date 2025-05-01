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

#include <memory>
#include <vector>
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
#include <Functions/NodeFunctionConcat.hpp>
#include <Functions/NodeFunctionConstantValue.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Functions/NodeFunctionFieldRename.hpp>
#include <Functions/NodeFunctionWhen.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <SerializableFunction.pb.h>

namespace NES
{

SerializableFunction*
FunctionSerializationUtil::serializeFunction(const std::shared_ptr<NodeFunction>& function, SerializableFunction* serializedFunction)
{
    NES_TRACE("FunctionSerializationUtil:: serialize function {}", *function);
    /// serialize function node depending on its type.
    if (Util::instanceOf<LogicalNodeFunction>(function))
    {
        /// serialize logical function
        serializeLogicalFunctions(function, serializedFunction);
    }
    else if (Util::instanceOf<NodeFunctionArithmetical>(function))
    {
        /// serialize arithmetical functions
        serializeArithmeticalFunctions(function, serializedFunction);
    }
    else if (Util::instanceOf<NodeFunctionConcat>(function))
    {
        /// serialize concat function node.
        NES_TRACE("FunctionSerializationUtil:: serialize concat arithmetical function to SerializableFunction_FunctionConcat");
        auto concatNodeFunction = Util::as<NodeFunctionConcat>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionConcat();
        serializeFunction(concatNodeFunction->getLeft(), serializedNodeFunction.mutable_left());
        serializeFunction(concatNodeFunction->getRight(), serializedNodeFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else if (Util::instanceOf<NodeFunctionConstantValue>(function))
    {
        /// serialize constant value function node.
        NES_TRACE("FunctionSerializationUtil:: serialize constant value function node.");
        auto constantValueFunction = Util::as<NodeFunctionConstantValue>(function);
        auto value = constantValueFunction->getConstantValue();
        auto serializedConstantValue = SerializableFunction_FunctionConstantValue();
        serializedConstantValue.set_value(value);
        serializedFunction->mutable_details()->PackFrom(serializedConstantValue);
    }
    else if (Util::instanceOf<NodeFunctionFieldAccess>(function))
    {
        /// serialize field access function node
        NES_TRACE("FunctionSerializationUtil:: serialize field access function node.");
        auto fieldAccessFunction = Util::as<NodeFunctionFieldAccess>(function);
        auto serializedFieldAccessFunction = SerializableFunction_FunctionFieldAccess();
        serializedFieldAccessFunction.set_fieldname(fieldAccessFunction->getFieldName());
        serializedFunction->mutable_details()->PackFrom(serializedFieldAccessFunction);
    }
    else if (Util::instanceOf<NodeFunctionFieldRename>(function))
    {
        /// serialize field rename function node
        NES_TRACE("FunctionSerializationUtil:: serialize field rename function node.");
        auto fieldRenameFunction = Util::as<NodeFunctionFieldRename>(function);
        auto serializedFieldRenameFunction = SerializableFunction_FunctionFieldRename();
        serializeFunction(fieldRenameFunction->getOriginalField(), serializedFieldRenameFunction.mutable_functionoriginalfieldaccess());
        serializedFieldRenameFunction.set_newfieldname(fieldRenameFunction->getNewFieldName());
        serializedFunction->mutable_details()->PackFrom(serializedFieldRenameFunction);
    }
    else if (Util::instanceOf<NodeFunctionFieldAssignment>(function))
    {
        /// serialize field assignment function node.
        NES_TRACE("FunctionSerializationUtil:: serialize field assignment function node.");
        auto fieldAssignmentNodeFunction = Util::as<NodeFunctionFieldAssignment>(function);
        auto serializedFieldAssignmentFunction = SerializableFunction_FunctionFieldAssignment();
        auto* serializedFieldAccessFunction = serializedFieldAssignmentFunction.mutable_field();
        serializedFieldAccessFunction->set_fieldname(fieldAssignmentNodeFunction->getField()->getFieldName());
        DataTypeSerializationUtil::serializeDataType(
            fieldAssignmentNodeFunction->getField()->getStamp(), serializedFieldAccessFunction->mutable_type());
        /// serialize assignment function
        serializeFunction(fieldAssignmentNodeFunction->getAssignment(), serializedFieldAssignmentFunction.mutable_assignment());
        serializedFunction->mutable_details()->PackFrom(serializedFieldAssignmentFunction);
    }
    else if (Util::instanceOf<NodeFunctionWhen>(function))
    {
        /// serialize when function node.
        NES_TRACE("FunctionSerializationUtil:: serialize when function {}.", *function);
        auto whenNodeFunction = Util::as<NodeFunctionWhen>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionWhen();
        serializeFunction(whenNodeFunction->getLeft(), serializedNodeFunction.mutable_left());
        serializeFunction(whenNodeFunction->getRight(), serializedNodeFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else if (Util::instanceOf<NodeFunctionCase>(function))
    {
        /// serialize case function node.
        NES_TRACE("FunctionSerializationUtil:: serialize case function {}.", *function);
        auto caseNodeFunction = Util::as<NodeFunctionCase>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionCase();
        for (const auto& elem : caseNodeFunction->getWhenChildren())
        {
            serializeFunction(elem, serializedNodeFunction.add_left());
        }
        serializeFunction(caseNodeFunction->getDefaultExp(), serializedNodeFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }

    else
    {
        throw CannotSerialize(fmt::format("function: {}", *function));
    }

    DataTypeSerializationUtil::serializeDataType(function->getStamp(), serializedFunction->mutable_stamp());
    NES_TRACE("FunctionSerializationUtil:: serialize function node to {}", serializedFunction->mutable_details()->type_url());
    return serializedFunction;
}

std::shared_ptr<NodeFunction> FunctionSerializationUtil::deserializeFunction(const SerializableFunction& serializedFunction)
{
    NES_TRACE("FunctionSerializationUtil:: deserialize function {}", serializedFunction.details().type_url());
    /// de-serialize function
    /// 1. check if the serialized function is a logical function
    auto nodeFunction = deserializeLogicalFunctions(serializedFunction);
    /// 2. if the function was not de-serialized then try if it's an arithmetical function
    if (!nodeFunction)
    {
        nodeFunction = deserializeArithmeticalFunctions(serializedFunction);
    }
    /// 3. if the function was not de-serialized try remaining function types
    if (!nodeFunction)
    {
        if (serializedFunction.details().Is<SerializableFunction_FunctionConcat>())
        {
            /// de-serialize CONCAT function node.
            NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as CONCAT function node.");
            auto serializedNodeFunction = SerializableFunction_FunctionConcat();
            serializedFunction.details().UnpackTo(&serializedNodeFunction);
            auto left = deserializeFunction(serializedNodeFunction.left());
            auto right = deserializeFunction(serializedNodeFunction.right());
            return NodeFunctionConcat::create(left, right);
        }
        if (serializedFunction.details().Is<SerializableFunction_FunctionConstantValue>())
        {
            /// de-serialize constant value function node.
            NES_TRACE("FunctionSerializationUtil:: de-serialize function as Constant Value function node.");
            auto serializedConstantValue = SerializableFunction_FunctionConstantValue();
            serializedFunction.details().UnpackTo(&serializedConstantValue);
            /// The data type stored in the function's stamp is equal to the datatype of the value
            auto valueDataType = DataTypeSerializationUtil::deserializeDataType(serializedFunction.stamp());
            nodeFunction = NodeFunctionConstantValue::create(valueDataType, serializedConstantValue.value());
        }
        else if (serializedFunction.details().Is<SerializableFunction_FunctionFieldAccess>())
        {
            /// de-serialize field access function node.
            NES_TRACE("FunctionSerializationUtil:: de-serialize function as FieldAccess function node.");
            SerializableFunction_FunctionFieldAccess serializedFieldAccessFunction;
            serializedFunction.details().UnpackTo(&serializedFieldAccessFunction);
            const auto& name = serializedFieldAccessFunction.fieldname();
            nodeFunction = NodeFunctionFieldAccess::create(name);
        }
        else if (serializedFunction.details().Is<SerializableFunction_FunctionFieldRename>())
        {
            /// de-serialize field rename function node.
            NES_TRACE("FunctionSerializationUtil:: de-serialize function as Field Rename function node.");
            SerializableFunction_FunctionFieldRename serializedFieldRenameFunction;
            serializedFunction.details().UnpackTo(&serializedFieldRenameFunction);
            auto originalFieldAccessFunction = deserializeFunction(serializedFieldRenameFunction.functionoriginalfieldaccess());
            if (!Util::instanceOf<NodeFunctionFieldAccess>(originalFieldAccessFunction))
            {
                throw CannotDeserialize(fmt::format(
                    "FunctionSerializationUtil: the original field access function should be of type NodeFunctionFieldAccess,"
                    "but was a {}",
                    *originalFieldAccessFunction));
            }
            const auto& newFieldName = serializedFieldRenameFunction.newfieldname();
            nodeFunction = NodeFunctionFieldRename::create(Util::as<NodeFunctionFieldAccess>(originalFieldAccessFunction), newFieldName);
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
            nodeFunction = NodeFunctionFieldAssignment::create(Util::as<NodeFunctionFieldAccess>(fieldAccessNode), fieldAssignmentFunction);
        }
        else if (serializedFunction.details().Is<SerializableFunction_FunctionWhen>())
        {
            /// de-serialize WHEN function node.
            NES_TRACE("FunctionSerializationUtil:: de-serialize function as When function node.");
            auto serializedNodeFunction = SerializableFunction_FunctionWhen();
            serializedFunction.details().UnpackTo(&serializedNodeFunction);
            auto left = deserializeFunction(serializedNodeFunction.left());
            auto right = deserializeFunction(serializedNodeFunction.right());
            return NodeFunctionWhen::create(left, right);
        }
        else if (serializedFunction.details().Is<SerializableFunction_FunctionCase>())
        {
            /// de-serialize CASE function node.
            NES_TRACE("FunctionSerializationUtil:: de-serialize function as Case function node.");
            auto serializedNodeFunction = SerializableFunction_FunctionCase();
            serializedFunction.details().UnpackTo(&serializedNodeFunction);
            std::vector<std::shared_ptr<NodeFunction>> leftExps;

            ///todo: deserialization might be possible more efficiently
            for (int i = 0; i < serializedNodeFunction.left_size(); i++)
            {
                const auto& leftNode = serializedNodeFunction.left(i);
                leftExps.push_back(deserializeFunction(leftNode));
            }
            auto right = deserializeFunction(serializedNodeFunction.right());
            return NodeFunctionCase::create(leftExps, right);
        }
        else
        {
            throw CannotDeserialize("FunctionSerializationUtil: could not de-serialize this function");
        }
    }

    if (!nodeFunction)
    {
        throw CannotDeserialize("FunctionSerializationUtil:: fatal error during de-serialization. The function node must not be null");
    }

    /// deserialize function stamp
    auto stamp = DataTypeSerializationUtil::deserializeDataType(serializedFunction.stamp());
    nodeFunction->setStamp(stamp);
    NES_TRACE("FunctionSerializationUtil:: deserialized function node to the following node: {}", *nodeFunction);
    return nodeFunction;
}

void FunctionSerializationUtil::serializeArithmeticalFunctions(
    const std::shared_ptr<NodeFunction>& function, SerializableFunction* serializedFunction)
{
    NES_TRACE("FunctionSerializationUtil:: serialize arithmetical function {}", *function);
    if (Util::instanceOf<NodeFunctionAdd>(function))
    {
        /// serialize add function node.
        NES_TRACE("FunctionSerializationUtil:: serialize ADD arithmetical function to SerializableFunction_FunctionAdd");
        auto addNodeFunction = Util::as<NodeFunctionAdd>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionAdd();
        serializeFunction(addNodeFunction->getLeft(), serializedNodeFunction.mutable_left());
        serializeFunction(addNodeFunction->getRight(), serializedNodeFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else if (Util::instanceOf<NodeFunctionSub>(function))
    {
        /// serialize sub function node.
        NES_TRACE("FunctionSerializationUtil:: serialize SUB arithmetical function to SerializableFunction_FunctionSub");
        auto subNodeFunction = Util::as<NodeFunctionSub>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionSub();
        serializeFunction(subNodeFunction->getLeft(), serializedNodeFunction.mutable_left());
        serializeFunction(subNodeFunction->getRight(), serializedNodeFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else if (Util::instanceOf<NodeFunctionMul>(function))
    {
        /// serialize mul function node.
        NES_TRACE("FunctionSerializationUtil:: serialize MUL arithmetical function to SerializableFunction_FunctionMul");
        auto mulNodeFunction = Util::as<NodeFunctionMul>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionMul();
        serializeFunction(mulNodeFunction->getLeft(), serializedNodeFunction.mutable_left());
        serializeFunction(mulNodeFunction->getRight(), serializedNodeFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else if (Util::instanceOf<NodeFunctionDiv>(function))
    {
        /// serialize div function node.
        NES_TRACE("FunctionSerializationUtil:: serialize DIV arithmetical function to SerializableFunction_FunctionDiv");
        auto divNodeFunction = Util::as<NodeFunctionDiv>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionDiv();
        serializeFunction(divNodeFunction->getLeft(), serializedNodeFunction.mutable_left());
        serializeFunction(divNodeFunction->getRight(), serializedNodeFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else if (Util::instanceOf<NodeFunctionMod>(function))
    {
        /// serialize mod function node.
        NES_TRACE("FunctionSerializationUtil:: serialize MODULO arithmetical function to SerializableFunction_FunctionPow");
        auto modNodeFunction = Util::as<NodeFunctionMod>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionMod();
        serializeFunction(modNodeFunction->getLeft(), serializedNodeFunction.mutable_left());
        serializeFunction(modNodeFunction->getRight(), serializedNodeFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else if (Util::instanceOf<NodeFunctionPow>(function))
    {
        /// serialize pow function node.
        NES_TRACE("FunctionSerializationUtil:: serialize POWER arithmetical function to SerializableFunction_FunctionPow");
        auto powNodeFunction = Util::as<NodeFunctionPow>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionPow();
        serializeFunction(powNodeFunction->getLeft(), serializedNodeFunction.mutable_left());
        serializeFunction(powNodeFunction->getRight(), serializedNodeFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else if (Util::instanceOf<NodeFunctionAbs>(function))
    {
        /// serialize abs function node.
        NES_TRACE("FunctionSerializationUtil:: serialize ABS arithmetical function to SerializableFunction_FunctionAbs");
        auto absNodeFunction = Util::as<NodeFunctionAbs>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionAbs();
        serializeFunction(absNodeFunction->child(), serializedNodeFunction.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else if (Util::instanceOf<NodeFunctionCeil>(function))
    {
        /// serialize ceil function node.
        NES_TRACE("FunctionSerializationUtil:: serialize CEIL arithmetical function to SerializableFunction_FunctionCeil");
        auto ceilNodeFunction = Util::as<NodeFunctionCeil>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionCeil();
        serializeFunction(ceilNodeFunction->child(), serializedNodeFunction.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else if (Util::instanceOf<NodeFunctionExp>(function))
    {
        /// serialize exp function node.
        NES_TRACE("FunctionSerializationUtil:: serialize EXP arithmetical function to SerializableFunction_FunctionExp");
        auto expNodeFunction = Util::as<NodeFunctionExp>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionExp();
        serializeFunction(expNodeFunction->child(), serializedNodeFunction.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else if (Util::instanceOf<NodeFunctionFloor>(function))
    {
        /// serialize floor function node.
        NES_TRACE("FunctionSerializationUtil:: serialize FLOOR arithmetical function to SerializableFunction_FunctionFloor");
        auto floorNodeFunction = Util::as<NodeFunctionFloor>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionFloor();
        serializeFunction(floorNodeFunction->child(), serializedNodeFunction.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else if (Util::instanceOf<NodeFunctionRound>(function))
    {
        /// serialize round function node.
        NES_TRACE("FunctionSerializationUtil:: serialize ROUND arithmetical function to SerializableFunction_FunctionRound");
        auto roundNodeFunction = Util::as<NodeFunctionRound>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionRound();
        serializeFunction(roundNodeFunction->child(), serializedNodeFunction.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else if (Util::instanceOf<NodeFunctionSqrt>(function))
    {
        /// serialize sqrt function node.
        NES_TRACE("FunctionSerializationUtil:: serialize SQRT arithmetical function to SerializableFunction_FunctionSqrt");
        auto sqrtNodeFunction = Util::as<NodeFunctionSqrt>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionSqrt();
        serializeFunction(sqrtNodeFunction->child(), serializedNodeFunction.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else
    {
        throw CannotSerialize(fmt::format(
            "TranslateToLegacyPhase:"
            "No serialization implemented for this arithmetical function node: {}",
            *function));
    }
}

void FunctionSerializationUtil::serializeLogicalFunctions(
    const std::shared_ptr<NodeFunction>& function, SerializableFunction* serializedFunction)
{
    NES_TRACE("FunctionSerializationUtil:: serialize logical function {}", *function);
    if (Util::instanceOf<NodeFunctionAnd>(function))
    {
        /// serialize and function node.
        NES_TRACE("FunctionSerializationUtil:: serialize AND logical function to SerializableFunction_FunctionAnd");
        auto andNodeFunction = Util::as<NodeFunctionAnd>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionAnd();
        serializeFunction(andNodeFunction->getLeft(), serializedNodeFunction.mutable_left());
        serializeFunction(andNodeFunction->getRight(), serializedNodeFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else if (Util::instanceOf<NodeFunctionOr>(function))
    {
        /// serialize or function node.
        NES_TRACE("FunctionSerializationUtil:: serialize OR logical function to SerializableFunction_FunctionOr");
        auto orNodeFunction = Util::as<NodeFunctionOr>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionOr();
        serializeFunction(orNodeFunction->getLeft(), serializedNodeFunction.mutable_left());
        serializeFunction(orNodeFunction->getRight(), serializedNodeFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else if (Util::instanceOf<NodeFunctionLess>(function))
    {
        /// serialize less function node.
        NES_TRACE("FunctionSerializationUtil:: serialize Less logical function to SerializableFunction_FunctionLess");
        auto lessNodeFunction = Util::as<NodeFunctionLess>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionLess();
        serializeFunction(lessNodeFunction->getLeft(), serializedNodeFunction.mutable_left());
        serializeFunction(lessNodeFunction->getRight(), serializedNodeFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else if (Util::instanceOf<NodeFunctionLessEquals>(function))
    {
        /// serialize less equals function node.
        NES_TRACE("FunctionSerializationUtil:: serialize Less Equals logical function to "
                  "SerializableFunction_FunctionLessEquals");
        auto lessNodeFunctionEquals = Util::as<NodeFunctionLessEquals>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionLessEquals();
        serializeFunction(lessNodeFunctionEquals->getLeft(), serializedNodeFunction.mutable_left());
        serializeFunction(lessNodeFunctionEquals->getRight(), serializedNodeFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else if (Util::instanceOf<NodeFunctionGreater>(function))
    {
        /// serialize greater function node.
        NES_TRACE("FunctionSerializationUtil:: serialize Greater logical function to SerializableFunction_FunctionGreater");
        auto greaterNodeFunction = Util::as<NodeFunctionGreater>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionGreater();
        serializeFunction(greaterNodeFunction->getLeft(), serializedNodeFunction.mutable_left());
        serializeFunction(greaterNodeFunction->getRight(), serializedNodeFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else if (Util::instanceOf<NodeFunctionGreaterEquals>(function))
    {
        /// serialize greater equals function node.
        NES_TRACE("FunctionSerializationUtil:: serialize Greater Equals logical function to "
                  "SerializableFunction_FunctionGreaterEquals");
        auto greaterNodeFunctionEquals = Util::as<NodeFunctionGreaterEquals>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionGreaterEquals();
        serializeFunction(greaterNodeFunctionEquals->getLeft(), serializedNodeFunction.mutable_left());
        serializeFunction(greaterNodeFunctionEquals->getRight(), serializedNodeFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else if (Util::instanceOf<NodeFunctionEquals>(function))
    {
        /// serialize equals function node.
        NES_TRACE("FunctionSerializationUtil:: serialize Equals logical function to SerializableFunction_FunctionEquals");
        auto equalsNodeFunction = Util::as<NodeFunctionEquals>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionEquals();
        serializeFunction(equalsNodeFunction->getLeft(), serializedNodeFunction.mutable_left());
        serializeFunction(equalsNodeFunction->getRight(), serializedNodeFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else if (Util::instanceOf<NodeFunctionNegate>(function))
    {
        /// serialize negate function node.
        NES_TRACE("FunctionSerializationUtil:: serialize negate logical function to SerializableFunction_FunctionNegate");
        auto equalsNodeFunction = Util::as<NodeFunctionNegate>(function);
        auto serializedNodeFunction = SerializableFunction_FunctionNegate();
        serializeFunction(equalsNodeFunction->child(), serializedNodeFunction.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedNodeFunction);
    }
    else
    {
        throw CannotSerialize(
            fmt::format("FunctionSerializationUtil: No serialization implemented for this logical function node: {}", *function));
    }
}

std::shared_ptr<NodeFunction> FunctionSerializationUtil::deserializeArithmeticalFunctions(const SerializableFunction& serializedFunction)
{
    if (serializedFunction.details().Is<SerializableFunction_FunctionAdd>())
    {
        /// de-serialize ADD function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as Add function node.");
        auto serializedNodeFunction = SerializableFunction_FunctionAdd();
        serializedFunction.details().UnpackTo(&serializedNodeFunction);
        auto left = deserializeFunction(serializedNodeFunction.left());
        auto right = deserializeFunction(serializedNodeFunction.right());
        return NodeFunctionAdd::create(left, right);
    }
    if (serializedFunction.details().Is<SerializableFunction_FunctionSub>())
    {
        /// de-serialize SUB function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as SUB function node.");
        auto serializedNodeFunction = SerializableFunction_FunctionSub();
        serializedFunction.details().UnpackTo(&serializedNodeFunction);
        auto left = deserializeFunction(serializedNodeFunction.left());
        auto right = deserializeFunction(serializedNodeFunction.right());
        return NodeFunctionSub::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionMul>())
    {
        /// de-serialize MUL function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as MUL function node.");
        auto serializedNodeFunction = SerializableFunction_FunctionMul();
        serializedFunction.details().UnpackTo(&serializedNodeFunction);
        auto left = deserializeFunction(serializedNodeFunction.left());
        auto right = deserializeFunction(serializedNodeFunction.right());
        return NodeFunctionMul::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionDiv>())
    {
        /// de-serialize DIV function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as DIV function node.");
        auto serializedNodeFunction = SerializableFunction_FunctionDiv();
        serializedFunction.details().UnpackTo(&serializedNodeFunction);
        auto left = deserializeFunction(serializedNodeFunction.left());
        auto right = deserializeFunction(serializedNodeFunction.right());
        return NodeFunctionDiv::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionMod>())
    {
        /// de-serialize MODULO function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as MODULO function node.");
        auto serializedNodeFunction = SerializableFunction_FunctionMod();
        serializedFunction.details().UnpackTo(&serializedNodeFunction);
        auto left = deserializeFunction(serializedNodeFunction.left());
        auto right = deserializeFunction(serializedNodeFunction.right());
        return NodeFunctionMod::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionPow>())
    {
        /// de-serialize POWER function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as POWER function node.");
        auto serializedNodeFunction = SerializableFunction_FunctionPow();
        serializedFunction.details().UnpackTo(&serializedNodeFunction);
        auto left = deserializeFunction(serializedNodeFunction.left());
        auto right = deserializeFunction(serializedNodeFunction.right());
        return NodeFunctionPow::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionAbs>())
    {
        /// de-serialize ABS function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as ABS function node.");
        auto serializedNodeFunction = SerializableFunction_FunctionAbs();
        serializedFunction.details().UnpackTo(&serializedNodeFunction);
        auto child = deserializeFunction(serializedNodeFunction.child());
        return NodeFunctionAbs::create(child);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionCeil>())
    {
        /// de-serialize CEIL function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as CEIL function node.");
        auto serializedNodeFunction = SerializableFunction_FunctionCeil();
        serializedFunction.details().UnpackTo(&serializedNodeFunction);
        auto child = deserializeFunction(serializedNodeFunction.child());
        return NodeFunctionCeil::create(child);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionExp>())
    {
        /// de-serialize EXP function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as EXP function node.");
        auto serializedNodeFunction = SerializableFunction_FunctionExp();
        serializedFunction.details().UnpackTo(&serializedNodeFunction);
        auto child = deserializeFunction(serializedNodeFunction.child());
        return NodeFunctionExp::create(child);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionFloor>())
    {
        /// de-serialize FLOOR function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as FLOOR function node.");
        auto serializedNodeFunction = SerializableFunction_FunctionFloor();
        serializedFunction.details().UnpackTo(&serializedNodeFunction);
        auto child = deserializeFunction(serializedNodeFunction.child());
        return NodeFunctionFloor::create(child);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionRound>())
    {
        /// de-serialize ROUND function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as ROUND function node.");
        auto serializedNodeFunction = SerializableFunction_FunctionRound();
        serializedFunction.details().UnpackTo(&serializedNodeFunction);
        auto child = deserializeFunction(serializedNodeFunction.child());
        return NodeFunctionRound::create(child);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionSqrt>())
    {
        /// de-serialize SQRT function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as SQRT function node.");
        auto serializedNodeFunction = SerializableFunction_FunctionSqrt();
        serializedFunction.details().UnpackTo(&serializedNodeFunction);
        auto child = deserializeFunction(serializedNodeFunction.child());
        return NodeFunctionSqrt::create(child);
    }
    return nullptr;
}

std::shared_ptr<NodeFunction> FunctionSerializationUtil::deserializeLogicalFunctions(const SerializableFunction& serializedFunction)
{
    NES_TRACE("FunctionSerializationUtil:: de-serialize logical function {}", serializedFunction.details().type_url());
    if (serializedFunction.details().Is<SerializableFunction_FunctionAnd>())
    {
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as AND function node.");
        /// de-serialize and function node.
        auto serializedNodeFunction = SerializableFunction_FunctionAnd();
        serializedFunction.details().UnpackTo(&serializedNodeFunction);
        auto left = deserializeFunction(serializedNodeFunction.left());
        auto right = deserializeFunction(serializedNodeFunction.right());
        return NodeFunctionAnd::create(left, right);
    }
    if (serializedFunction.details().Is<SerializableFunction_FunctionOr>())
    {
        /// de-serialize or function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as OR function node.");
        auto serializedNodeFunction = SerializableFunction_FunctionOr();
        serializedFunction.details().UnpackTo(&serializedNodeFunction);
        auto left = deserializeFunction(serializedNodeFunction.left());
        auto right = deserializeFunction(serializedNodeFunction.right());
        return NodeFunctionOr::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionLess>())
    {
        /// de-serialize less function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as LESS function node.");
        auto serializedNodeFunction = SerializableFunction_FunctionLess();
        serializedFunction.details().UnpackTo(&serializedNodeFunction);
        auto left = deserializeFunction(serializedNodeFunction.left());
        auto right = deserializeFunction(serializedNodeFunction.right());
        return NodeFunctionLess::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionLessEquals>())
    {
        /// de-serialize less equals function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as LESS Equals function node.");
        auto serializedNodeFunction = SerializableFunction_FunctionLessEquals();
        serializedFunction.details().UnpackTo(&serializedNodeFunction);
        auto left = deserializeFunction(serializedNodeFunction.left());
        auto right = deserializeFunction(serializedNodeFunction.right());
        return NodeFunctionLessEquals::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionGreater>())
    {
        /// de-serialize greater function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as Greater function node.");
        auto serializedNodeFunction = SerializableFunction_FunctionGreater();
        serializedFunction.details().UnpackTo(&serializedNodeFunction);
        auto left = deserializeFunction(serializedNodeFunction.left());
        auto right = deserializeFunction(serializedNodeFunction.right());
        return NodeFunctionGreater::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionGreaterEquals>())
    {
        /// de-serialize greater equals function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as GreaterEquals function node.");
        auto serializedNodeFunction = SerializableFunction_FunctionGreaterEquals();
        serializedFunction.details().UnpackTo(&serializedNodeFunction);
        auto left = deserializeFunction(serializedNodeFunction.left());
        auto right = deserializeFunction(serializedNodeFunction.right());
        return NodeFunctionGreaterEquals::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionEquals>())
    {
        /// de-serialize equals function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as Equals function node.");
        auto serializedNodeFunction = SerializableFunction_FunctionEquals();
        serializedFunction.details().UnpackTo(&serializedNodeFunction);
        auto left = deserializeFunction(serializedNodeFunction.left());
        auto right = deserializeFunction(serializedNodeFunction.right());
        return NodeFunctionEquals::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionNegate>())
    {
        /// de-serialize negate function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as Negate function node.");
        auto serializedNodeFunction = SerializableFunction_FunctionNegate();
        serializedFunction.details().UnpackTo(&serializedNodeFunction);
        auto child = deserializeFunction(serializedNodeFunction.child());
        return NodeFunctionNegate::create(child);
    }
    return nullptr;
}

}
