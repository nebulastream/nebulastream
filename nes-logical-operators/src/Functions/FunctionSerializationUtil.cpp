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

#include <Functions/FunctionSerializationUtil.hpp>
#include <fmt/format.h>
#include <Functions/ArithmeticalFunctions/AbsoluteUnaryLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/AddBinaryLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/CeilUnaryLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/DivBinaryLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/ExpUnaryLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/FloorUnaryLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/ModuloBinaryLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/MulBinaryLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/PowBinaryLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/RoundUnaryLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/SqrtUnaryLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/SubBinaryLogicalFunction.hpp>
#include <Functions/CaseLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAssignmentBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/LogicalFunctions/AndBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/EqualsBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/GreaterBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/GreaterEqualsBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/LessBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/LessEqualsBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/NegateUnaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/OrBinaryLogicalFunction.hpp>
#include <Functions/RenameLogicalFunction.hpp>
#include <Functions/WhenBinaryLogicalFunction.hpp>
#include <ErrorHandling.hpp>
#include <SerializableFunction.pb.h>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

SerializableFunction*
FunctionSerializationUtil::serializeFunction(const std::shared_ptr<LogicalFunction>& function, SerializableFunction* serializedFunction)
{
    NES_DEBUG("FunctionSerializationUtil:: serialize function {}", *function);
    /// serialize function node depending on its type.
    if (Util::instanceOf<AndBinaryLogicalFunction>(function))
    {
        /// serialize and function node.
        NES_TRACE("FunctionSerializationUtil:: serialize AND logical function to SerializableFunction_FunctionAnd");
        auto andLogicalFunction = Util::as<AndBinaryLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionAnd();
        serializeFunction(andLogicalFunction->getLeftChild(), serializedLogicalFunction.mutable_left());
        serializeFunction(andLogicalFunction->getRightChild(), serializedLogicalFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else if (Util::instanceOf<OrBinaryLogicalFunction>(function))
    {
        /// serialize or function node.
        NES_TRACE("FunctionSerializationUtil:: serialize OR logical function to SerializableFunction_FunctionOr");
        auto orLogicalFunction = Util::as<OrBinaryLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionOr();
        serializeFunction(orLogicalFunction->getLeftChild(), serializedLogicalFunction.mutable_left());
        serializeFunction(orLogicalFunction->getRightChild(), serializedLogicalFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else if (Util::instanceOf<LessBinaryLogicalFunction>(function))
    {
        /// serialize less function node.
        NES_TRACE("FunctionSerializationUtil:: serialize Less logical function to SerializableFunction_FunctionLess");
        auto lessLogicalFunction = Util::as<LessBinaryLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionLess();
        serializeFunction(lessLogicalFunction->getLeftChild(), serializedLogicalFunction.mutable_left());
        serializeFunction(lessLogicalFunction->getRightChild(), serializedLogicalFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else if (Util::instanceOf<LessEqualsBinaryLogicalFunction>(function))
    {
        /// serialize less equals function node.
        NES_TRACE("FunctionSerializationUtil:: serialize Less Equals logical function to "
                  "SerializableFunction_FunctionLessEquals");
        auto lessEqualsBinaryLogicalFunction = Util::as<LessEqualsBinaryLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionLessEquals();
        serializeFunction(lessEqualsBinaryLogicalFunction->getLeftChild(), serializedLogicalFunction.mutable_left());
        serializeFunction(lessEqualsBinaryLogicalFunction->getRightChild(), serializedLogicalFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else if (Util::instanceOf<GreaterBinaryLogicalFunction>(function))
    {
        /// serialize greater function node.
        NES_TRACE("FunctionSerializationUtil:: serialize Greater logical function to SerializableFunction_FunctionGreater");
        auto greaterLogicalFunction = Util::as<GreaterBinaryLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionGreater();
        serializeFunction(greaterLogicalFunction->getLeftChild(), serializedLogicalFunction.mutable_left());
        serializeFunction(greaterLogicalFunction->getRightChild(), serializedLogicalFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else if (Util::instanceOf<GreaterEqualsBinaryLogicalFunction>(function))
    {
        /// serialize greater equals function node.
        NES_TRACE("FunctionSerializationUtil:: serialize Greater Equals logical function to "
                  "SerializableFunction_FunctionGreaterEquals");
        auto greaterEqualsBinaryLogicalFunction = Util::as<GreaterEqualsBinaryLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionGreaterEquals();
        serializeFunction(greaterEqualsBinaryLogicalFunction->getLeftChild(), serializedLogicalFunction.mutable_left());
        serializeFunction(greaterEqualsBinaryLogicalFunction->getRightChild(), serializedLogicalFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else if (Util::instanceOf<EqualsBinaryLogicalFunction>(function))
    {
        /// serialize equals function node.
        NES_TRACE("FunctionSerializationUtil:: serialize Equals logical function to SerializableFunction_FunctionEquals");
        auto equalsLogicalFunction = Util::as<EqualsBinaryLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionEquals();
        serializeFunction(equalsLogicalFunction->getLeftChild(), serializedLogicalFunction.mutable_left());
        serializeFunction(equalsLogicalFunction->getRightChild(), serializedLogicalFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else if (Util::instanceOf<NegateUnaryLogicalFunction>(function))
    {
        /// serialize negate function node.
        NES_TRACE("FunctionSerializationUtil:: serialize negate logical function to SerializableFunction_FunctionNegate");
        auto equalsLogicalFunction = Util::as<NegateUnaryLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionNegate();
        serializeFunction(equalsLogicalFunction->getChild(), serializedLogicalFunction.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else if (Util::instanceOf<AddBinaryLogicalFunction>(function))
    {
        /// serialize add function node.
        NES_TRACE("FunctionSerializationUtil:: serialize ADD arithmetical function to SerializableFunction_FunctionAdd");
        auto addLogicalFunction = Util::as<AddBinaryLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionAdd();
        serializeFunction(addLogicalFunction->getLeftChild(), serializedLogicalFunction.mutable_left());
        serializeFunction(addLogicalFunction->getRightChild(), serializedLogicalFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else if (Util::instanceOf<SubBinaryLogicalFunction>(function))
    {
        /// serialize sub function node.
        NES_TRACE("FunctionSerializationUtil:: serialize SUB arithmetical function to SerializableFunction_FunctionSub");
        auto subLogicalFunction = Util::as<SubBinaryLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionSub();
        serializeFunction(subLogicalFunction->getLeftChild(), serializedLogicalFunction.mutable_left());
        serializeFunction(subLogicalFunction->getRightChild(), serializedLogicalFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else if (Util::instanceOf<MulBinaryLogicalFunction>(function))
    {
        /// serialize mul function node.
        NES_TRACE("FunctionSerializationUtil:: serialize MUL arithmetical function to SerializableFunction_FunctionMul");
        auto mulLogicalFunction = Util::as<MulBinaryLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionMul();
        serializeFunction(mulLogicalFunction->getLeftChild(), serializedLogicalFunction.mutable_left());
        serializeFunction(mulLogicalFunction->getRightChild(), serializedLogicalFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else if (Util::instanceOf<DivBinaryLogicalFunction>(function))
    {
        /// serialize div function node.
        NES_TRACE("FunctionSerializationUtil:: serialize DIV arithmetical function to SerializableFunction_FunctionDiv");
        auto divLogicalFunction = Util::as<DivBinaryLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionDiv();
        serializeFunction(divLogicalFunction->getLeftChild(), serializedLogicalFunction.mutable_left());
        serializeFunction(divLogicalFunction->getRightChild(), serializedLogicalFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else if (Util::instanceOf<ModuloBinaryLogicalFunction>(function))
    {
        /// serialize mod function node.
        NES_TRACE("FunctionSerializationUtil:: serialize MODULO arithmetical function to SerializableFunction_FunctionPow");
        auto modLogicalFunction = Util::as<ModuloBinaryLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionMod();
        serializeFunction(modLogicalFunction->getLeftChild(), serializedLogicalFunction.mutable_left());
        serializeFunction(modLogicalFunction->getRightChild(), serializedLogicalFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else if (Util::instanceOf<PowBinaryLogicalFunction>(function))
    {
        /// serialize pow function node.
        NES_TRACE("FunctionSerializationUtil:: serialize POWER arithmetical function to SerializableFunction_FunctionPow");
        auto powLogicalFunction = Util::as<PowBinaryLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionPow();
        serializeFunction(powLogicalFunction->getLeftChild(), serializedLogicalFunction.mutable_left());
        serializeFunction(powLogicalFunction->getRightChild(), serializedLogicalFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else if (Util::instanceOf<AbsoluteUnaryLogicalFunction>(function))
    {
        /// serialize abs function node.
        NES_TRACE("FunctionSerializationUtil:: serialize ABS arithmetical function to SerializableFunction_FunctionAbs");
        auto absLogicalFunction = Util::as<AbsoluteUnaryLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionAbs();
        serializeFunction(absLogicalFunction->getChild(), serializedLogicalFunction.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else if (Util::instanceOf<CeilUnaryLogicalFunction>(function))
    {
        /// serialize ceil function node.
        NES_TRACE("FunctionSerializationUtil:: serialize CEIL arithmetical function to SerializableFunction_FunctionCeil");
        auto ceilLogicalFunction = Util::as<CeilUnaryLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionCeil();
        serializeFunction(ceilLogicalFunction->getChild(), serializedLogicalFunction.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else if (Util::instanceOf<ExpUnaryLogicalFunction>(function))
    {
        /// serialize exp function node.
        NES_TRACE("FunctionSerializationUtil:: serialize EXP arithmetical function to SerializableFunction_FunctionExp");
        auto expLogicalFunction = Util::as<ExpUnaryLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionExp();
        serializeFunction(expLogicalFunction->getChild(), serializedLogicalFunction.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else if (Util::instanceOf<FloorUnaryLogicalFunction>(function))
    {
        /// serialize floor function node.
        NES_TRACE("FunctionSerializationUtil:: serialize FLOOR arithmetical function to SerializableFunction_FunctionFloor");
        auto floorLogicalFunction = Util::as<FloorUnaryLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionFloor();
        serializeFunction(floorLogicalFunction->getChild(), serializedLogicalFunction.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else if (Util::instanceOf<RoundUnaryLogicalFunction>(function))
    {
        /// serialize round function node.
        NES_TRACE("FunctionSerializationUtil:: serialize ROUND arithmetical function to SerializableFunction_FunctionRound");
        auto roundLogicalFunction = Util::as<RoundUnaryLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionRound();
        serializeFunction(roundLogicalFunction->getChild(), serializedLogicalFunction.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else if (Util::instanceOf<SqrtUnaryLogicalFunction>(function))
    {
        /// serialize sqrt function node.
        NES_TRACE("FunctionSerializationUtil:: serialize SQRT arithmetical function to SerializableFunction_FunctionSqrt");
        auto sqrtLogicalFunction = Util::as<SqrtUnaryLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionSqrt();
        serializeFunction(sqrtLogicalFunction->getChild(), serializedLogicalFunction.mutable_child());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else if (Util::instanceOf<ConstantValueLogicalFunction>(function))
    {
        /// serialize constant value function node.
        NES_TRACE("FunctionSerializationUtil:: serialize constant value function node.");
        auto constantValueFunction = Util::as<ConstantValueLogicalFunction>(function);
        auto value = constantValueFunction->getConstantValue();
        auto serializedConstantValue = SerializableFunction_FunctionConstantValue();
        serializedConstantValue.set_value(value);
        serializedFunction->mutable_details()->PackFrom(serializedConstantValue);
    }
    else if (Util::instanceOf<FieldAccessLogicalFunction>(function))
    {
        /// serialize field access function node
        NES_TRACE("FunctionSerializationUtil:: serialize field access function node.");
        auto fieldAccessFunction = Util::as<FieldAccessLogicalFunction>(function);
        auto serializedFieldAccessFunction = SerializableFunction_FunctionFieldAccess();
        serializedFieldAccessFunction.set_fieldname(fieldAccessFunction->getFieldName());
        serializedFunction->mutable_details()->PackFrom(serializedFieldAccessFunction);
    }
    else if (Util::instanceOf<RenameLogicalFunction>(function))
    {
        /// serialize field rename function node
        NES_TRACE("FunctionSerializationUtil:: serialize field rename function node.");
        auto fieldRenameFunction = Util::as<RenameLogicalFunction>(function);
        auto serializedFieldRenameFunction = SerializableFunction_FunctionFieldRename();
        serializeFunction(fieldRenameFunction->getOriginalField(), serializedFieldRenameFunction.mutable_functionoriginalfieldaccess());
        serializedFieldRenameFunction.set_newfieldname(fieldRenameFunction->getNewFieldName());
        serializedFunction->mutable_details()->PackFrom(serializedFieldRenameFunction);
    }
    else if (Util::instanceOf<FieldAssignmentBinaryLogicalFunction>(function))
    {
        /// serialize field assignment function node.
        NES_TRACE("FunctionSerializationUtil:: serialize field assignment function node.");
        auto fieldAssignmentLogicalFunction = Util::as<FieldAssignmentBinaryLogicalFunction>(function);
        auto serializedFieldAssignmentFunction = SerializableFunction_FunctionFieldAssignment();
        auto* serializedFieldAccessFunction = serializedFieldAssignmentFunction.mutable_field();
        serializedFieldAccessFunction->set_fieldname(fieldAssignmentLogicalFunction->getField()->getFieldName());
        DataTypeSerializationUtil::serializeDataType(
            fieldAssignmentLogicalFunction->getField()->getStamp(), serializedFieldAccessFunction->mutable_type());
        /// serialize assignment function
        serializeFunction(fieldAssignmentLogicalFunction->getAssignment(), serializedFieldAssignmentFunction.mutable_assignment());
        serializedFunction->mutable_details()->PackFrom(serializedFieldAssignmentFunction);
    }
    else if (Util::instanceOf<WhenBinaryLogicalFunction>(function))
    {
        /// serialize when function node.
        NES_TRACE("FunctionSerializationUtil:: serialize when function {}.", *function);
        auto whenLogicalFunction = Util::as<WhenBinaryLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionWhen();
        serializeFunction(whenLogicalFunction->getLeftChild(), serializedLogicalFunction.mutable_left());
        serializeFunction(whenLogicalFunction->getRightChild(), serializedLogicalFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else if (Util::instanceOf<CaseLogicalFunction>(function))
    {
        /// serialize case function node.
        NES_TRACE("FunctionSerializationUtil:: serialize case function {}.", *function);
        auto caseLogicalFunction = Util::as<CaseLogicalFunction>(function);
        auto serializedLogicalFunction = SerializableFunction_FunctionCase();
        for (const auto& elem : caseLogicalFunction->getWhenChildren())
        {
            serializeFunction(elem, serializedLogicalFunction.add_left());
        }
        serializeFunction(caseLogicalFunction->getDefaultExp(), serializedLogicalFunction.mutable_right());
        serializedFunction->mutable_details()->PackFrom(serializedLogicalFunction);
    }
    else
    {
        throw CannotSerialize(fmt::format("function: {}", *function));
    }

    DataTypeSerializationUtil::serializeDataType(function->getStamp(), serializedFunction->mutable_stamp());
    NES_DEBUG("FunctionSerializationUtil:: serialize function node to {}", serializedFunction->mutable_details()->type_url());
    return serializedFunction;
}

std::shared_ptr<LogicalFunction> FunctionSerializationUtil::deserializeFunction(const SerializableFunction& serializedFunction)
{
    NES_DEBUG("FunctionSerializationUtil:: deserialize function {}", serializedFunction.details().type_url());
    /// de-serialize function
    /// 1. check if the serialized function is a logical function
    if (serializedFunction.details().Is<SerializableFunction_FunctionAnd>())
    {
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as AND function node.");
        /// de-serialize and function node.
        auto serializedLogicalFunction = SerializableFunction_FunctionAnd();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        auto left = deserializeFunction(serializedLogicalFunction.left());
        auto right = deserializeFunction(serializedLogicalFunction.right());
        return AndBinaryLogicalFunction::create(left, right);
    }
    if (serializedFunction.details().Is<SerializableFunction_FunctionOr>())
    {
        /// de-serialize or function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as OR function node.");
        auto serializedLogicalFunction = SerializableFunction_FunctionOr();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        auto left = deserializeFunction(serializedLogicalFunction.left());
        auto right = deserializeFunction(serializedLogicalFunction.right());
        return OrBinaryLogicalFunction::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionLess>())
    {
        /// de-serialize less function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as LESS function node.");
        auto serializedLogicalFunction = SerializableFunction_FunctionLess();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        auto left = deserializeFunction(serializedLogicalFunction.left());
        auto right = deserializeFunction(serializedLogicalFunction.right());
        return LessBinaryLogicalFunction::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionLessEquals>())
    {
        /// de-serialize less equals function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as LESS Equals function node.");
        auto serializedLogicalFunction = SerializableFunction_FunctionLessEquals();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        auto left = deserializeFunction(serializedLogicalFunction.left());
        auto right = deserializeFunction(serializedLogicalFunction.right());
        return LessEqualsBinaryLogicalFunction::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionGreater>())
    {
        /// de-serialize greater function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as Greater function node.");
        auto serializedLogicalFunction = SerializableFunction_FunctionGreater();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        auto left = deserializeFunction(serializedLogicalFunction.left());
        auto right = deserializeFunction(serializedLogicalFunction.right());
        return GreaterBinaryLogicalFunction::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionGreaterEquals>())
    {
        /// de-serialize greater equals function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as GreaterEquals function node.");
        auto serializedLogicalFunction = SerializableFunction_FunctionGreaterEquals();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        auto left = deserializeFunction(serializedLogicalFunction.left());
        auto right = deserializeFunction(serializedLogicalFunction.right());
        return GreaterEqualsBinaryLogicalFunction::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionEquals>())
    {
        /// de-serialize equals function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as Equals function node.");
        auto serializedLogicalFunction = SerializableFunction_FunctionEquals();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        auto left = deserializeFunction(serializedLogicalFunction.left());
        auto right = deserializeFunction(serializedLogicalFunction.right());
        return EqualsBinaryLogicalFunction::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionNegate>())
    {
        /// de-serialize negate function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize logical function as Negate function node.");
        auto serializedLogicalFunction = SerializableFunction_FunctionNegate();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        auto child = deserializeFunction(serializedLogicalFunction.child());
        return NegateUnaryLogicalFunction::create(child);
    } else if (serializedFunction.details().Is<SerializableFunction_FunctionAdd>())
    {
        /// de-serialize ADD function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as Add function node.");
        auto serializedLogicalFunction = SerializableFunction_FunctionAdd();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        auto left = deserializeFunction(serializedLogicalFunction.left());
        auto right = deserializeFunction(serializedLogicalFunction.right());
        return AddBinaryLogicalFunction::create(left, right);
    }
    if (serializedFunction.details().Is<SerializableFunction_FunctionSub>())
    {
        /// de-serialize SUB function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as SUB function node.");
        auto serializedLogicalFunction = SerializableFunction_FunctionSub();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        auto left = deserializeFunction(serializedLogicalFunction.left());
        auto right = deserializeFunction(serializedLogicalFunction.right());
        return SubBinaryLogicalFunction::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionMul>())
    {
        /// de-serialize MUL function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as MUL function node.");
        auto serializedLogicalFunction = SerializableFunction_FunctionMul();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        auto left = deserializeFunction(serializedLogicalFunction.left());
        auto right = deserializeFunction(serializedLogicalFunction.right());
        return MulBinaryLogicalFunction::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionDiv>())
    {
        /// de-serialize DIV function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as DIV function node.");
        auto serializedLogicalFunction = SerializableFunction_FunctionDiv();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        auto left = deserializeFunction(serializedLogicalFunction.left());
        auto right = deserializeFunction(serializedLogicalFunction.right());
        return DivBinaryLogicalFunction::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionMod>())
    {
        /// de-serialize MODULO function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as MODULO function node.");
        auto serializedLogicalFunction = SerializableFunction_FunctionMod();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        auto left = deserializeFunction(serializedLogicalFunction.left());
        auto right = deserializeFunction(serializedLogicalFunction.right());
        return ModuloBinaryLogicalFunction::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionPow>())
    {
        /// de-serialize POWER function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as POWER function node.");
        auto serializedLogicalFunction = SerializableFunction_FunctionPow();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        auto left = deserializeFunction(serializedLogicalFunction.left());
        auto right = deserializeFunction(serializedLogicalFunction.right());
        return PowBinaryLogicalFunction::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionAbs>())
    {
        /// de-serialize ABS function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as ABS function node.");
        auto serializedLogicalFunction = SerializableFunction_FunctionAbs();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        auto child = deserializeFunction(serializedLogicalFunction.child());
        return AbsoluteUnaryLogicalFunction::create(child);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionCeil>())
    {
        /// de-serialize CEIL function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as CEIL function node.");
        auto serializedLogicalFunction = SerializableFunction_FunctionCeil();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        auto child = deserializeFunction(serializedLogicalFunction.child());
        return CeilUnaryLogicalFunction::create(child);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionExp>())
    {
        /// de-serialize EXP function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as EXP function node.");
        auto serializedLogicalFunction = SerializableFunction_FunctionExp();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        auto child = deserializeFunction(serializedLogicalFunction.child());
        return ExpUnaryLogicalFunction::create(child);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionFloor>())
    {
        /// de-serialize FLOOR function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as FLOOR function node.");
        auto serializedLogicalFunction = SerializableFunction_FunctionFloor();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        auto child = deserializeFunction(serializedLogicalFunction.child());
        return FloorUnaryLogicalFunction::create(child);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionRound>())
    {
        /// de-serialize ROUND function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as ROUND function node.");
        auto serializedLogicalFunction = SerializableFunction_FunctionRound();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        auto child = deserializeFunction(serializedLogicalFunction.child());
        return RoundUnaryLogicalFunction::create(child);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionSqrt>())
    {
        /// de-serialize SQRT function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize arithmetical function as SQRT function node.");
        auto serializedLogicalFunction = SerializableFunction_FunctionSqrt();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        auto child = deserializeFunction(serializedLogicalFunction.child());
        return SqrtUnaryLogicalFunction::create(child);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionConstantValue>())
    {
        /// de-serialize constant value function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize function as Constant Value function node.");
        auto serializedConstantValue = SerializableFunction_FunctionConstantValue();
        serializedFunction.details().UnpackTo(&serializedConstantValue);
        /// The data type stored in the function's stamp is equal to the datatype of the value
        auto valueDataType = DataTypeSerializationUtil::deserializeDataType(serializedFunction.stamp());
        return ConstantValueLogicalFunction::create(valueDataType, serializedConstantValue.value());
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionFieldAccess>())
    {
        /// de-serialize field access function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize function as FieldAccess function node.");
        SerializableFunction_FunctionFieldAccess serializedFieldAccessFunction;
        serializedFunction.details().UnpackTo(&serializedFieldAccessFunction);
        const auto& name = serializedFieldAccessFunction.fieldname();
        return FieldAccessLogicalFunction::create(name);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionFieldRename>())
    {
        /// de-serialize field rename function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize function as Field Rename function node.");
        SerializableFunction_FunctionFieldRename serializedFieldRenameFunction;
        serializedFunction.details().UnpackTo(&serializedFieldRenameFunction);
        auto originalFieldAccessFunction = deserializeFunction(serializedFieldRenameFunction.functionoriginalfieldaccess());
        if (!Util::instanceOf<FieldAccessLogicalFunction>(originalFieldAccessFunction))
        {
            throw CannotDeserialize(fmt::format(
                "FunctionSerializationUtil: the original field access function should be of type FieldAccessLogicalFunction,"
                "but was a {}",
                *originalFieldAccessFunction));
        }
        const auto& newFieldName = serializedFieldRenameFunction.newfieldname();
        return RenameLogicalFunction::create(Util::as<FieldAccessLogicalFunction>(originalFieldAccessFunction), newFieldName);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionFieldAssignment>())
    {
        /// de-serialize field read function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize function as FieldAssignment function node.");
        SerializableFunction_FunctionFieldAssignment serializedFieldAccessFunction;
        serializedFunction.details().UnpackTo(&serializedFieldAccessFunction);
        const auto* field = serializedFieldAccessFunction.mutable_field();
        auto fieldStamp = DataTypeSerializationUtil::deserializeDataType(field->type());
        auto fieldAccessNode = FieldAccessLogicalFunction::create(fieldStamp, field->fieldname());
        auto fieldAssignmentFunction = deserializeFunction(serializedFieldAccessFunction.assignment());
        return FieldAssignmentBinaryLogicalFunction::create(Util::as<FieldAccessLogicalFunction>(fieldAccessNode), fieldAssignmentFunction);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionWhen>())
    {
        /// de-serialize WHEN function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize function as When function node.");
        auto serializedLogicalFunction = SerializableFunction_FunctionWhen();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        auto left = deserializeFunction(serializedLogicalFunction.left());
        auto right = deserializeFunction(serializedLogicalFunction.right());
        return WhenBinaryLogicalFunction::create(left, right);
    }
    else if (serializedFunction.details().Is<SerializableFunction_FunctionCase>())
    {
        /// de-serialize CASE function node.
        NES_TRACE("FunctionSerializationUtil:: de-serialize function as Case function node.");
        auto serializedLogicalFunction = SerializableFunction_FunctionCase();
        serializedFunction.details().UnpackTo(&serializedLogicalFunction);
        std::vector<std::shared_ptr<LogicalFunction>> leftExps;

        ///todo: deserialization might be possible more efficiently
        for (int i = 0; i < serializedLogicalFunction.left_size(); i++)
        {
            const auto& leftNode = serializedLogicalFunction.left(i);
            leftExps.push_back(deserializeFunction(leftNode));
        }
            auto right = deserializeFunction(serializedLogicalFunction.right());
            return CaseLogicalFunction::create(leftExps, right);
        }
        else
        {
            throw CannotDeserialize("FunctionSerializationUtil: could not de-serialize this function");
        }
    }
}
