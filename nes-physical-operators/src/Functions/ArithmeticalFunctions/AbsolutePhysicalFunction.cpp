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

#include <Functions/ArithmeticalFunctions/AbsolutePhysicalFunction.hpp>

#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <val_bool.hpp>

namespace NES
{
AbsolutePhysicalFunction::AbsolutePhysicalFunction(PhysicalFunction childFunction, DataType inputType, DataType outputType)
    : childFunction(std::move(childFunction)), inputType(std::move(inputType)), outputType(std::move(outputType))
{
}

VarVal AbsolutePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto value = childFunction.execute(record, arena);
    if (not inputType.isSignedInteger() and not inputType.isFloat())
    {
        return value.castToType(outputType.type);
    }

    /// We need to built a zero and negativeOne via castToType, as we can not make any assumptions on the input type.
    /// Both select branches must have the same type: integer promotion widens value * negativeOne (e.g. int8 -> int),
    /// so we cast each branch to the output type before selecting, which also produces the required result type.
    const auto zero = VarVal{0}.castToType(inputType.type);
    const auto negativeOne = VarVal{-1}.castToType(inputType.type);
    return VarVal::select(
        (value < zero).getRawValueAs<nautilus::val<bool>>(),
        (value * negativeOne).castToType(outputType.type),
        value.castToType(outputType.type));
}

PhysicalFunctionRegistryReturnType AbsolutePhysicalFunction::createAbs(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 1, "Absolute function must have exactly one child function");
    PRECONDITION(physicalFunctionRegistryArguments.inputTypes.size() == 1, "Absolute function must have exactly one input type");
    return AbsolutePhysicalFunction(
        physicalFunctionRegistryArguments.childFunctions[0],
        physicalFunctionRegistryArguments.inputTypes[0],
        physicalFunctionRegistryArguments.outputType);
}


}
