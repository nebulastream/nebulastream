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
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{
AbsolutePhysicalFunction::AbsolutePhysicalFunction(PhysicalFunction childFunction, DataType inputType)
    : childFunction(std::move(childFunction)), inputType(std::move(inputType))
{
}

VarVal AbsolutePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto value = childFunction.execute(record, arena);
    if (not inputType.isSignedInteger() and not inputType.isFloat())
    {
        return value;
    }

    /// We need to built a zero and negativeOne via castToType, as we can not make any assumptions on the input type.
    const auto zero = VarVal{0}.castToType(inputType.type);
    const auto negativeOne = VarVal{-1}.castToType(inputType.type);
    if (value < zero)
    {
        return value * negativeOne;
    }
    return value;
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterAbsPhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 1, "Absolute function must have exactly one child function");
    PRECONDITION(physicalFunctionRegistryArguments.inputTypes.size() == 1, "Absolute function must have exactly one input type");
    return AbsolutePhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0], physicalFunctionRegistryArguments.inputTypes[0]);
}


}
