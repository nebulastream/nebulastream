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

#include <Functions/ArithmeticalFunctions/FloorPhysicalFunction.hpp>

#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <std/cmath.h>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <val.hpp>

namespace NES
{
FloorPhysicalFunction::FloorPhysicalFunction(PhysicalFunction childFunction, DataType inputType, DataType outputType)
    : childFunction(std::move(childFunction)), inputType(std::move(inputType)), outputType(std::move(outputType))
{
}

VarVal FloorPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto value = childFunction.execute(record, arena);
    /// Floats are floored in double precision and cast back to the input's float type.
    if (inputType.isFloat())
    {
        const auto flooredValue = nautilus::floor(value.getRawValueAs<nautilus::val<double>>());
        /// Reattach the null state, as getRawValueAs() strips it.
        return VarVal{flooredValue, value.isNullable(), value.isNull()}.castToType(outputType.type);
    }
    /// Integers are already integral and type inference guarantees outputType == inputType, so FLOOR is the identity.
    return value;
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterFloorPhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 1, "Floor function must have exactly one child function");
    PRECONDITION(physicalFunctionRegistryArguments.inputTypes.size() == 1, "Floor function must have exactly one input type");
    return FloorPhysicalFunction(
        physicalFunctionRegistryArguments.childFunctions[0],
        physicalFunctionRegistryArguments.inputTypes[0],
        physicalFunctionRegistryArguments.outputType);
}

}
