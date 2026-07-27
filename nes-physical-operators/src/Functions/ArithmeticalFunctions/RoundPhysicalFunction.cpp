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

#include <Functions/ArithmeticalFunctions/RoundPhysicalFunction.hpp>

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
RoundPhysicalFunction::RoundPhysicalFunction(PhysicalFunction childFunction, DataType inputType, DataType outputType)
    : childFunction(std::move(childFunction)), inputType(std::move(inputType)), outputType(std::move(outputType))
{
}

VarVal RoundPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto value = childFunction.execute(record, arena);
    /// Floats are rounded in double precision and cast back to the input's float type. nautilus::round follows std::round, so ties
    /// round half away from zero (ROUND(2.5) = 3, ROUND(-2.5) = -3), matching PostgreSQL (numeric), DuckDB, MySQL, and SQLite.
    if (inputType.isFloat())
    {
        const auto roundedValue = nautilus::round(value.getRawValueAs<nautilus::val<double>>());
        /// Reattach the null state, as getRawValueAs() strips it.
        return VarVal{roundedValue, value.isNullable(), value.isNull()}.castToType(outputType.type);
    }
    /// Integers are already integral and type inference guarantees outputType == inputType, so ROUND is the identity.
    return value;
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterRoundPhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 1, "Round function must have exactly one child function");
    PRECONDITION(physicalFunctionRegistryArguments.inputTypes.size() == 1, "Round function must have exactly one input type");
    return RoundPhysicalFunction(
        physicalFunctionRegistryArguments.childFunctions[0],
        physicalFunctionRegistryArguments.inputTypes[0],
        physicalFunctionRegistryArguments.outputType);
}

}
