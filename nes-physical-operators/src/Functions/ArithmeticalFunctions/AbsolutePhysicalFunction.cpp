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

#include <cstdint>
#include <limits>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <function.hpp>
#include <val_arith.hpp>
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

    /// Unsigned integers are a no-op: the magnitude equals the value and the result type is unchanged.
    if (not inputType.isSignedInteger() and not inputType.isFloat())
    {
        return value.castToType(outputType.type);
    }

    /// Signed integer types are widened by one rank during schema inference so the magnitude of the minimum value is representable
    /// (e.g. ABS(INT8 -128) = INT16 128). INT64 cannot be widened, so ABS(INT64_MIN) is not representable and must raise a runtime error.
    if (inputType.isType(DataType::Type::INT64))
    {
        const auto rawValue = value.getRawValueAs<nautilus::val<int64_t>>();
        const auto isMinValue = rawValue == nautilus::val<int64_t>{std::numeric_limits<int64_t>::min()};
        /// A NULL value carries arbitrary raw bits, so only raise the overflow error when the value is INT64_MIN and non-null.
        if (isMinValue and not value.isNull())
        {
            nautilus::invoke(+[] { throw ArithmeticalError("ABS() of INT64_MIN overflows"); });
        }
    }

    /// Cast to the widened output type first, so that negating the minimum value does not overflow.
    /// Non-const so the `return widenedValue` path can move rather than copy (performance-no-automatic-move).
    auto widenedValue = value.castToType(outputType.type);
    const auto zero = VarVal{0}.castToType(outputType.type);
    const auto negativeOne = VarVal{-1}.castToType(outputType.type);
    if (widenedValue < zero)
    {
        return widenedValue * negativeOne;
    }
    return widenedValue;
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterAbsPhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 1, "Absolute function must have exactly one child function");
    PRECONDITION(physicalFunctionRegistryArguments.inputTypes.size() == 1, "Absolute function must have exactly one input type");
    return AbsolutePhysicalFunction(
        physicalFunctionRegistryArguments.childFunctions[0],
        physicalFunctionRegistryArguments.inputTypes[0],
        physicalFunctionRegistryArguments.outputType);
}


}
