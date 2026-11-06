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

#include <Functions/LowerPhysicalFunction.hpp>

#include <cstdint>
#include <span>
#include <utility>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <select.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>

namespace NES
{

namespace
{
/// Copies `size` bytes from `inputPtr` to `outputPtr`, mapping the ASCII letters A-Z to a-z.
uint64_t toLowerAscii(int8_t* inputPtr, const uint64_t size, int8_t* outputPtr)
{
    const std::span input{inputPtr, size};
    const std::span output{outputPtr, size};
    for (uint64_t i = 0; i < size; ++i)
    {
        const auto byte = input[i];
        output[i] = (byte >= 'A' && byte <= 'Z') ? static_cast<int8_t>(byte - 'A' + 'a') : byte;
    }
    return size;
}
}

LowerPhysicalFunction::LowerPhysicalFunction(PhysicalFunction childFunction) : childFunction(std::move(childFunction))
{
}

VarVal LowerPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto value = childFunction.execute(record, arena);
    const auto nullable = value.isNullable();
    nautilus::val<bool> isNull = false;
    if (nullable)
    {
        isNull = value.isNull();
    }

    const auto input = value.getRawValueAs<VariableSizedData>();
    const auto output = arena.allocateVariableSizedData(nautilus::select(isNull, nautilus::val<uint64_t>{0}, input.getSize()));
    if (isNull)
    {
        return VarVal{output, nullable, isNull};
    }
    const auto size = nautilus::invoke(toLowerAscii, input.getContent(), input.getSize(), output.getContent());
    return VarVal{VariableSizedData(output.getContent(), size), nullable, isNull};
}

/// NOLINTNEXTLINE(readability-identifier-naming)
PhysicalFunctionRegistryReturnType LowerPhysicalFunction::createLOWER(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 1, "LOWER function must have exactly one child function");
    return LowerPhysicalFunction(arguments.childFunctions[0]);
}
}
