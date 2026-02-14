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

#include "ConditionalPhysicalFunction.hpp"

#include <algorithm>
#include <cstddef>
#include <ranges>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <static.hpp>

namespace NES
{
ConditionalPhysicalFunction::ConditionalPhysicalFunction(std::vector<PhysicalFunction> inputFns) : elseFn(inputFns.back())
{
    inputFns.pop_back();

    whenThenFns.reserve(inputFns.size() / 2);
    for (const auto chunks = inputFns | std::views::chunk(2); const auto& pair : chunks)
    {
        whenThenFns.emplace_back(pair[0], pair[1]);
    }
}

VarVal ConditionalPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    /**
     * TODO: evaluate this function for an incoming record.
     *       hint: for primitive types, use nautilus::static_val<size_t> instead of size_t.
     */
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterConditionalPhysicalFunction(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(
        arguments.childFunctions.size() >= 3,
        "Conditional function must have at least 3 child functions: one condition, one return function, and one default function");
    PRECONDITION(
        arguments.childFunctions.size() % 2 == 1,
        "Conditional function must have an odd number of child functions: an even number of WHEN/THEN pairs, and one default function");

    const auto withoutDefault = arguments.inputTypes | std::views::take(arguments.inputTypes.size() - 1);
    const auto conditionFnTypes = withoutDefault | std::views::stride(2);
    PRECONDITION(
        std::ranges::all_of(conditionFnTypes, [](const auto& dataType) -> bool { return dataType.isType(DataType::Type::BOOLEAN); }),
        "All condition functions must have type BOOLEAN");

    const auto resultFnTypes = withoutDefault | std::views::drop(1) | std::views::stride(2);
    const auto& outputType = arguments.inputTypes.back();
    PRECONDITION(
        std::ranges::all_of(resultFnTypes, [&outputType](const auto& dataType) -> bool { return dataType == outputType; }),
        "All result functions must have the same type");

    return ConditionalPhysicalFunction(std::move(arguments.childFunctions));
}
}
