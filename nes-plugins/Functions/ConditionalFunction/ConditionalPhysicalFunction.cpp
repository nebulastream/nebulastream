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

#include <cstddef>
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
    for (size_t i = 0; i + 1 < inputFns.size(); i += 2)
    {
        whenThenFns.emplace_back(inputFns[i], inputFns[i + 1]);
    }
}

VarVal ConditionalPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    for (nautilus::static_val<size_t> i = 0; i < whenThenFns.size(); ++i)
    {
        if (whenThenFns.at(i).first.execute(record, arena))
        {
            return whenThenFns.at(i).second.execute(record, arena);
        }
    }
    return elseFn.execute(record, arena);
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

    const auto& inputTypes = arguments.inputTypes;
    for (size_t i = 0; i + 1 < inputTypes.size() - 1; i += 2)
    {
        PRECONDITION(inputTypes[i].isType(DataType::Type::BOOLEAN), "All condition functions must have type BOOLEAN");
    }

    const auto& outputType = inputTypes.back();
    for (size_t i = 1; i + 1 < inputTypes.size() - 1; i += 2)
    {
        PRECONDITION(inputTypes[i] == outputType, "All result functions must have the same type");
    }

    return ConditionalPhysicalFunction(std::move(arguments.childFunctions));
}
}
