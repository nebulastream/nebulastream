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

#include <Functions/ConditionalPhysicalFunction.hpp>

#include <iterator>
#include <utility>
#include <vector>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <static.hpp>

namespace NES
{
ConditionalPhysicalFunction::ConditionalPhysicalFunction(std::vector<PhysicalFunction> childFunctions) : elseCase{childFunctions.back()}
{
    /// The trailing child is the default result; the remaining children form (when, then) pairs.
    childFunctions.pop_back();
    whenThens.reserve(childFunctions.size() / 2);
    for (auto when = childFunctions.begin(); when != childFunctions.end(); when += 2)
    {
        whenThens.push_back(WhenThenPhysicalFunction{.when = *when, .then = *std::next(when)});
    }
}

VarVal ConditionalPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    for (const auto& [when, then] : nautilus::static_iterable(whenThens))
    {
        if (when.execute(record, arena))
        {
            return then.execute(record, arena);
        }
    }
    return elseCase.execute(record, arena);
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterConditionalPhysicalFunction(PhysicalFunctionRegistryArguments arguments)
{
    /// The logical function already validated the argument shape and types, so a bad shape here is a lowering bug.
    PRECONDITION(
        arguments.childFunctions.size() >= 3 and arguments.childFunctions.size() % 2 == 1,
        "ConditionalPhysicalFunction requires an odd number of child functions >= 3 (when/then pairs plus a default), but got {}",
        arguments.childFunctions.size());
    return ConditionalPhysicalFunction{std::move(arguments.childFunctions)};
}
}
