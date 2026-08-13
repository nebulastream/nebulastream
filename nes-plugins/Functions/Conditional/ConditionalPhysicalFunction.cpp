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
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <static.hpp>

namespace NES
{
namespace
{
/// Groups the leading elements of the flat child list into (when, then) pairs, leaving out the
/// trailing default result.
std::vector<WhenThenPhysicalFunction> groupWhenThens(const std::vector<PhysicalFunction>& children)
{
    std::vector<WhenThenPhysicalFunction> whenThens;
    whenThens.reserve(children.size() / 2);
    for (auto when = children.begin(); when != std::prev(children.end()); when += 2)
    {
        whenThens.push_back(WhenThenPhysicalFunction{.when = *when, .then = *std::next(when)});
    }
    return whenThens;
}

/// Branches may disagree on whether they can produce a null, while the expression as a whole has
/// one declared type. Rebuilding the result pins it to that type instead of to the branch that
/// produced it.
VarVal withDeclaredType(const VarVal& value, const DataType& resultType)
{
    return value.customVisit([&]<typename T>(const T& raw) { return VarVal{raw, resultType.nullable, value.isNull()}; });
}
}

ConditionalPhysicalFunction::ConditionalPhysicalFunction(
    std::vector<WhenThenPhysicalFunction> whenThens, PhysicalFunction elseCase, DataType resultType)
    : whenThens(std::move(whenThens)), elseCase(std::move(elseCase)), resultType(std::move(resultType))
{
}

/// Every branch returns from inside the loop, and the loop is unrolled while the query is traced, so every branch adds an exit
/// to the trace. At 100 branches that is about 2.7 times the IR blocks of a variant with a single exit. That is fine for the
/// branch counts that queries use in practice, but nesting multiplies them, so this may need optimizing later.
VarVal ConditionalPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    for (const auto& [when, then] : nautilus::static_iterable(whenThens))
    {
        /// A condition that is null is not true, so it does not select its branch. Converting the
        /// condition to a bool already accounts for that.
        if (when.execute(record, arena))
        {
            return withDeclaredType(then.execute(record, arena), resultType);
        }
    }
    return withDeclaredType(elseCase.execute(record, arena), resultType);
}

PhysicalFunctionRegistryReturnType ConditionalPhysicalFunction::createConditional(PhysicalFunctionRegistryArguments arguments)
{
    /// The logical function already validated the argument shape and types, so a bad shape here is a lowering bug.
    PRECONDITION(
        arguments.childFunctions.size() >= 3 and arguments.childFunctions.size() % 2 == 1,
        "ConditionalPhysicalFunction requires an odd number of child functions >= 3 (when/then pairs plus a default), but got {}",
        arguments.childFunctions.size());
    return ConditionalPhysicalFunction{groupWhenThens(arguments.childFunctions), arguments.childFunctions.back(), arguments.outputType};
}
}
