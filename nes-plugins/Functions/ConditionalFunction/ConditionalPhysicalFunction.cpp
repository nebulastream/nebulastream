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

#include <ConditionalPhysicalFunction.hpp>

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
#include <val_bool.hpp>

namespace NES
{

ConditionalPhysicalFunction::ConditionalPhysicalFunction(std::vector<PhysicalFunction> inputFns) : elseFn(inputFns.back())
{
    inputFns.pop_back();

    whenThenFns.reserve(inputFns.size() / 2);
    for (std::size_t i = 0; i + 1 < inputFns.size(); i += 2)
    {
        whenThenFns.emplace_back(inputFns[i], inputFns[i + 1]);
    }
}

VarVal ConditionalPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto result = elseFn.execute(record, arena);

    for (std::size_t i = whenThenFns.size(); i-- > 0;)
    {
        const auto& [conditionFn, thenFn] = whenThenFns.at(i);
        const auto conditionValue = conditionFn.execute(record, arena);
        const auto branchSelected = not conditionValue.isNull() and conditionValue.getRawValueAs<nautilus::val<bool>>();
        result = VarVal::select(branchSelected, thenFn.execute(record, arena), result);
    }

    return result;
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterConditionalPhysicalFunction(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(
        arguments.childFunctions.size() >= 3,
        "Conditional function must have at least 3 child functions: one condition, one result, and one default");
    PRECONDITION(
        arguments.childFunctions.size() % 2 == 1,
        "Conditional function must have an odd number of child functions: WHEN/THEN pairs plus one default");

    for (std::size_t i = 0; i + 1 < arguments.inputTypes.size() - 1; i += 2)
    {
        PRECONDITION(arguments.inputTypes[i].isType(DataType::Type::BOOLEAN), "All Conditional conditions must have type BOOLEAN");
    }

    const auto& outputType = arguments.outputType;
    for (std::size_t i = 1; i + 1 < arguments.inputTypes.size(); i += 2)
    {
        PRECONDITION(arguments.inputTypes[i] == outputType, "All Conditional result expressions must have the same type");
    }

    return ConditionalPhysicalFunction(std::move(arguments.childFunctions));
}

}
