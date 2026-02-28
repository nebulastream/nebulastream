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

#include <Functions/BooleanFunctions/AndPhysicalFunction.hpp>

#include <utility>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <select.hpp>
#include <val_bool.hpp>

namespace NES
{

VarVal AndPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto leftValue = leftPhysicalFunction.execute(record, arena);
    const auto rightValue = rightPhysicalFunction.execute(record, arena);

    /// Any expression involving null results in NULL, except for NULL and False.
    /// As NULL and False can be determined without evaluation the NULL value.
    const auto specialCondition = (leftValue.cast<nautilus::val<bool>>() == false and not leftValue.isNull())
        or (rightValue.cast<nautilus::val<bool>>() == false and not rightValue.isNull());
    const auto newValue
        = nautilus::select(specialCondition, nautilus::val<bool>{false}, (leftValue && rightValue).cast<nautilus::val<bool>>());
    const auto newNull = nautilus::select(specialCondition, nautilus::val<bool>{false}, leftValue.isNull() or rightValue.isNull());
    return VarVal{newValue, leftValue.isNullable() or rightValue.isNullable(), newNull};
}

AndPhysicalFunction::AndPhysicalFunction(PhysicalFunction leftPhysicalFunction, PhysicalFunction rightPhysicalFunction)
    : leftPhysicalFunction(std::move(leftPhysicalFunction)), rightPhysicalFunction(std::move(rightPhysicalFunction))
{
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterAndPhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 2, "And function must have exactly two child functions");
    return AndPhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0], physicalFunctionRegistryArguments.childFunctions[1]);
}

}
