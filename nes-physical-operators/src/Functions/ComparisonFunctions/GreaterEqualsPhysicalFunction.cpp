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

#include <memory>
#include <utility>
#include <vector>
#include <Functions/ComparisonFunctions/GreaterEqualsPhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>


namespace NES::Functions
{

VarVal GreaterEqualsPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto leftValue = leftPhysicalFunction.execute(record, arena);
    const auto rightValue = rightPhysicalFunction.execute(record, arena);
    return leftValue >= rightValue;
}

GreaterEqualsPhysicalFunction::GreaterEqualsPhysicalFunction(PhysicalFunction leftPhysicalFunction, PhysicalFunction rightPhysicalFunction)
    : leftPhysicalFunction(leftPhysicalFunction), rightPhysicalFunction(rightPhysicalFunction)
{
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterGreaterEqualsPhysicalFunction(
    PhysicalFunctionRegistryArguments PhysicalFunctionRegistryArguments)
{
    PRECONDITION(
        PhysicalFunctionRegistryArguments.childFunctions.size() == 2, "GreaterEquals function must have exactly two sub-functions");
    return GreaterEqualsPhysicalFunction(
        PhysicalFunctionRegistryArguments.childFunctions[0], PhysicalFunctionRegistryArguments.childFunctions[1]);
}

}
