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
#include <Functions/LogicalFunctions/OrPhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES::Functions
{

VarVal OrPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto leftValue = leftPhysicalFunction->execute(record, arena);
    const auto rightValue = rightPhysicalFunction->execute(record, arena);
    return leftValue || rightValue;
}

OrPhysicalFunction::OrPhysicalFunction(
    std::unique_ptr<PhysicalFunction> leftPhysicalFunction, std::unique_ptr<PhysicalFunction> rightPhysicalFunction)
    : leftPhysicalFunction(std::move(leftPhysicalFunction)), rightPhysicalFunction(std::move(rightPhysicalFunction))
{
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterOrPhysicalFunction(PhysicalFunctionRegistryArguments PhysicalFunctionRegistryArguments)
{
    PRECONDITION(PhysicalFunctionRegistryArguments.childFunctions.size() == 2, "Or function must have exactly two sub-functions");
    return std::make_unique<OrPhysicalFunction>(
        std::move(PhysicalFunctionRegistryArguments.childFunctions[0]), std::move(PhysicalFunctionRegistryArguments.childFunctions[1]));
}

}
