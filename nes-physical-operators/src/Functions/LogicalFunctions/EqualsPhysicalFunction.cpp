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
#include <Functions/LogicalFunctions/EqualsPhysicalFunction.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_enum.hpp>
#include <ErrorHandling.hpp>
#include <ExecutableFunctionRegistry.hpp>
#include <function.hpp>


namespace NES::Functions
{

VarVal EqualsPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto leftValue = leftExecutableFunction->execute(record, arena);
    const auto rightValue = rightExecutableFunction->execute(record, arena);
    const auto result = leftValue == rightValue;
    return result;
}

EqualsPhysicalFunction::EqualsPhysicalFunction(
    std::unique_ptr<PhysicalFunction> leftExecutableFunction, std::unique_ptr<PhysicalFunction> rightExecutableFunction)
    : leftExecutableFunction(std::move(leftExecutableFunction)), rightExecutableFunction(std::move(rightExecutableFunction))
{
}

ExecutableFunctionRegistryReturnType ExecutableFunctionGeneratedRegistrar::RegisterEqualsExecutableFunction(
    ExecutableFunctionRegistryArguments executableFunctionRegistryArguments)
{
    PRECONDITION(executableFunctionRegistryArguments.childFunctions.size() == 2, "Equals function must have exactly two sub-functions");
    return std::make_unique<EqualsPhysicalFunction>(
        std::move(executableFunctionRegistryArguments.childFunctions[0]), std::move(executableFunctionRegistryArguments.childFunctions[1]));
}

}
