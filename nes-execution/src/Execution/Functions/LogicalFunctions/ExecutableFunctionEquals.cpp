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

#include <Execution/Functions/Function.hpp>
#include <Execution/Functions/LogicalFunctions/ExecutableFunctionEquals.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/Execution.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_enum.hpp>
#include <ErrorHandling.hpp>
#include <ExecutableFunctionRegistry.hpp>
#include <function.hpp>

namespace NES::Runtime::Execution::Functions
{

VarVal ExecutableFunctionEquals::execute(const Record& record, ArenaRef& arena) const
{
    const auto leftValue = leftExecutableFunction->execute(record, arena);
    const auto rightValue = rightExecutableFunction->execute(record, arena);
    const auto result = leftValue == rightValue;
    return result;
}

ExecutableFunctionEquals::ExecutableFunctionEquals(
    std::unique_ptr<Function> leftExecutableFunction, std::unique_ptr<Function> rightExecutableFunction)
    : leftExecutableFunction(std::move(leftExecutableFunction)), rightExecutableFunction(std::move(rightExecutableFunction))
{
}

ExecutableFunctionRegistryReturnType ExecutableFunctionGeneratedRegistrar::RegisterEqualsExecutableFunction(
    ExecutableFunctionRegistryArguments executableFunctionRegistryArguments)
{
    PRECONDITION(executableFunctionRegistryArguments.childFunctions.size() == 2, "Equals function must have exactly two sub-functions");
    return std::make_unique<ExecutableFunctionEquals>(
        std::move(executableFunctionRegistryArguments.childFunctions[0]), std::move(executableFunctionRegistryArguments.childFunctions[1]));
}

}
