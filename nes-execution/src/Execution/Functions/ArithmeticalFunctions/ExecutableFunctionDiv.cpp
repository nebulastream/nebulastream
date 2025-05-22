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
#include <Execution/Functions/ArithmeticalFunctions/ExecutableFunctionDiv.hpp>
#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <ExecutableFunctionRegistry.hpp>

namespace NES::Runtime::Execution::Functions
{

VarVal ExecutableFunctionDiv::execute(const Record& record, ArenaRef& arena) const
{
    const auto leftValue = leftExecutableFunction->execute(record, arena);
    const auto rightValue = rightExecutableFunction->execute(record, arena);
    return leftValue / rightValue;
}

ExecutableFunctionDiv::ExecutableFunctionDiv(
    std::unique_ptr<Function> leftExecutableFunction, std::unique_ptr<Function> rightExecutableFunction)
    : leftExecutableFunction(std::move(leftExecutableFunction)), rightExecutableFunction(std::move(rightExecutableFunction))
{
}

ExecutableFunctionRegistryReturnType
ExecutableFunctionGeneratedRegistrar::RegisterDivExecutableFunction(ExecutableFunctionRegistryArguments executableFunctionRegistryArguments)
{
    PRECONDITION(executableFunctionRegistryArguments.childFunctions.size() == 2, "Div function must have exactly two sub-functions");
    return std::make_unique<ExecutableFunctionDiv>(
        std::move(executableFunctionRegistryArguments.childFunctions[0]), std::move(executableFunctionRegistryArguments.childFunctions[1]));
}

}
