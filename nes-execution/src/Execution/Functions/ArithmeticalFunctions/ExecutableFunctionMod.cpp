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
#include <Execution/Functions/ArithmeticalFunctions/ExecutableFunctionMod.hpp>
#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <ExecutableFunctionRegistry.hpp>

namespace NES::Runtime::Execution::Functions
{

VarVal ExecutableFunctionMod::execute(const Record& record, ArenaRef& arena) const
{
    const auto leftValue = leftExecutableFunctionSub->execute(record, arena);
    const auto rightValue = rightExecutableFunctionSub->execute(record, arena);
    return leftValue % rightValue;
}

ExecutableFunctionMod::ExecutableFunctionMod(
    std::unique_ptr<Function> leftExecutableFunctionSub, std::unique_ptr<Function> rightExecutableFunctionSub)
    : leftExecutableFunctionSub(std::move(leftExecutableFunctionSub)), rightExecutableFunctionSub(std::move(rightExecutableFunctionSub))
{
}

ExecutableFunctionRegistryReturnType
ExecutableFunctionGeneratedRegistrar::RegisterModExecutableFunction(ExecutableFunctionRegistryArguments executableFunctionRegistryArguments)
{
    PRECONDITION(executableFunctionRegistryArguments.childFunctions.size() == 2, "Mod function must have exactly two sub-functions");
    return std::make_unique<ExecutableFunctionMod>(
        std::move(executableFunctionRegistryArguments.childFunctions[0]), std::move(executableFunctionRegistryArguments.childFunctions[1]));
}

}
