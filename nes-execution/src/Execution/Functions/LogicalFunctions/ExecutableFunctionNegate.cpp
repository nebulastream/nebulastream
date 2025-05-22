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
#include <Execution/Functions/Function.hpp>
#include <Execution/Functions/LogicalFunctions/ExecutableFunctionNegate.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <ExecutableFunctionRegistry.hpp>

namespace NES::Runtime::Execution::Functions
{

VarVal ExecutableFunctionNegate::execute(const Record& record, ArenaRef& arena) const
{
    const auto value = childFunction->execute(record, arena);
    return !value;
}

ExecutableFunctionNegate::ExecutableFunctionNegate(std::unique_ptr<Function> childFunction) : childFunction(std::move(childFunction))
{
}

ExecutableFunctionRegistryReturnType ExecutableFunctionGeneratedRegistrar::RegisterNegateExecutableFunction(
    ExecutableFunctionRegistryArguments executableFunctionRegistryArguments)
{
    PRECONDITION(executableFunctionRegistryArguments.childFunctions.size() == 1, "Negate function must have exactly one sub-function");
    return std::make_unique<ExecutableFunctionNegate>(std::move(executableFunctionRegistryArguments.childFunctions[0]));
}

}
