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

#include <cstdint>
#include <memory>
#include <utility>
#include <Execution/Functions/ExecutableFunctionConcat.hpp>
#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/std/cstring.h>
#include <ErrorHandling.hpp>
#include <ExecutableFunctionRegistry.hpp>
#include <val.hpp>

namespace NES::Runtime::Execution::Functions
{

ExecutableFunctionConcat::ExecutableFunctionConcat(
    std::unique_ptr<Function> leftExecutableFunction, std::unique_ptr<Function> rightExecutableFunction)
    : leftExecutableFunction(std::move(leftExecutableFunction)), rightExecutableFunction(std::move(rightExecutableFunction))
{
}

VarVal ExecutableFunctionConcat::execute(const Record& record, ArenaRef& arena) const
{
    const auto leftValue = leftExecutableFunction->execute(record, arena).cast<VariableSizedData>();
    const auto rightValue = rightExecutableFunction->execute(record, arena).cast<VariableSizedData>();

    const auto newSize = leftValue.getContentSize() + rightValue.getContentSize();
    auto newVarSizeData = arena.allocateVariableSizedData(newSize);

    /// Writing the left value and then the right value to the new variable sized data
    nautilus::memcpy(newVarSizeData.getContent(), leftValue.getContent(), leftValue.getContentSize());
    nautilus::memcpy(newVarSizeData.getContent() + leftValue.getContentSize(), rightValue.getContent(), rightValue.getContentSize());
    VarVal(nautilus::val<uint32_t>(newSize)).writeToMemory(newVarSizeData.getReference());
    return newVarSizeData;
}

ExecutableFunctionRegistryReturnType ExecutableFunctionGeneratedRegistrar::RegisterConcatExecutableFunction(
    ExecutableFunctionRegistryArguments executableFunctionRegistryArguments)
{
    PRECONDITION(executableFunctionRegistryArguments.childFunctions.size() == 2, "And function must have exactly two sub-functions");
    return std::make_unique<ExecutableFunctionConcat>(
        std::move(executableFunctionRegistryArguments.childFunctions[0]), std::move(executableFunctionRegistryArguments.childFunctions[1]));
}
}
