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

#pragma once
#ifndef INCLUDED_FROM_REGISTRY_FUNCTION_EXECUTABLE
#    error "This file should not be included directly! " \
"Include instead include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>"
#endif


namespace NES
{

namespace Runtime::Execution::Functions
{
/// Defining all arithmetic supported functions
std::unique_ptr<Function> RegisterExecutableFunctionAdd(std::vector<std::unique_ptr<Functions::Function>> childFunctions);
std::unique_ptr<Function> RegisterExecutableFunctionMul(std::vector<std::unique_ptr<Functions::Function>> childFunctions);
std::unique_ptr<Function> RegisterExecutableFunctionSub(std::vector<std::unique_ptr<Functions::Function>> childFunctions);
std::unique_ptr<Function> RegisterExecutableFunctionDiv(std::vector<std::unique_ptr<Functions::Function>> childFunctions);

/// Defining all logical supported functions
std::unique_ptr<Function> RegisterExecutableFunctionEquals(std::vector<std::unique_ptr<Functions::Function>> childFunctions);
std::unique_ptr<Function> RegisterExecutableFunctionNegate(std::vector<std::unique_ptr<Functions::Function>> childFunctions);
}

template <>
inline void
Registrar<std::string, Runtime::Execution::Functions::Function, std::vector<std::unique_ptr<Runtime::Execution::Functions::Function>>>::
    registerAll(Registry<Registrar>& registry)
{
    using namespace NES::Runtime::Execution::Functions;

    /// Registering all arithmetic supported functions
    registry.registerPlugin("Add", RegisterExecutableFunctionAdd);
    registry.registerPlugin("Mul", RegisterExecutableFunctionMul);
    registry.registerPlugin("Sub", RegisterExecutableFunctionSub);
    registry.registerPlugin("Div", RegisterExecutableFunctionDiv);

    /// Registering all logical supported functions
    registry.registerPlugin("Equals", RegisterExecutableFunctionEquals);
    registry.registerPlugin("Negate", RegisterExecutableFunctionNegate);
}

}
