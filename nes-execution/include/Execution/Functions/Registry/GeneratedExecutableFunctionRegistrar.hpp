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
std::unique_ptr<Function> RegisterAddFunction(std::vector<FunctionPtr> subFunctions);
std::unique_ptr<Function> RegisterMulFunction(std::vector<FunctionPtr> subFunctions);
std::unique_ptr<Function> RegisterSubFunction(std::vector<FunctionPtr> subFunctions);
std::unique_ptr<Function> RegisterDivFunction(std::vector<FunctionPtr> subFunctions);

/// Defining all logical supported functions
std::unique_ptr<Function> RegisterEqualsFunction(std::vector<FunctionPtr> subFunctions);
std::unique_ptr<Function> RegisterNegateFunction(std::vector<FunctionPtr> subFunctions);
}

template <>
inline void
Registrar<std::string, Runtime::Execution::Functions::Function, std::vector<Runtime::Execution::Functions::FunctionPtr>>::
    registerAll(Registry<Registrar>& registry)
{
    using namespace NES::Runtime::Execution::Functions;

    /// Registering all arithmetic supported functions
    registry.registerPlugin("Add", RegisterAddFunction);
    registry.registerPlugin("Mul", RegisterMulFunction);
    registry.registerPlugin("Sub", RegisterSubFunction);
    registry.registerPlugin("Div", RegisterDivFunction);

    /// Registering all logical supported functions
    registry.registerPlugin("Equals", RegisterEqualsFunction);
    registry.registerPlugin("Negate", RegisterNegateFunction);
}

}
