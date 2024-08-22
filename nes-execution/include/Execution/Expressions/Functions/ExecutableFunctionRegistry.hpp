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

#include <Execution/Expressions/Expression.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PluginRegistry.hpp>

namespace NES::Runtime::Execution::Expressions
{

class ExecutableFunctionRegistry
    : public BaseRegistry<ExecutableFunctionRegistry, std::string, Expression, const std::vector<ExpressionPtr>&>
{
};

}

#define INCLUDED_FROM_EXECUTABLE_FUNCTION_REGISTRY
#include <Execution/Expressions/Functions/GeneratedExecutableFunctionRegistrar.hpp>
#undef INCLUDED_FROM_EXECUTABLE_FUNCTION_REGISTRY
