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
#include <memory>
#include <string>
#ifndef INCLUDED_FROM_LOGICAL_FUNCTION_REGISTRY
#    error "This file should not be included directly! " \
"Include instead include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>"
#endif
namespace NES
{

std::unique_ptr<LogicalFunction> RegisterSinFunction();
std::unique_ptr<LogicalFunction> RegisterRadiansFunction();
std::unique_ptr<LogicalFunction> RegisterLog2Function();
std::unique_ptr<LogicalFunction> RegisterLog10Function();
std::unique_ptr<LogicalFunction> RegisterLnFunction();
std::unique_ptr<LogicalFunction> RegisterAbsFunction();
std::unique_ptr<LogicalFunction> RegisterPowerFunction();
std::unique_ptr<LogicalFunction> RegisterCosFunction();

template <>
inline void Registrar<std::string, LogicalFunction>::registerAll(Registry<Registrar>& registry)
{
    registry.registerPlugin("sin", RegisterSinFunction);
    registry.registerPlugin("radians", RegisterRadiansFunction);
    registry.registerPlugin("log2", RegisterLog2Function);
    registry.registerPlugin("log10", RegisterLog10Function);
    registry.registerPlugin("ln", RegisterLnFunction);
    registry.registerPlugin("abs", RegisterAbsFunction);
    registry.registerPlugin("power", RegisterPowerFunction);
    registry.registerPlugin("cos", RegisterCosFunction);
}
}