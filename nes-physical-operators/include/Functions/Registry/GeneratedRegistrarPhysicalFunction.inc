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

namespace Functions
{
/// Defining all arithmetic supported functions
std::unique_ptr<PhysicalFunction> RegisterAddPhysicalFunction(std::vector<std::unique_ptr<PhysicalFunction>> childFunctions);
std::unique_ptr<PhysicalFunction> RegisterMulPhysicalFunction(std::vector<std::unique_ptr<PhysicalFunction>> childFunctions);
std::unique_ptr<PhysicalFunction> RegisterSubPhysicalFunction(std::vector<std::unique_ptr<PhysicalFunction>> childFunctions);
std::unique_ptr<PhysicalFunction> RegisterDivPhysicalFunction(std::vector<std::unique_ptr<PhysicalFunction>> childFunctions);
std::unique_ptr<PhysicalFunction> RegisterModPhysicalFunction(std::vector<std::unique_ptr<Functions::PhysicalFunction>> childFunctions);

/// Defining all logical supported functions
std::unique_ptr<PhysicalFunction> RegisterEqualsPhysicalFunction(std::vector<std::unique_ptr<PhysicalFunction>> childFunctions);
std::unique_ptr<PhysicalFunction> RegisterNegatePhysicalFunction(std::vector<std::unique_ptr<PhysicalFunction>> childFunctions);
std::unique_ptr<PhysicalFunction> RegisterAndPhysicalFunction(std::vector<std::unique_ptr<PhysicalFunction>> childFunctions);
std::unique_ptr<PhysicalFunction> RegisterOrPhysicalFunction(std::vector<std::unique_ptr<PhysicalFunction>> childFunctions);

/// Defining all comparison supported functions
std::unique_ptr<PhysicalFunction> RegisterLessPhysicalFunction(std::vector<std::unique_ptr<PhysicalFunction>> childFunctions);
std::unique_ptr<PhysicalFunction> RegisterLessEqualsPhysicalFunction(std::vector<std::unique_ptr<PhysicalFunction>> childFunctions);
std::unique_ptr<PhysicalFunction> RegisterGreaterPhysicalFunction(std::vector<std::unique_ptr<PhysicalFunction>> childFunctions);
std::unique_ptr<PhysicalFunction> RegisterGreaterEqualsPhysicalFunction(std::vector<std::unique_ptr<PhysicalFunction>> childFunctions);
}

template <>
inline void
Registrar<Functions::RegistryPhysicalFunction, Functions::RegistrySignaturePhysicalFunction>::registerAll(Registry<Registrar>& registry)
{
    using namespace NES::Functions;

    /// Registering all arithmetic supported functions
    registry.registerPlugin("Add", RegisterAddPhysicalFunction);
    registry.registerPlugin("Mul", RegisterMulPhysicalFunction);
    registry.registerPlugin("Sub", RegisterSubPhysicalFunction);
    registry.registerPlugin("Div", RegisterDivPhysicalFunction);
    registry.registerPlugin("Mod", RegisterModPhysicalFunction);

    /// Registering all logical supported functions
    registry.registerPlugin("Equals", RegisterEqualsPhysicalFunction);
    registry.registerPlugin("Negate", RegisterNegatePhysicalFunction);
    registry.registerPlugin("And", RegisterAndPhysicalFunction);
    registry.registerPlugin("Or", RegisterOrPhysicalFunction);

    /// Registering all comparison supported functions
    registry.registerPlugin("Less", RegisterLessPhysicalFunction);
    registry.registerPlugin("LessEquals", RegisterLessEqualsPhysicalFunction);
    registry.registerPlugin("Greater", RegisterGreaterPhysicalFunction);
    registry.registerPlugin("GreaterEquals", RegisterGreaterEqualsPhysicalFunction);
}
}
