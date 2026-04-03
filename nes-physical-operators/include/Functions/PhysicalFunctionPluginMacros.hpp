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

/// @file PhysicalFunctionPluginMacros.hpp
/// @brief Single macro to define a complete physical function plugin in one .cpp file.
///
/// ## Usage
///
///     #include <Functions/PhysicalFunctionPluginMacros.hpp>
///
///     namespace NES {
///     NES_PHYSICAL_FUNCTION(2, Uuid)
///     {
///         /// `childFunctions` is a std::vector<PhysicalFunction> with the child functions.
///         /// `record` and `arena` are available.
///         auto high = childFunctions[0].execute(record, arena);
///         auto low  = childFunctions[1].execute(record, arena);
///         /// ... compute result ...
///         return VarVal{result, nullable, false};
///     }
///     }
///
/// Works for any arity. The macro must be invoked inside `namespace NES { }`.

#include <utility>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

/// NOLINTBEGIN(cppcoreguidelines-macro-usage)

/// Declares a physical function class with vector-based child storage.
/// Works for any arity. The plugin author provides the execute() body.
#define NES_PHYSICAL_FUNCTION(arity, name) \
    class name##PhysicalFunction final \
    { \
    public: \
        explicit name##PhysicalFunction(std::vector<PhysicalFunction> childFunctions); \
        [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const; \
\
    private: \
        std::vector<PhysicalFunction> childFunctions; \
    }; \
    static_assert(PhysicalFunctionConcept<name##PhysicalFunction>); \
    name##PhysicalFunction::name##PhysicalFunction(std::vector<PhysicalFunction> childFunctions) \
        : childFunctions(std::move(childFunctions)) \
    { \
    } \
    PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::Register##name##PhysicalFunction( \
        PhysicalFunctionRegistryArguments args) \
    { \
        PRECONDITION(args.childFunctions.size() == (arity), #name "PhysicalFunction must have exactly " #arity " child function(s)"); \
        return name##PhysicalFunction(std::move(args.childFunctions)); \
    } \
    VarVal name##PhysicalFunction::execute([[maybe_unused]] const Record& record, [[maybe_unused]] ArenaRef& arena) const

/// NOLINTEND(cppcoreguidelines-macro-usage)
