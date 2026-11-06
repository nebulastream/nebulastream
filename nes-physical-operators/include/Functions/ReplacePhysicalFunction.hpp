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

#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

/// Physical implementation of SQL REPLACE(str, search, replacement), see ReplaceLogicalFunction.
class ReplacePhysicalFunction final
{
public:
    ReplacePhysicalFunction(PhysicalFunction stringFunction, PhysicalFunction searchFunction, PhysicalFunction replacementFunction);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

    /// NOLINTNEXTLINE(readability-identifier-naming)
    static PhysicalFunctionRegistryReturnType createREPLACE(PhysicalFunctionRegistryArguments arguments);

private:
    PhysicalFunction stringFunction;
    PhysicalFunction searchFunction;
    PhysicalFunction replacementFunction;
};

static_assert(PhysicalFunctionConcept<ReplacePhysicalFunction>);

}
