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
#include <vector>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES::Lookback
{
/// Forward-declared on purpose. The generated registry glue includes this header but is not
/// compiled with the ODBC include path, and the context's definition reaches sql.h through the
/// connection config. std::shared_ptr does not need a complete type to destroy.
class LookbackContext;
}

namespace NES
{

/// Executable half of LOOKBACK. The connection, SQL, and row template are resolved once here, at
/// lowering time; per tuple the traced code only stages the parameter bytes, calls out to the
/// database, and copies the rendering into the arena.
class LookbackPhysicalFunction final
{
public:
    LookbackPhysicalFunction(std::shared_ptr<Lookback::LookbackContext> context, std::vector<PhysicalFunction> parameters);

    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

    static PhysicalFunctionRegistryReturnType createLOOKBACK(PhysicalFunctionRegistryArguments arguments);

private:
    /// shared_ptr, not a value member: the type-erasing PhysicalFunction wrapper copies the
    /// concrete function, and the raw pointer handed to nautilus::invoke is baked into the trace
    /// as a constant, so the pointee must have a stable address for the module's lifetime.
    std::shared_ptr<Lookback::LookbackContext> context;
    std::vector<PhysicalFunction> parameters;
};

static_assert(PhysicalFunctionConcept<LookbackPhysicalFunction>);

}
