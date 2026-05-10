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
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// Per-record materialisation of a STRUCT value: allocates a buffer sized to the
/// struct layout, evaluates each child function, and writes each result at the
/// matching field index. Output type carries the full STRUCT layout (including
/// field offsets) so `StructData::writeAt(i, ...)` lands at the right offset.
class ConstructStructPhysicalFunction
{
public:
    ConstructStructPhysicalFunction(std::vector<PhysicalFunction> childFunctions, DataType outputType);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

private:
    std::vector<PhysicalFunction> childFunctions;
    DataType outputType;
};

static_assert(PhysicalFunctionConcept<ConstructStructPhysicalFunction>);

}
