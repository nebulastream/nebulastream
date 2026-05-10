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

#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>

namespace NES
{

/// Per-record physical implementation of `to_celsius(ThermalFrame)`.
///
/// Pulls the `pixels` FIXEDSIZED<UINT16,count> view out of the child's
/// StructData, allocates a same-shape FIXEDSIZED<FLOAT32,count> in the arena,
/// and runs the conversion in a single `nautilus::invoke` that loops in plain
/// C++. The output FixedSizedData aliases the arena buffer; downstream sinks
/// see it just like a top-level FIXEDSIZED column.
class ToCelsiusPhysicalFunction final
{
public:
    ToCelsiusPhysicalFunction(PhysicalFunction childFunction, DataType outputType);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

private:
    PhysicalFunction childFunction;
    DataType outputType;
};

static_assert(PhysicalFunctionConcept<ToCelsiusPhysicalFunction>);

}
