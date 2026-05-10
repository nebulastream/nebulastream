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

/// Per-record physical implementation of `to_rgb(ThermalFrame, name) -> RGBFrame`.
///
/// Pulls the pixels FixedSizedData out of the ThermalFrame StructData and the
/// colormap name out of the VariableSizedData second child, allocates a
/// planar `[r|g|b]` UINT8 buffer in the arena (each channel `count` bytes),
/// and runs the colormap selection inside one nautilus::invoke. The output
/// VarVal is a StructData with the RGBFrame layout aliasing the arena buffer.
class ToRGBPhysicalFunction final
{
public:
    ToRGBPhysicalFunction(PhysicalFunction frameFunction, PhysicalFunction colormapFunction, DataType outputType);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

private:
    PhysicalFunction frameFunction;
    PhysicalFunction colormapFunction;
    DataType outputType;
};

static_assert(PhysicalFunctionConcept<ToRGBPhysicalFunction>);

}
