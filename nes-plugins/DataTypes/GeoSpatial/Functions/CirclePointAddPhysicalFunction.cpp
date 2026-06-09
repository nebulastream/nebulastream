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

#include <CirclePointAddPhysicalFunction.hpp>

#include <cstdint>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/StructData.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/val.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

CirclePointAddPhysicalFunction::CirclePointAddPhysicalFunction(
    PhysicalFunction leftFunction, PhysicalFunction rightFunction, DataType outputType)
    : leftFunction(std::move(leftFunction)), rightFunction(std::move(rightFunction)), outputType(std::move(outputType))
{
}

VarVal CirclePointAddPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto left = leftFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto right = rightFunction.execute(record, arena).getRawValueAs<StructData>();

    const auto center = left.at("center").getRawValueAs<StructData>();
    const auto centerX = center.at("x").getRawValueAs<nautilus::val<double>>();
    const auto centerY = center.at("y").getRawValueAs<nautilus::val<double>>();

    const auto pointX = right.at("x").getRawValueAs<nautilus::val<double>>();
    const auto pointY = right.at("y").getRawValueAs<nautilus::val<double>>();

    /// Allocate the output struct from the bound `CirclePoint` layout — host-side
    /// constant under nautilus tracing, so the bump is folded.
    const auto pointBytes = outputType.fields[0].second.getSizeInBytesWithoutNull();
    const nautilus::val<int8_t*> pointBuffer = arena.allocateMemory(pointBytes);
    const StructData point{pointBuffer, outputType.fields[0].second.fields};
    point.writeAt(static_cast<size_t>(0), VarVal{centerX + pointX});
    point.writeAt(static_cast<size_t>(1), VarVal{centerY + pointY});

    const auto totalBytes = outputType.getSizeInBytesWithoutNull();
    const nautilus::val<int8_t*> outputBuffer = arena.allocateMemory(totalBytes);
    const StructData out{outputBuffer, outputType.fields};
    out.writeAt(static_cast<size_t>(0), VarVal{point});
    out.writeAt(static_cast<size_t>(1), VarVal{left.at("radius")});

    return VarVal{out, outputType.nullable, nautilus::val<bool>{false}};
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterAdd_Circle_PointPhysicalFunction(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 2, "Add_CirclePoint_CirclePoint expects exactly two child functions");
    return CirclePointAddPhysicalFunction(arguments.childFunctions[0], arguments.childFunctions[1], arguments.outputType);
}

}