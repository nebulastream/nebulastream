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

#include <ReadingAddPhysicalFunction.hpp>

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

ReadingAddPhysicalFunction::ReadingAddPhysicalFunction(PhysicalFunction leftFunction, PhysicalFunction rightFunction, DataType outputType)
    : leftFunction(std::move(leftFunction)), rightFunction(std::move(rightFunction)), outputType(std::move(outputType))
{
}

VarVal ReadingAddPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto left = leftFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto right = rightFunction.execute(record, arena).getRawValueAs<StructData>();

    const auto leftTemp = left.at("temperature").getRawValueAs<nautilus::val<double>>();
    const auto rightTemp = right.at("temperature").getRawValueAs<nautilus::val<double>>();
    const auto leftHum = left.at("humidity").getRawValueAs<nautilus::val<double>>();
    const auto rightHum = right.at("humidity").getRawValueAs<nautilus::val<double>>();

    /// Allocate the output struct from the bound `Reading` layout — host-side
    /// constant under nautilus tracing, so the bump is folded.
    const auto totalBytes = outputType.getSizeInBytesWithoutNull();
    const nautilus::val<int8_t*> outputBuffer = arena.allocateMemory(totalBytes);
    const StructData out{outputBuffer, outputType.fields};
    out.writeAt(static_cast<size_t>(0), VarVal{leftTemp + rightTemp});
    out.writeAt(static_cast<size_t>(1), VarVal{leftHum + rightHum});

    return VarVal{out, outputType.nullable, nautilus::val<bool>{false}};
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterAdd_Reading_ReadingPhysicalFunction(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 2, "Add_Reading_Reading expects exactly two child functions");
    return ReadingAddPhysicalFunction(arguments.childFunctions[0], arguments.childFunctions[1], arguments.outputType);
}

}
