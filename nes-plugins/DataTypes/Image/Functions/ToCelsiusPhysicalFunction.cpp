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

#include <ToCelsiusPhysicalFunction.hpp>

#include <cstdint>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/FixedSizedData.hpp>
#include <Nautilus/DataTypes/StructData.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

ToCelsiusPhysicalFunction::ToCelsiusPhysicalFunction(PhysicalFunction childFunction, DataType outputType)
    : childFunction(std::move(childFunction)), outputType(std::move(outputType))
{
}

VarVal ToCelsiusPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    /// Logical type is `to_celsius(ThermalFrame) -> FIXEDSIZED<FLOAT32, count>`,
    /// so we know the child's value is the ThermalFrame StructData and the
    /// output shape comes from the bound logical output type.
    const auto frameStruct = childFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto pixelsView = frameStruct.at("pixels").getRawValueAs<FixedSizedData>();

    const auto count = static_cast<size_t>(outputType.count);
    const auto outputBytes = count * sizeof(float);
    const nautilus::val<int8_t*> outputBuffer = arena.allocateMemory(outputBytes);
    auto outFixedSized = FixedSizedData{outputBuffer, count, DataType::Type::FLOAT32};

    /// Compute in double, cast once at the store. Single-precision arithmetic diverges
    /// between Nautilus interpreter (sequential rounding) and compiler (FMA contraction):
    /// e.g. raw=30000 yielded 26.850006 vs 26.849998. Doing the math at double and
    /// rounding once on store removes the mode-dependent divergence.
    const nautilus::val<double> scale = 0.01;
    const nautilus::val<double> kelvinToCelsiusOffset = 273.15;

    for (nautilus::val<size_t> i = 0; i < pixelsView.getNumElements(); ++i)
    {
        const auto rawAsDouble = static_cast<nautilus::val<double>>(pixelsView.at(i).getRawValueAs<nautilus::val<float>>());
        const nautilus::val<double> celsius = (rawAsDouble * scale) - kelvinToCelsiusOffset;
        outFixedSized.writeAt(i, VarVal{static_cast<nautilus::val<float>>(celsius)});
    }
    return VarVal{outFixedSized, outputType.nullable, nautilus::val<bool>{false}};
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterTO_CELSIUSPhysicalFunction(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 1, "ToCelsius expects exactly one child function");
    return ToCelsiusPhysicalFunction(arguments.childFunctions[0], arguments.outputType);
}

}
