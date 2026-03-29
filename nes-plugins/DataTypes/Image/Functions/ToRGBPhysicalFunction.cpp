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

#include <ToRGBPhysicalFunction.hpp>

#include <algorithm>
#include <cstdint>
#include <string_view>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/FixedSizedData.hpp>
#include <Nautilus/DataTypes/StructData.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

namespace
{
/// Iron-palette waypoints calibrated to the body-temperature range. Body-temp
/// thermal values cluster in raw ~29500-31500 (a tiny slice of UINT16), so a
/// palette spanning the full 0..65535 leaves them all clumped near pure red.
/// The waypoints below put the entire "useful" thermal range across the full
/// blue→red→orange→yellow→white iron ramp, with the orange-to-yellow knee
/// landing right at the 31100 fever threshold so fever pixels jump to a
/// visibly different colour from healthy-warm pixels.
struct IronWaypoint
{
    uint16_t raw;
    uint8_t r;
    uint8_t g;
    uint8_t b;
};

constexpr IronWaypoint IRON_WAYPOINTS[] = {
    {29500, 0, 0, 80}, // ~22°C: dark blue
    {29900, 0, 0, 200}, // ~26°C: blue
    {30300, 180, 0, 100}, // ~30°C: purple-red
    {30700, 255, 0, 0}, // ~34°C: red
    {31100, 255, 200, 0}, // ~38°C (fever threshold): orange
    {31500, 255, 255, 255}, // ~42°C: white
};
constexpr size_t IRON_WAYPOINT_COUNT = sizeof(IRON_WAYPOINTS) / sizeof(IRON_WAYPOINTS[0]);

constexpr uint8_t lerpByte(const uint8_t a, const uint8_t b, const uint32_t t /*0..255*/)
{
    return static_cast<uint8_t>((static_cast<uint32_t>(a) * (255U - t) + static_cast<uint32_t>(b) * t) / 255U);
}

/// Linear interpolation between iron waypoints. Out-of-range raw values clamp
/// to the nearest endpoint.
void mapIronPalette(const uint16_t raw, uint8_t& r, uint8_t& g, uint8_t& b)
{
    if (raw <= IRON_WAYPOINTS[0].raw)
    {
        r = IRON_WAYPOINTS[0].r;
        g = IRON_WAYPOINTS[0].g;
        b = IRON_WAYPOINTS[0].b;
        return;
    }
    if (raw >= IRON_WAYPOINTS[IRON_WAYPOINT_COUNT - 1].raw)
    {
        r = IRON_WAYPOINTS[IRON_WAYPOINT_COUNT - 1].r;
        g = IRON_WAYPOINTS[IRON_WAYPOINT_COUNT - 1].g;
        b = IRON_WAYPOINTS[IRON_WAYPOINT_COUNT - 1].b;
        return;
    }
    for (size_t k = 0; k + 1 < IRON_WAYPOINT_COUNT; ++k)
    {
        if (raw <= IRON_WAYPOINTS[k + 1].raw)
        {
            const uint32_t span = IRON_WAYPOINTS[k + 1].raw - IRON_WAYPOINTS[k].raw;
            const uint32_t t = static_cast<uint32_t>(raw - IRON_WAYPOINTS[k].raw) * 255U / span;
            r = lerpByte(IRON_WAYPOINTS[k].r, IRON_WAYPOINTS[k + 1].r, t);
            g = lerpByte(IRON_WAYPOINTS[k].g, IRON_WAYPOINTS[k + 1].g, t);
            b = lerpByte(IRON_WAYPOINTS[k].b, IRON_WAYPOINTS[k + 1].b, t);
            return;
        }
    }
}

/// Maps a centi-Kelvin pixel into a planar `[r|g|b]` UINT8 buffer.
///
/// `colormapName` is interpreted by string match; unknown names fall back to
/// `grayscale`. Two PoC palettes:
///
///   - `grayscale`: high byte of UINT16 broadcast across R/G/B.
///   - `iron`:      body-temperature-calibrated piecewise-linear ramp
///                  (see IRON_WAYPOINTS).
void thermalToRGB(
    const int8_t* rawPixels, int8_t* rgbOut, const uint64_t numElements, const int8_t* colormapNamePtr, const uint64_t colormapNameLen)
{
    const auto* const in = reinterpret_cast<const uint16_t*>(rawPixels);
    auto* const r = reinterpret_cast<uint8_t*>(rgbOut);
    auto* const g = r + numElements;
    auto* const b = g + numElements;
    const std::string_view name(reinterpret_cast<const char*>(colormapNamePtr), colormapNameLen);

    if (name == "iron")
    {
        for (uint64_t i = 0; i < numElements; ++i)
        {
            mapIronPalette(in[i], r[i], g[i], b[i]);
        }
        return;
    }

    /// Default / "grayscale": top byte of UINT16 across R=G=B.
    for (uint64_t i = 0; i < numElements; ++i)
    {
        const uint8_t v = static_cast<uint8_t>(in[i] >> 8);
        r[i] = v;
        g[i] = v;
        b[i] = v;
    }
}
}

ToRGBPhysicalFunction::ToRGBPhysicalFunction(PhysicalFunction frameFunction, PhysicalFunction colormapFunction, DataType outputType)
    : frameFunction(std::move(frameFunction)), colormapFunction(std::move(colormapFunction)), outputType(std::move(outputType))
{
}

VarVal ToRGBPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto frameStruct = frameFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto pixelsView = frameStruct.at("pixels").getRawValueAs<FixedSizedData>();
    const auto colormapName = colormapFunction.execute(record, arena).getRawValueAs<VariableSizedData>();

    /// Output pixel count comes from the bound logical output type, which the
    /// logical function derives from the input ThermalFrame layout.
    PRECONDITION(outputType.fields.size() == 3, "RGBFrame layout must have exactly three channels (r, g, b)");
    const auto count = static_cast<size_t>(outputType.fields[0].second.count);
    const auto totalBytes = count * 3;

    const nautilus::val<int8_t*> outputBuffer = arena.allocateMemory(totalBytes);
    nautilus::invoke(
        thermalToRGB,
        pixelsView.getRawPtr(),
        outputBuffer,
        nautilus::val<uint64_t>(count),
        colormapName.getContent(),
        colormapName.getSize());

    return VarVal{StructData{outputBuffer, outputType.fields}, outputType.nullable, nautilus::val<bool>{false}};
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterTO_RGBPhysicalFunction(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 2, "ToRGB expects exactly two child functions");
    return ToRGBPhysicalFunction(arguments.childFunctions[0], arguments.childFunctions[1], arguments.outputType);
}

}
