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

#include <cstddef>
#include <cstdint>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/FixedSizedData.hpp>
#include <Nautilus/DataTypes/StructData.hpp>
#include <nautilus/val.hpp>

namespace NES
{

/// Domain-specific concretization of `StructData` for ThermalFrame layouts.
///
/// ThermalFrame's logical type is `STRUCT { pixels: FIXEDSIZED<UINT16, N> }`.
/// This wrapper takes a pointer to the inline pixel bytes plus the pixel count
/// and gives callers typed access — `getPixels()` returns a `FixedSizedData`
/// view of the same memory.
///
/// Plugins are expected to add one of these per nominal struct type so
/// physical/runtime code can work with strongly-typed values rather than
/// reaching into a generic `StructData::at("pixels")` and casting at every
/// call site.
class ThermalFrameData
{
public:
    /// Builds the ThermalFrame layout for a given pixel count and constructs
    /// the wrapping StructData over `pixelBuffer`.
    ThermalFrameData(const nautilus::val<int8_t*>& pixelBuffer, uint32_t pixelCount);

    /// View of the same memory typed as `FIXEDSIZED<UINT16, pixelCount>`.
    [[nodiscard]] FixedSizedData getPixels() const;

    [[nodiscard]] uint32_t getPixelCount() const;

    /// Lower-level access for callers that want to plug into the generic
    /// StructData machinery (e.g. equality, ostream, generic iteration).
    [[nodiscard]] const StructData& asStructData() const;

private:
    StructData inner;
    uint32_t pixelCount;
};

}
