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

#include <cstdint>
#include <span>
#include <Operators/Statistic/EquiWidthHistogramBlobLayout.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// Throws CannotProbeStatistic unless payload is a blob EquiWidthHistogramBlob's layout can decode. A histogram
/// blob's size depends on its own header, so the probe's fixed-size check cannot cover it and this is the only thing
/// standing between a foreign or truncated payload and an out-of-bounds read in traced code. Plain C++ on purpose:
/// it runs once per loaded statistic, outside any trace.
void validateEquiWidthHistogramBlob(std::span<const int8_t> payload);

/// Nautilus accessors for the blob header.
void writeEquiWidthHistogramBlobHeader(
    const nautilus::val<int8_t*>& blobMemArea,
    const nautilus::val<uint64_t>& numberOfBins,
    const nautilus::val<uint64_t>& minValue,
    const nautilus::val<uint64_t>& maxValue);
[[nodiscard]] nautilus::val<uint64_t> readEquiWidthHistogramBlobNumberOfBins(const nautilus::val<int8_t*>& blobMemArea);
[[nodiscard]] nautilus::val<uint64_t> readEquiWidthHistogramBlobMinValue(const nautilus::val<int8_t*>& blobMemArea);
[[nodiscard]] nautilus::val<uint64_t> readEquiWidthHistogramBlobMaxValue(const nautilus::val<int8_t*>& blobMemArea);
[[nodiscard]] nautilus::val<int8_t*> getEquiWidthHistogramBlobCounters(const nautilus::val<int8_t*>& blobMemArea);

/// Reads the bins of an equi-width histogram blob. Not a full C++ iterator: the caller drives the loop with the bin
/// count from the header.
///
/// Bin i covers [binStart(i), binEnd(i)), except for the last bin, whose binEnd is maxValue and is included. Because
/// the width is an integer division, the last bin also absorbs the remainder (maxValue - minValue) % numberOfBins,
/// so it spans up to numberOfBins more values than a regular bin. A caller that wants the most even bins picks
/// maxValue - minValue as a multiple of the bin count; the last bin is then wider by exactly the one value maxValue.
class EquiWidthHistogramBlobBinReader
{
public:
    explicit EquiWidthHistogramBlobBinReader(const nautilus::val<int8_t*>& blobMemArea);

    [[nodiscard]] nautilus::val<uint64_t> getNumberOfBins() const;
    [[nodiscard]] nautilus::val<uint64_t> getBinStart(const nautilus::val<uint64_t>& binIndex) const;
    [[nodiscard]] nautilus::val<uint64_t> getBinEnd(const nautilus::val<uint64_t>& binIndex) const;
    [[nodiscard]] nautilus::val<uint64_t> getBinCounter(const nautilus::val<uint64_t>& binIndex) const;

private:
    nautilus::val<int8_t*> counters;
    nautilus::val<uint64_t> numberOfBins;
    nautilus::val<uint64_t> minValue;
    nautilus::val<uint64_t> maxValue;
    nautilus::val<uint64_t> binWidth;
};

}
