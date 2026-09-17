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

#include <Statistics/EquiWidthHistogramBlob.hpp>

#include <cstdint>
#include <cstring>
#include <span>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <Operators/Statistic/EquiWidthHistogramBlobLayout.hpp>
#include <ErrorHandling.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{

uint64_t readHeaderField(const std::span<const int8_t> payload, const uint64_t offset)
{
    uint64_t value = 0;
    std::memcpy(&value, payload.data() + offset, sizeof(value));
    return value;
}

}

void validateEquiWidthHistogramBlob(const std::span<const int8_t> payload)
{
    if (payload.size() < EquiWidthHistogramBlob::HEADER_SIZE)
    {
        throw CannotProbeStatistic(
            "An equi-width histogram blob is at least its {}-byte header, but the stored payload is {} bytes",
            EquiWidthHistogramBlob::HEADER_SIZE,
            payload.size());
    }

    const auto numberOfBins = readHeaderField(payload, EquiWidthHistogramBlob::NUMBER_OF_BINS_OFFSET);
    const auto minValue = readHeaderField(payload, EquiWidthHistogramBlob::MIN_VALUE_OFFSET);
    const auto maxValue = readHeaderField(payload, EquiWidthHistogramBlob::MAX_VALUE_OFFSET);

    if (numberOfBins == 0)
    {
        throw CannotProbeStatistic("An equi-width histogram blob has at least one bin, but the stored header says none");
    }
    if (minValue >= maxValue)
    {
        throw CannotProbeStatistic("An equi-width histogram covers the values {}..{}, which is not a range", minValue, maxValue);
    }
    if (maxValue - minValue < numberOfBins)
    {
        throw CannotProbeStatistic(
            "An equi-width histogram of {} bins needs a range of at least that many values, but {}..{} holds {}",
            numberOfBins,
            minValue,
            maxValue,
            maxValue - minValue);
    }

    /// Compared by division rather than against numberOfBins * COUNTER_SIZE, which a hostile header could overflow.
    const auto counterBytes = payload.size() - EquiWidthHistogramBlob::HEADER_SIZE;
    if (counterBytes % EquiWidthHistogramBlob::COUNTER_SIZE != 0 or counterBytes / EquiWidthHistogramBlob::COUNTER_SIZE != numberOfBins)
    {
        throw CannotProbeStatistic(
            "An equi-width histogram is a {}-byte header followed by one {}-byte counter per bin, but a stored payload of "
            "{} bytes carries {} whole counters for the {} bins its header claims",
            EquiWidthHistogramBlob::HEADER_SIZE,
            EquiWidthHistogramBlob::COUNTER_SIZE,
            payload.size(),
            counterBytes / EquiWidthHistogramBlob::COUNTER_SIZE,
            numberOfBins);
    }
}

void writeEquiWidthHistogramBlobHeader(
    const nautilus::val<int8_t*>& blobMemArea,
    const nautilus::val<uint64_t>& numberOfBins,
    const nautilus::val<uint64_t>& minValue,
    const nautilus::val<uint64_t>& maxValue)
{
    VarVal{numberOfBins}.writeToMemory(blobMemArea + nautilus::val<uint64_t>{EquiWidthHistogramBlob::NUMBER_OF_BINS_OFFSET});
    VarVal{minValue}.writeToMemory(blobMemArea + nautilus::val<uint64_t>{EquiWidthHistogramBlob::MIN_VALUE_OFFSET});
    VarVal{maxValue}.writeToMemory(blobMemArea + nautilus::val<uint64_t>{EquiWidthHistogramBlob::MAX_VALUE_OFFSET});
}

nautilus::val<uint64_t> readEquiWidthHistogramBlobNumberOfBins(const nautilus::val<int8_t*>& blobMemArea)
{
    return readValueFromMemRef<uint64_t>(blobMemArea + nautilus::val<uint64_t>{EquiWidthHistogramBlob::NUMBER_OF_BINS_OFFSET});
}

nautilus::val<uint64_t> readEquiWidthHistogramBlobMinValue(const nautilus::val<int8_t*>& blobMemArea)
{
    return readValueFromMemRef<uint64_t>(blobMemArea + nautilus::val<uint64_t>{EquiWidthHistogramBlob::MIN_VALUE_OFFSET});
}

nautilus::val<uint64_t> readEquiWidthHistogramBlobMaxValue(const nautilus::val<int8_t*>& blobMemArea)
{
    return readValueFromMemRef<uint64_t>(blobMemArea + nautilus::val<uint64_t>{EquiWidthHistogramBlob::MAX_VALUE_OFFSET});
}

nautilus::val<int8_t*> getEquiWidthHistogramBlobCounters(const nautilus::val<int8_t*>& blobMemArea)
{
    return blobMemArea + nautilus::val<uint64_t>{EquiWidthHistogramBlob::HEADER_SIZE};
}

EquiWidthHistogramBlobBinReader::EquiWidthHistogramBlobBinReader(const nautilus::val<int8_t*>& blobMemArea)
    : counters(getEquiWidthHistogramBlobCounters(blobMemArea))
    , numberOfBins(readEquiWidthHistogramBlobNumberOfBins(blobMemArea))
    , minValue(readEquiWidthHistogramBlobMinValue(blobMemArea))
    , maxValue(readEquiWidthHistogramBlobMaxValue(blobMemArea))
    , binWidth((maxValue - minValue) / numberOfBins) /// equiWidthHistogramBinWidth, in Nautilus values
{
}

nautilus::val<uint64_t> EquiWidthHistogramBlobBinReader::getNumberOfBins() const
{
    return numberOfBins;
}

nautilus::val<uint64_t> EquiWidthHistogramBlobBinReader::getBinStart(const nautilus::val<uint64_t>& binIndex) const
{
    return minValue + (binIndex * binWidth);
}

nautilus::val<uint64_t> EquiWidthHistogramBlobBinReader::getBinEnd(const nautilus::val<uint64_t>& binIndex) const
{
    /// The last bin ends at maxValue, inclusive: it holds both the remainder of the integer division and maxValue itself.
    /// A select rather than a ternary, which would need the traced condition as a plain bool and bake one branch in.
    const auto nextBin = binIndex + nautilus::val<uint64_t>{1};
    const auto regularEnd = minValue + (nextBin * binWidth);
    return VarVal::select(nextBin < numberOfBins, VarVal{regularEnd}, VarVal{maxValue}).getRawValueAs<nautilus::val<uint64_t>>();
}

nautilus::val<uint64_t> EquiWidthHistogramBlobBinReader::getBinCounter(const nautilus::val<uint64_t>& binIndex) const
{
    return readValueFromMemRef<uint64_t>(counters + (binIndex * nautilus::val<uint64_t>{EquiWidthHistogramBlob::COUNTER_SIZE}));
}

}
