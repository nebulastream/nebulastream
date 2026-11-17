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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>

#include <Operators/Statistic/StatisticBlobType.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Formatter.hpp>
#include <fmt/base.h>
#include <fmt/format.h>

namespace NES
{

/// A statistic captures some property of a stream (or another component) over a period of time.
/// It does not carry its StatisticId: the store keys statistics by id, so every insert and lookup already names it.
/// Since statistics are built and probed by compiled queries, the synopsis itself is kept as an opaque byte array;
/// its layout is only understood by the physical function identified by `typeName`.
/// The array is shared and immutable, so copying a tuple is cheap and a copy handed out by the store keeps the payload
/// alive even after the store deletes its own copy.
class StatisticTuple
{
public:
    StatisticTuple(
        StatisticBlobType typeName,
        const Timestamp& startTs,
        const Timestamp& endTs,
        const uint64_t numberOfSeenMeasurements,
        /// NOLINTNEXTLINE(modernize-avoid-c-arrays) dynamic byte buffer requires array form
        std::shared_ptr<const std::byte[]> statisticData,
        const uint64_t statisticDataSize)
        : typeName(std::move(typeName))
        , startTs(startTs)
        , endTs(endTs)
        , numberOfSeenMeasurements(numberOfSeenMeasurements)
        , statisticData(std::move(statisticData))
        , statisticDataSize(statisticDataSize)
    {
    }

    [[nodiscard]] const StatisticBlobType& getTypeName() const { return typeName; }

    [[nodiscard]] Timestamp getStartTs() const { return startTs; }

    [[nodiscard]] Timestamp getEndTs() const { return endTs; }

    [[nodiscard]] const int8_t* getStatisticData() const { return reinterpret_cast<const int8_t*>(statisticData.get()); }

    [[nodiscard]] uint64_t getStatisticDataSize() const { return statisticDataSize; }

    [[nodiscard]] uint64_t getNumberOfSeenMeasurements() const { return numberOfSeenMeasurements; }

    bool operator==(const StatisticTuple& other) const
    {
        return typeName == other.typeName and startTs == other.startTs and endTs == other.endTs
            and numberOfSeenMeasurements == other.numberOfSeenMeasurements and statisticDataSize == other.statisticDataSize
            and std::equal(statisticData.get(), statisticData.get() + statisticDataSize, other.statisticData.get());
    }

    friend std::ostream& operator<<(std::ostream& os, const StatisticTuple& statistic);

private:
    StatisticBlobType typeName;
    Timestamp startTs;
    Timestamp endTs;
    uint64_t numberOfSeenMeasurements;
    /// NOLINTNEXTLINE(modernize-avoid-c-arrays) dynamic byte buffer requires array form
    std::shared_ptr<const std::byte[]> statisticData;
    uint64_t statisticDataSize;
};

}

FMT_OSTREAM(NES::StatisticTuple);
