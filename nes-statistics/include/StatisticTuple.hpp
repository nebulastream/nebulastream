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

#include <Identifiers/StatisticIdentifiers.hpp>
#include <Statistic/StatisticTypes.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Formatter.hpp>
#include <fmt/base.h>
#include <fmt/format.h>

namespace NES
{

/// A statistic captures some property of a stream (or another component) over a period of time.
/// Since statistics are built and probed by compiled queries, the synopsis itself is kept as an opaque, shared byte array;
/// its layout is only understood by the physical function identified by `typeName`.
class StatisticTuple
{
public:
    /// Aliases back to the nes-common definitions so that 'StatisticTuple::StatisticId' and
    /// 'StatisticTuple::StatisticType' keep working.
    using StatisticId = NES::StatisticId;
    using StatisticType = NES::StatisticType;

    StatisticTuple(
        const StatisticId statisticId,
        std::string typeName,
        const Timestamp& startTs,
        const Timestamp& endTs,
        const uint64_t numberOfSeenMeasurements,
        std::shared_ptr<std::byte[]> statisticData,
        const uint64_t statisticDataSize)
        : statisticId(statisticId)
        , typeName(std::move(typeName))
        , startTs(startTs)
        , endTs(endTs)
        , numberOfSeenMeasurements(numberOfSeenMeasurements)
        , statisticData(std::move(statisticData))
        , statisticDataSize(statisticDataSize)
    {
    }

    StatisticTuple(const StatisticTuple&) = default;
    StatisticTuple& operator=(const StatisticTuple&) = delete;
    StatisticTuple(StatisticTuple&&) = default;
    StatisticTuple& operator=(StatisticTuple&&) = delete;

    [[nodiscard]] const std::string& getTypeName() const { return typeName; }

    [[nodiscard]] Timestamp getStartTs() const { return startTs; }

    [[nodiscard]] Timestamp getEndTs() const { return endTs; }

    [[nodiscard]] const int8_t* getStatisticData() const { return reinterpret_cast<const int8_t*>(statisticData.get()); }

    [[nodiscard]] uint64_t getStatisticDataSize() const { return statisticDataSize; }

    [[nodiscard]] uint64_t getNumberOfSeenMeasurements() const { return numberOfSeenMeasurements; }

    [[nodiscard]] StatisticId getStatisticId() const { return statisticId; }

    bool operator==(const StatisticTuple& other) const
    {
        return statisticId == other.statisticId and typeName == other.typeName and startTs == other.startTs and endTs == other.endTs
            and numberOfSeenMeasurements == other.numberOfSeenMeasurements and statisticDataSize == other.statisticDataSize
            and std::equal(statisticData.get(), statisticData.get() + statisticDataSize, other.statisticData.get());
    }

    friend std::ostream& operator<<(std::ostream& os, const StatisticTuple& statistic);

private:
    StatisticId statisticId;
    std::string typeName;
    Timestamp startTs;
    Timestamp endTs;
    uint64_t numberOfSeenMeasurements;
    std::shared_ptr<std::byte[]> statisticData;
    uint64_t statisticDataSize;
};

}

FMT_OSTREAM(NES::StatisticTuple);
