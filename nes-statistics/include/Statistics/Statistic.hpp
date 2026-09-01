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
#include <utility>
#include <Identifiers/NESStrongType.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Formatter.hpp>

namespace NES
{

/// Uniquely identifies a statistic. Chosen by the user when submitting a statistic build query.
using StatisticId = NESStrongType<uint64_t, struct StatisticId_, 0, 1>;

/// Defines how the statistic data of a Statistic is to be interpreted
enum class StatisticType : uint8_t
{
    ReservoirSample,
};

/// A statistic represents particular information of a stream over a period of time.
/// As statistics are built and probed by compiled queries, the statistic data is stored as an opaque byte array
/// whose layout is defined by the statistic type.
class Statistic
{
public:
    Statistic(
        const StatisticId statisticId,
        const StatisticType statisticType,
        const Timestamp startTs,
        const Timestamp endTs,
        const uint64_t numberOfSeenTuples,
        std::shared_ptr<std::byte[]> statisticData,
        const uint64_t statisticDataSize)
        : statisticId(statisticId)
        , statisticType(statisticType)
        , startTs(startTs)
        , endTs(endTs)
        , numberOfSeenTuples(numberOfSeenTuples)
        , statisticData(std::move(statisticData))
        , statisticDataSize(statisticDataSize)
    {
    }

    [[nodiscard]] StatisticId getStatisticId() const { return statisticId; }

    [[nodiscard]] StatisticType getStatisticType() const { return statisticType; }

    [[nodiscard]] Timestamp getStartTs() const { return startTs; }

    [[nodiscard]] Timestamp getEndTs() const { return endTs; }

    [[nodiscard]] uint64_t getNumberOfSeenTuples() const { return numberOfSeenTuples; }

    [[nodiscard]] const int8_t* getStatisticData() const { return reinterpret_cast<const int8_t*>(statisticData.get()); }

    [[nodiscard]] uint64_t getStatisticDataSize() const { return statisticDataSize; }

    bool operator==(const Statistic& other) const
    {
        return statisticId == other.statisticId and statisticType == other.statisticType and startTs == other.startTs
            and endTs == other.endTs and numberOfSeenTuples == other.numberOfSeenTuples and statisticDataSize == other.statisticDataSize
            and std::equal(statisticData.get(), statisticData.get() + statisticDataSize, other.statisticData.get());
    }

    friend std::ostream& operator<<(std::ostream& os, const Statistic& statistic)
    {
        return os << "Statistic(id: " << statistic.statisticId << ", type: " << static_cast<uint32_t>(statistic.statisticType)
                  << ", start: " << statistic.startTs << ", end: " << statistic.endTs
                  << ", numberOfSeenTuples: " << statistic.numberOfSeenTuples << ", dataSize: " << statistic.statisticDataSize << ")";
    }

private:
    StatisticId statisticId;
    StatisticType statisticType;
    Timestamp startTs;
    Timestamp endTs;
    uint64_t numberOfSeenTuples;
    std::shared_ptr<std::byte[]> statisticData;
    uint64_t statisticDataSize;
};

}

FMT_OSTREAM(NES::Statistic);
