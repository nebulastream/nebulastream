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
#include <magic_enum/magic_enum.hpp>
#include <vector>
#include <Util/Logger/Formatter.hpp>
#include <fmt/base.h>
#include <fmt/format.h>

namespace NES
{


/// @brief A statistic represents particular information of a component, stream or etc. over a period of time.
/// As we build and probe these statistics via a query compiler, this class solely stores the statistic as a vector of bytes/int8_t
class Statistic
{
public:
    /// The StatisticHash uniquely identifies a statistic anywhere in our system, which means we do not need to send
    /// characteristics and windows to all workers to uniquely identify a statistic.
    using StatisticHash = uint64_t;

    /// Unique identifier across the system so that we can track statistic over the component
    /// We assume that the highest 8-bit are never used
    using StatisticId = uint64_t;

    /// Fixed-point representation of the fractional part of the golden ratio to provide better distribution properties in hash functions
    static constexpr auto goldenRatio = 0x9e3779b97f4a7c15;

    /// Defines what statistic type is for the underlying statistic memory area
    enum class StatisticType : uint8_t
    {
        Histogram,
        Sample,
        Sketch
    };

    Statistic(
        const StatisticId statisticId,
        const StatisticType statisticType,
        const Windowing::TimeMeasure& startTs,
        const Windowing::TimeMeasure& endTs,
        const uint64_t numberOfSeenTuples,
        int8_t* statistic,
        const uint64_t statisticSize)
        : statisticId(statisticId)
        , statisticType(statisticType)
        , startTs(startTs)
        , endTs(endTs)
        , numberOfSeenTuples(numberOfSeenTuples)
        , statistic(statistic, statistic + statisticSize)
    {
    }

    Statistic(const Statistic&) = default;
    Statistic& operator=(const Statistic&) = default;
    Statistic(Statistic&&) = default;
    Statistic& operator=(Statistic&&) = default;

    [[nodiscard]] StatisticType getStatisticType() const { return statisticType; }

    [[nodiscard]] Windowing::TimeMeasure getStartTs() const { return startTs; }

    [[nodiscard]] Windowing::TimeMeasure getEndTs() const { return endTs; }

    [[nodiscard]] const int8_t* getStatisticData() const { return statistic.data(); }

    [[nodiscard]] uint64_t getNumberOfSeenTuples() const { return numberOfSeenTuples; }

    bool operator==(const Statistic& other) const
    {
        return statisticId == other.statisticId and statisticType == other.statisticType and startTs == other.startTs
            and endTs == other.endTs and statistic == other.statistic and numberOfSeenTuples == other.numberOfSeenTuples;
    }

    [[nodiscard]] StatisticHash getHash() const
    {
        /// For now, a statistic hash is simply the statistic type combined with the statistic id.
        /// In the future, we might add additional variable to it, e.g., metrics.
        return (statisticId << 8) | (static_cast<uint8_t>(statisticType));
    }

    friend std::ostream& operator<<(std::ostream& os, const Statistic& statistic)
    {
        return os << fmt::format(
                   "Statistic(Id: {}, StatisticType: {}, startTs: {}, endTs: {}, seenTuples: {}, statistic: {}B)",
                   statistic.statisticId,
                   magic_enum::enum_name(statistic.statisticType),
                   statistic.startTs.toString(),
                   statistic.endTs.toString(),
                   statistic.numberOfSeenTuples,
                   statistic.statistic.size());
    }

private:
    StatisticId statisticId;
    StatisticType statisticType;
    Windowing::TimeMeasure startTs;
    Windowing::TimeMeasure endTs;
    uint64_t numberOfSeenTuples;
    std::vector<int8_t> statistic;
};
}

FMT_OSTREAM(NES::Statistic);