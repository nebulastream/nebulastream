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
#include <Identifiers/NESStrongType.hpp>
#include <Util/Logger/Formatter.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <fmt/base.h>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>

namespace NES
{


/// @brief A statistic represents particular information of a component, stream or etc. over a period of time.
/// As we build and probe these statistics via a query compiler, this class stores the statistic data as a shared byte array.
class Statistic
{
public:
    /// Uniquely identifies a statistic. Assigned by the StatisticCoordinator via an atomic counter.
    using StatisticId = NESStrongType<uint64_t, struct StatisticId_, 0, 1>;

    /// Defines what statistic type is for the underlying statistic memory area
    enum class StatisticType : uint8_t
    {
        Equi_Width_Histogram,
        Reservoir_Sample,
        Count_Min_Sketch
    };

    Statistic(
        const StatisticId statisticId,
        const StatisticType statisticType,
        const Windowing::TimeMeasure& startTs,
        const Windowing::TimeMeasure& endTs,
        const uint64_t numberOfSeenTuples,
        std::shared_ptr<std::byte[]> statisticData,
        const uint64_t statisticDataSize)
        : statisticId(statisticId)
        , statisticType(statisticType)
        , startTsMillis(startTs.getTime())
        , endTsMillis(endTs.getTime())
        , numberOfSeenTuples(numberOfSeenTuples)
        , statisticData(std::move(statisticData))
        , statisticDataSize(statisticDataSize)
    {
    }

    Statistic(const Statistic&) = default;
    Statistic& operator=(const Statistic&) = default;
    Statistic(Statistic&&) = default;
    Statistic& operator=(Statistic&&) = default;

    [[nodiscard]] StatisticType getStatisticType() const { return statisticType; }

    [[nodiscard]] Windowing::TimeMeasure getStartTs() const { return Windowing::TimeMeasure{startTsMillis}; }

    [[nodiscard]] Windowing::TimeMeasure getEndTs() const { return Windowing::TimeMeasure{endTsMillis}; }

    [[nodiscard]] const int8_t* getStatisticData() const { return reinterpret_cast<const int8_t*>(statisticData.get()); }

    [[nodiscard]] uint64_t getNumberOfSeenTuples() const { return numberOfSeenTuples; }

    [[nodiscard]] StatisticId getStatisticId() const { return statisticId; }

    bool operator==(const Statistic& other) const
    {
        return statisticId == other.statisticId and statisticType == other.statisticType and startTsMillis == other.startTsMillis
            and endTsMillis == other.endTsMillis and numberOfSeenTuples == other.numberOfSeenTuples
            and statisticDataSize == other.statisticDataSize
            and std::equal(statisticData.get(), statisticData.get() + statisticDataSize, other.statisticData.get());
    }

    friend std::ostream& operator<<(std::ostream& os, const Statistic& statistic);

private:
    StatisticId statisticId;
    StatisticType statisticType;
    uint64_t startTsMillis;
    uint64_t endTsMillis;
    uint64_t numberOfSeenTuples;
    std::shared_ptr<std::byte[]> statisticData;
    uint64_t statisticDataSize;
};

/// Overload modulo operator for StatisticId as it is commonly used to index into hash maps.
inline size_t operator%(const Statistic::StatisticId id, const size_t containerSize)
{
    return id.getRawValue() % containerSize;
}

}

FMT_OSTREAM(NES::Statistic);
