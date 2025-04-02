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

#include <memory>
#include <optional>
#include <vector>
#include <Measures/TimeMeasure.hpp>

namespace NES::Runtime
{

enum class StatisticType : uint8_t
{
    Histogram,
    Sample,
    Sketch
};

// TODO(nikla44): create statistic classes and data interpreter
struct Statistic
{
    Statistic(
        const StatisticType type, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs, int8_t* statisticPtr)
        : type(type), startTs(startTs), endTs(endTs), statistic(statisticPtr)
    {
    }

    [[nodiscard]] StatisticType getType() const { return type; }
    [[nodiscard]] Windowing::TimeMeasure getStartTs() const { return startTs; }
    [[nodiscard]] Windowing::TimeMeasure getEndTs() const { return endTs; }
    [[nodiscard]] int8_t* getStatisticData() const { return statistic; }

private:
    StatisticType type;
    Windowing::TimeMeasure startTs;
    Windowing::TimeMeasure endTs;
    int8_t* statistic;
};

using StatisticHash = uint64_t;
using StatisticPtr = std::shared_ptr<Statistic>;
using HashStatisticPair = std::pair<StatisticHash, StatisticPtr>;

class AbstractStatisticStore
{
public:
    AbstractStatisticStore() = default;
    virtual ~AbstractStatisticStore() = default;

    /// Inserts a statistic with the statisticHash into a StatisticStore. Returns false, if statistic already exists
    virtual bool insertStatistic(const StatisticHash& statisticHash, StatisticPtr statistic) = 0;

    /// Deletes all statistics belonging to the statisticHash in the period of [startTs, endTs]. Returns true, if any statistic was deleted
    virtual bool
    deleteStatistics(const StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
        = 0;

    /// Gets all statistics belonging to the statisticHash in the period of [startTs, endTs]
    virtual std::vector<StatisticPtr>
    getStatistics(const StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs) = 0;

    /// Gets a single statistic belonging to the statisticHash that has exactly the startTs and endTs
    virtual std::optional<StatisticPtr>
    getSingleStatistic(const StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs) = 0;

    /// Returns all statistics which are currently saved in this store
    virtual std::vector<HashStatisticPair> getAllStatistics() = 0;
};

}
