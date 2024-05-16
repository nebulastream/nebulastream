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

#ifndef NES_NES_RUNTIME_INCLUDE_STATISTICCOLLECTION_STATISTICSTORAGE_BUILDSTATISTICSTORE_HPP_
#define NES_NES_RUNTIME_INCLUDE_STATISTICCOLLECTION_STATISTICSTORAGE_BUILDSTATISTICSTORE_HPP_
#include <StatisticCollection/StatisticStorage/AbstractStatisticStore.hpp>
#include <folly/Synchronized.h>
#include <unordered_map>
#include <Identifiers/Identifiers.hpp>

namespace NES::Statistic {

class BuildStatisticStore : public AbstractStatisticStore {
  public:
    static StatisticStorePtr create();

    std::vector<StatisticPtr> getStatistics(const StatisticHash& statisticHash,
                                            const Windowing::TimeMeasure& startTs,
                                            const Windowing::TimeMeasure& endTs) override;
    std::optional<StatisticPtr> getSingleStatistic(const StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs,
                                    const Windowing::TimeMeasure& endTs) override;
    std::vector<HashStatisticPair> getAllStatistics() override;
    bool insertStatistic(const StatisticHash& statisticHash, StatisticPtr statistic) override;
    bool deleteStatistics(const StatisticHash& statisticHash,
                          const Windowing::TimeMeasure& startTs,
                          const Windowing::TimeMeasure& endTs) override;
    ~BuildStatisticStore() override;

  private:
    folly::Synchronized<std::unordered_map<StatisticHash, std::unordered_map<WatermarkTs, StatisticPtr>>>
        statisticHashToStatistics;
};

}// namespace NES::Statistic

#endif//NES_NES_RUNTIME_INCLUDE_STATISTICCOLLECTION_STATISTICSTORAGE_BUILDSTATISTICSTORE_HPP_
