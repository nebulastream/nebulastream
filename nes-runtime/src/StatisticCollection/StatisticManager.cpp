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

#include <StatisticCollection/StatisticManager.hpp>
#include <Statistics/Synopses/CountMinStatistic.hpp>
#include <Statistics/Synopses/HyperLogLogStatistic.hpp>
#include <Statistics/Synopses/ReservoirSampleStatistic.hpp>
#include <Statistics/Synopses/EquiWidthHistogramStatistic.hpp>
#include <Statistics/Synopses/DDSketchStatistic.hpp>
#include <Util/Core.hpp>
#include <Util/StatisticProbeUtil.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Statistic {

StatisticManagerPtr StatisticManager::create(StatisticStorePtr statisticStore) {
    return std::make_shared<StatisticManager>(StatisticManager(statisticStore));
}

std::vector<StatisticValue<>> StatisticManager::getStatistics(const StatisticProbeRequest& probeRequest) {
    auto statistics = statisticStore->getStatistics(probeRequest.statisticHash, probeRequest.startTs, probeRequest.endTs);
    std::vector<StatisticValue<>> statisticValues;
    for (const auto& statistic : statistics) {
        // We might rewrite this later to use the query compiler. For now, we solve this not in the most efficient way.
        if (statistic->instanceOf<ReservoirSampleStatistic>()) {
            const auto reservoirSample = statistic->as<ReservoirSampleStatistic>();
            const auto sampleSchema = reservoirSample->getSchema();
            const auto sampleSizeInBytes = reservoirSample->getSampleSize() * sampleSchema->getSchemaSizeInBytes();
            const auto memoryLayout = Util::createMemoryLayout(sampleSchema, sampleSizeInBytes);
            auto statisticValue = StatisticProbeUtil::probeReservoirSample(*reservoirSample,
                                                                           probeRequest.probeExpression,
                                                                           memoryLayout);
            statisticValues.emplace_back(statisticValue);
        } else if (statistic->instanceOf<HyperLogLogStatistic>()) {
            const auto hyperLogLog = statistic->as<HyperLogLogStatistic>();
            statisticValues.emplace_back(StatisticValue<>(hyperLogLog->getEstimate(), hyperLogLog->getStartTs(), hyperLogLog->getEndTs()));
        } else if (statistic->instanceOf<CountMinStatistic>()) {
            const auto countMin = statistic->as<CountMinStatistic>();
            auto statisticValue = StatisticProbeUtil::probeCountMin(*countMin, probeRequest.probeExpression);
            statisticValues.emplace_back(statisticValue);
        } else if (statistic->instanceOf<EquiWidthHistogramStatistic>()) {
            const auto equiWidthHistogram = statistic->as<EquiWidthHistogramStatistic>();
            auto statisticValue = StatisticProbeUtil::probeEquiWidthHistogram(*equiWidthHistogram, probeRequest.probeExpression);
            statisticValues.emplace_back(statisticValue);
        } else if (statistic->instanceOf<DDSketchStatistic>()) {
            const auto ddSketch = statistic->as<DDSketchStatistic>();
            auto statisticValue = StatisticProbeUtil::probeDDSketch(*ddSketch, probeRequest.probeExpression);
            statisticValues.emplace_back(statisticValue);
        } else {
            NES_NOT_IMPLEMENTED();
        }
    }

    return statisticValues;
}

StatisticManager::StatisticManager(const StatisticStorePtr& statisticStore) : statisticStore(statisticStore) {}

StatisticStorePtr StatisticManager::getStatisticStore() const { return statisticStore; }
}// namespace NES::Statistic
