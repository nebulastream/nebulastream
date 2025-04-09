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

#include <StatisticCollection/DefaultStatisticStore.hpp>
#include <StatisticCollection/StatisticStoreManager.hpp>

namespace NES::Runtime
{

StatisticStoreManager& StatisticStoreManager::getStatisticStoreManagerInstance()
{
    static StatisticStoreManager instance;
    return instance;
}

template <typename T>
std::vector<StatisticValue<T>> StatisticStoreManager::getStatistics(
    const StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    auto statistics = statisticStore->getStatistics(statisticHash, startTs, endTs);
    for (const auto& statistic : statistics)
    {
        // TODO(nikla44): reinterpret statistics as dedicated statistic type
    }
    return {};
}

std::shared_ptr<AbstractStatisticStore> StatisticStoreManager::getStatisticStore() const
{
    return statisticStore;
}

StatisticStoreManager::StatisticStoreManager() : statisticStore(std::make_shared<DefaultStatisticStore>())
{
}

}
