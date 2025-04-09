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

#include <StatisticCollection/AbstractStatisticStore.hpp>

namespace NES::Runtime
{

// TODO(nikla44): create statistic classes and value interpreter
template <typename T>
using StatisticValue = T;

class StatisticStoreManager
{
public:
    static StatisticStoreManager& getStatisticStoreManagerInstance();

    template <typename T>
    std::vector<StatisticValue<T>>
    getStatistics(const StatisticHash& statisticHash, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs);
    [[nodiscard]] std::shared_ptr<AbstractStatisticStore> getStatisticStore() const;

private:
    StatisticStoreManager();

    // Delete copy constructor and assignment operator to prevent copying
    StatisticStoreManager(const StatisticStoreManager&) = delete;
    StatisticStoreManager& operator=(const StatisticStoreManager&) = delete;

    std::shared_ptr<AbstractStatisticStore> statisticStore;
};

}
