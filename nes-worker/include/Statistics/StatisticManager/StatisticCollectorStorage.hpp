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

#ifndef NES_NES_WORKER_INCLUDE_STATISTICS_STATISTICMANAGER_STATISTICCOLLECTORSTORAGE_HPP_
#define NES_NES_WORKER_INCLUDE_STATISTICS_STATISTICMANAGER_STATISTICCOLLECTORSTORAGE_HPP_

#include <Statistics/StatisticManager/StatisticCollectorIdentifier.hpp>
#include <unordered_map>

namespace NES::Experimental::Statistics {

class StatisticCollectorStorage {
  public:
    /**
     * a class that allows for the storage and retrieval of StatisticCollectors
     */
    StatisticCollectorStorage();

    /**
     * @brief receives a statistic and stores it in the trackedStatistics hash-map
     * @param statisticCollectorIdentifier a key, uniquely identifying the statistic
     * @param statistic the statistic itself
     */
    void createStatistic(const StatisticCollectorIdentifier& statisticCollectorIdentifier, const double statistic);

    /**
     * @brief given a unique identifier, the hash-map searches for the specific statistic and if available returns it
     * @param statisticCollectorIdentifier the unique identifier, specifying the statistic
     * @return a statistic
     */
    double probeStatistic(const StatisticCollectorIdentifier& statisticCollectorIdentifier);

    /**
     * @brief given a unique statistic identifier, the according statistic is deleted from the hash-map, if it is found
     * @param statisticCollectorIdentifier
     */
    void deleteStatistic(const StatisticCollectorIdentifier& statisticCollectorIdentifier);

  private:
    std::unordered_map<StatisticCollectorIdentifier, double, StatisticCollectorIdentifier::Hash> trackedStatistics;
};
}

#endif//NES_NES_WORKER_INCLUDE_STATISTICS_STATISTICMANAGER_STATISTICCOLLECTORSTORAGE_HPP_
