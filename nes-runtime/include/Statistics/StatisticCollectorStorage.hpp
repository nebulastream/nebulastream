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

#include <Util/StatisticCollectorIdentifier.hpp>
#include <unordered_map>

namespace NES::Experimental::Statistics {

class Statistic;
using StatisticPtr = std::shared_ptr<Statistic>;

class StatisticCollectorIdentifier;
using StatisticCollectorIdentifierPtr = std::shared_ptr<StatisticCollectorIdentifier>;

class StatisticProbeParameter;
using StatisticProbeParameterPtr = std::shared_ptr<StatisticProbeParameter>;

class StatisticCollectorStorage {
  public:
    /**
     * a class that allows for the storage and retrieval of Statistics
     */
    StatisticCollectorStorage();

    /**
     * @brief receives a Statistic obj and stores it in the trackedStatistics hash-map
     * @param statisticCollectorIdentifier a key, uniquely identifying the statistic
     * @param statistic the statistic obj itself
     */
    void addStatistic(StatisticCollectorIdentifier& statisticCollectorIdentifier, const StatisticPtr statistic);

    /**
     * @brief adds numerous Statistics to the storage
     * @param allStatisticCollectors the vector of Statistic objects
     */
    void addStatistics(std::vector<StatisticPtr> allStatistics);

    /**
     * @brief given a unique identifier, the hash-map searches for the specific statistic and if available returns it
     * @param statisticCollectorIdentifier the unique identifier, specifying the statistic
     * @param probeExpression
     * @return a statistic
     */
    double probeStatistic(const StatisticCollectorIdentifier& statisticCollectorIdentifier, StatisticProbeParameterPtr& probeParameter);

    /**
     * @brief given a unique statistic identifier, the according statistic is deleted from the hash-map, if it is found
     * @param statisticCollectorIdentifier
     */
    void deleteStatistic(const StatisticCollectorIdentifier& statisticCollectorIdentifier);

    /**
     * @param the key with which we identify the statistic that we want
     * @return a statistic object 
     */
    StatisticPtr getStatistic(StatisticCollectorIdentifier& statisticCollectorIdentifier);

  private:
    std::unordered_map<StatisticCollectorIdentifier, StatisticPtr, StatisticCollectorIdentifier::Hash> trackedStatistics;
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_WORKER_INCLUDE_STATISTICS_STATISTICMANAGER_STATISTICCOLLECTORSTORAGE_HPP_
