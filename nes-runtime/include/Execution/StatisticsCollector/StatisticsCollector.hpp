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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_STATISTICSCOLLECTOR_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_STATISTICSCOLLECTOR_HPP_

#include <Execution/StatisticsCollector/Statistic.hpp>
#include <memory>
#include <vector>
#include <Execution/StatisticsCollector/CollectorTrigger.hpp>

namespace NES::Runtime::Execution {
/**
 * @brief Collects various statistics that can be implemented with the interface Statistic.
 */
class StatisticsCollector {
  public:
    /**
     * @brief Constructor for the StatisticsCollector.
     */
    StatisticsCollector() : idToStatisticMap(){};

    /**
     * @brief Adds a statistic that should be collected to the list of statistics.
     * @param statistic that should be collected.
     */
    void addStatistic(uint64_t id, std::shared_ptr<Statistic> statistic);

    /**
     * @brief Handles different triggers that trigger the collection of a statistic.
     * @param trigger triggers the collection of a statistic.
     */
    void updateStatisticsHandler(CollectorTrigger trigger);

  private:
    std::map<uint64_t,std::vector<std::shared_ptr<Statistic>>> idToStatisticMap;
};

}// namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_EXECUTION_STATISTICSCOLLECTOR_HPP_