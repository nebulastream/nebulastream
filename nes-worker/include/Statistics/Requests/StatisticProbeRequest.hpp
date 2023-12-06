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

#ifndef NES_NES_WORKER_INCLUDE_STATISTICS_REQUESTS_STATISTICPROBEREQUEST_HPP_
#define NES_NES_WORKER_INCLUDE_STATISTICS_REQUESTS_STATISTICPROBEREQUEST_HPP_

#include "Operators/LogicalOperators/Statistics/StatisticCollectorType.hpp"
#include <Statistics/Requests/StatisticRequest.hpp>
#include <memory>
#include <string>
#include <vector>

namespace NES::Experimental::Statistics {

/**
 * @brief the inherited class that defines what is needed to probe Statistics
 */
class StatisticProbeRequest : public StatisticRequest {
  public:
    StatisticProbeRequest(const std::string& logicalSourceName,
                          const std::string& fieldName,
                          const StatisticCollectorType statisticCollectorType,
                          const std::string& probeExpression,
                          const std::vector<std::string>& physicalSourceNames,
                          const time_t startTime,
                          const time_t endTime,
                          const bool merge = false);

    /**
     * @return returns the expression that is used to define what statistic(s) are to be queried
     */
    [[nodiscard]] const std::string& getProbeExpression() const;

    /**
     * @return returns the physicalSourceNames over which the statisticCollectors were generated that we wish to probe/query
     */
    [[nodiscard]] const std::vector<std::string>& getPhysicalSourceNames() const;

    /**
     * @brief deletes the entries of the physicalSourceNames
     */
    void clearPhysicalSourceNames();

    /**
     * @param physicalSourceName adds a entry to the list/vector of physicalSourceNames
     */
    void addPhysicalSourceName(std::string& physicalSourceName);

    /**
     * @return returns the first possibleTime for which we want to query/probe one or more statistics
     */
    [[nodiscard]] const time_t& getStartTime() const;

    /**
     * @return returns the last possible time for which we want to probe/query one or more statistics
     */
    [[nodiscard]] const time_t& getEndTime() const;

    /**
     * @return returns true or false and describes wether we want to merge multiple local statistics or
     * output them separately
     */
    [[nodiscard]] const bool& getMerge() const;

  private:
    std::string probeExpression;
    std::vector<std::string> physicalSourceNames;
    time_t startTime;
    time_t endTime;
    bool merge;
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_WORKER_INCLUDE_STATISTICS_REQUESTS_STATISTICPROBEREQUEST_HPP_
