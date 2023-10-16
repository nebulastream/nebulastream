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

#ifndef NES_NES_WORKER_INCLUDE_STATISTICS_REQUESTS_STATISTICDELETEREQUEST_HPP_
#define NES_NES_WORKER_INCLUDE_STATISTICS_REQUESTS_STATISTICDELETEREQUEST_HPP_

#include <Statistics/Requests/StatisticRequest.hpp>
#include <Statistics/StatisticCollectors/StatisticCollectorType.hpp>
#include <memory>
#include <string>
#include <vector>

namespace NES::Experimental::Statistics {

/**
 * @brief the inherited class that defines what is needed to delete Statistics
 */
class StatisticDeleteRequest : public StatisticRequest {
  public:
    /**
     * @brief the constructor of the StatisticDeleteRequest that contains all necessary information to stop a statistic query and delete the according StatisticCollectors
     * @param logicalSourceName the logical source over which the query is defined
     * @param fieldName the field name over which the statistic was generated
     * @param statisticCollectorType the type of data structure/statistic
     * @param endTime the latest possible time for which we want to keep statisticCollectors. Everything older is deleted
     * @param stop a bool that defines whether we want to stop the query as well or whether we only wish to delete old statistics
     */
    StatisticDeleteRequest(const std::string& logicalSourceName,
                           const std::string& fieldName,
                           const StatisticCollectorType statisticCollectorType,
                           const time_t endTime,
                           const bool stop = true);

    /**
     * @return returns the vector of physicalSourceNames that is required for the deletion of
     */
    [[nodiscard]] std::vector<std::string>& getPhysicalSourceNames();

    /**
     * @return returns the latest possible time for which we want to keep statisticCollectors. Everything older is deleted
     */
    [[nodiscard]] const time_t& getEndTime() const;

    /**
     * @return returns the boolean that defines whether we also want to stop the query
     */
    [[nodiscard]] const bool& getStop() const;

  private:
    void addPhysicalSourceName(const std::string& physicalSourceName);
    std::vector<std::string> physicalSourceNames;
    time_t endTime;
    bool stop;
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_WORKER_INCLUDE_STATISTICS_REQUESTS_STATISTICDELETEREQUEST_HPP_
