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

#ifndef NES_NES_WORKER_INCLUDE_STATISTICS_REQUESTS_STATISTICREQUEST_HPP_
#define NES_NES_WORKER_INCLUDE_STATISTICS_REQUESTS_STATISTICREQUEST_HPP_

#include <memory>
#include <string>
#include <vector>

#include <Statistics/StatisticCollectors/StatisticCollectorType.hpp>

namespace NES::Experimental::Statistics {
/**
 * @brief The abstract class from which all requests inherit
 */
class StatisticRequest {
  public:
    /**
     * @brief the general class containing common parameters for all statistic requests
     * @param logicalSourceName the logical name of the source
     * @param fieldName the name of the field
     * @param statisticCollectorType the type of statistic/data structure with which the statistic is constructed
     */
    StatisticRequest(const std::string& logicalSourceName,
                     const std::string& fieldName,
                     const StatisticCollectorType statisticCollectorType);

    /**
     * @return returns the logicalStreamName of the request
     */
    std::string getLogicalSourceName() const;

    /**
     * @return returns the fieldName of the request
     */
    std::string getFieldName() const;

    /**
     * @return returns the type of StatisticCollector with which the statistic is to be generated
     */
    StatisticCollectorType getStatisticCollectorType() const;

  private:
    std::string logicalSourceName;
    std::string fieldName;
    StatisticCollectorType statisticCollectorType;
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_WORKER_INCLUDE_STATISTICS_REQUESTS_STATISTICREQUEST_HPP_
