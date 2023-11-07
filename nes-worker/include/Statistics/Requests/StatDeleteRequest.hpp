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

#ifndef NES_NES_CORE_INCLUDE_STATISTICS_REQUESTS_STATDELETEREQUEST_HPP_
#define NES_NES_CORE_INCLUDE_STATISTICS_REQUESTS_STATDELETEREQUEST_HPP_

#include <memory>
#include <string>
#include <vector>

#include <Statistics/Requests/StatRequest.hpp>
#include <Statistics/StatCollectors/StatCollectorType.hpp>

namespace NES::Experimental::Statistics {

/**
 * @brief the inherited class that defines what is needed to delete Statistics
 */
class StatDeleteRequest : public StatRequest {
  public:
    StatDeleteRequest(const std::string& logicalSourceName,
                      const std::string& fieldName,
                      const StatCollectorType statCollectorType,
                      const time_t endTime)
        : StatRequest(logicalSourceName, fieldName, statCollectorType), endTime(endTime) {}

    /**
     * @return returns the latest possible time for which we want to keep statCollectors. Everything older is deleted
     */
    time_t getEndTime() const { return endTime; }

  private:
    time_t endTime;
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_CORE_INCLUDE_STATISTICS_REQUESTS_STATDELETEREQUEST_HPP_
