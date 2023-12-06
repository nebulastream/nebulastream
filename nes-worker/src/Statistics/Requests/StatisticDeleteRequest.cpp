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

#include "Statistics/Requests/StatisticDeleteRequest.hpp"
#include "Operators/LogicalOperators/Statistics/StatisticCollectorType.hpp"

namespace NES::Experimental::Statistics {

StatisticDeleteRequest::StatisticDeleteRequest(const std::string& logicalSourceName,
                                               const std::string& fieldName,
                                               const StatisticCollectorType statisticCollectorType,
                                               const time_t endTime,
                                               const bool stop)
    : StatisticRequest(logicalSourceName, fieldName, statisticCollectorType), physicalSourceNames(std::vector<std::string>{}),
      endTime(endTime), stop(stop) {}

std::vector<std::string>& StatisticDeleteRequest::getPhysicalSourceNames() {
    return physicalSourceNames;
}

const time_t& StatisticDeleteRequest::getEndTime() const {
    return endTime;
}

const bool& StatisticDeleteRequest::getStop() const {
    return stop;
}

void StatisticDeleteRequest::addPhysicalSourceName(const std::string& physicalSourceName) {
    this->physicalSourceNames.push_back(physicalSourceName);
    return;
}

}// namespace NES::Experimental::Statistics
