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

#include <Statistics/Requests/StatisticProbeRequest.hpp>

namespace NES::Experimental::Statistics {

StatisticProbeRequest::StatisticProbeRequest(const std::string& logicalSourceName,
                                             const std::string& fieldName,
                                             const StatisticCollectorType statisticCollectorType,
                                             const std::string& probeExpression,
                                             const std::vector<std::string>& physicalSourceNames,
                                             const time_t startTime,
                                             const time_t endTime,
                                             const bool merge)
    : StatisticRequest(logicalSourceName, fieldName, statisticCollectorType), probeExpression(probeExpression),
      physicalSourceNames(physicalSourceNames), startTime(startTime), endTime(endTime), merge(merge) {}

const std::string& StatisticProbeRequest::getProbeExpression() const { return probeExpression; }

const std::vector<std::string>& StatisticProbeRequest::getPhysicalSourceNames() const { return physicalSourceNames; }

void StatisticProbeRequest::clearPhysicalSourceNames() {
    this->physicalSourceNames.clear();
    return;
}

void StatisticProbeRequest::addPhysicalSourceName(std::string& physicalSourceName) {
    physicalSourceNames.push_back(physicalSourceName);
    return;
}

const time_t& StatisticProbeRequest::getStartTime() const { return startTime; }

const time_t& StatisticProbeRequest::getEndTime() const { return endTime; }

const bool& StatisticProbeRequest::getMerge() const { return merge; }

}// namespace NES::Experimental::Statistics
