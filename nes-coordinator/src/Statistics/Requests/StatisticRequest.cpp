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

#include <Statistics/Requests/StatisticRequest.hpp>

namespace NES::Experimental::Statistics {

StatisticRequest::StatisticRequest(const std::string& logicalSourceName,
                                   const std::string& fieldName,
                                   const StatisticCollectorType statisticCollectorType)
    : logicalSourceName(logicalSourceName), fieldName(fieldName), statisticCollectorType(statisticCollectorType) {}

std::string StatisticRequest::getLogicalSourceName() const { return logicalSourceName; }

std::string StatisticRequest::getFieldName() const { return fieldName; }

StatisticCollectorType StatisticRequest::getStatisticCollectorType() const { return statisticCollectorType; }
}// namespace NES::Experimental::Statistics
