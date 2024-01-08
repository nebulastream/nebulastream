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

#include <Util/StatisticCollectorIdentifier.hpp>

namespace NES::Experimental::Statistics {

StatisticCollectorIdentifier::StatisticCollectorIdentifier(const std::string& logicalSourceName,
                                                           const std::string& physicalSourceName,
                                                           const WorkerId& workerId,
                                                           const std::string& fieldName,
                                                           const StatisticCollectorType statisticCollectorType)
    : logicalSourceName(logicalSourceName), physicalSourceName(physicalSourceName), workerId(workerId), fieldName(fieldName),
      statisticCollectorType(statisticCollectorType) {}

bool StatisticCollectorIdentifier::operator==(const StatisticCollectorIdentifier& statisticCollectorIdentifier) const {
    return logicalSourceName == statisticCollectorIdentifier.getLogicalSourceName()
        && physicalSourceName == statisticCollectorIdentifier.getPhysicalSourceName()
        && workerId == statisticCollectorIdentifier.getWorkerId() && fieldName == statisticCollectorIdentifier.getFieldName()
        && statisticCollectorType == statisticCollectorIdentifier.getStatisticCollectorType();
}

const std::string& StatisticCollectorIdentifier::getLogicalSourceName() const { return logicalSourceName; }

const std::string& StatisticCollectorIdentifier::getPhysicalSourceName() const { return physicalSourceName; }

const WorkerId& StatisticCollectorIdentifier::getWorkerId() const { return workerId; }

const std::string& StatisticCollectorIdentifier::getFieldName() const { return fieldName; }

const StatisticCollectorType& StatisticCollectorIdentifier::getStatisticCollectorType() const { return statisticCollectorType; }

}// namespace NES::Experimental::Statistics
