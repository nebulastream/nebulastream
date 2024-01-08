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

#include <string>
#include <Operators/LogicalOperators/Sinks/StatisticStorageSinkDescriptor.hpp>

namespace NES::Experimental::Statistics {

StatisticStorageSinkDescriptor::StatisticStorageSinkDescriptor(StatisticCollectorType statisticCollectorType,
                                                               uint64_t numberOfOrigins)
    : SinkDescriptor(numberOfOrigins), statisticCollectorType(statisticCollectorType) {}

SinkDescriptorPtr StatisticStorageSinkDescriptor::create(StatisticCollectorType statisticCollectorType,
                                                         uint64_t numberOfOrigins) {
    return std::make_shared<StatisticStorageSinkDescriptor>(
        StatisticStorageSinkDescriptor(statisticCollectorType, numberOfOrigins));
}

std::string StatisticStorageSinkDescriptor::toString() const { return "StatisticStorageSinkDescriptor()"; }

bool StatisticStorageSinkDescriptor::equal(const NES::SinkDescriptorPtr& other) {
    if (!other->instanceOf<StatisticStorageSinkDescriptor>()) {
        return false;
    }
    auto otherSinkDesc = other->as<StatisticStorageSinkDescriptor>();
    return this->statisticCollectorType == otherSinkDesc->statisticCollectorType;
}

StatisticCollectorType StatisticStorageSinkDescriptor::getStatisticCollectorType() const { return statisticCollectorType; }

}// namespace NES::Experimental::Statistics
