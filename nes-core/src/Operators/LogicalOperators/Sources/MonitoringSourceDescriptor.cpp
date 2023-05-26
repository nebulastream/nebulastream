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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/MonitoringSourceDescriptor.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES {

MonitoringSourceDescriptor::MonitoringSourceDescriptor(std::chrono::milliseconds waitTime,
                                                       Monitoring::MetricCollectorType metricCollectorType)
    : SourceDescriptor(Schema::create()), waitTime(waitTime), metricCollectorType(metricCollectorType) {}

SourceDescriptorPtr MonitoringSourceDescriptor::create(std::chrono::milliseconds waitTime,
                                                       Monitoring::MetricCollectorType metricCollectorType) {
    return std::make_shared<MonitoringSourceDescriptor>(MonitoringSourceDescriptor(waitTime, metricCollectorType));
}

bool MonitoringSourceDescriptor::equal(SourceDescriptorPtr const& other) const {
    if (!other->instanceOf<const MonitoringSourceDescriptor>()) {
        return false;
    }
    auto otherNetworkSource = other->as<const MonitoringSourceDescriptor>();
    return waitTime == otherNetworkSource->getWaitTime() && metricCollectorType == otherNetworkSource->getMetricCollectorType();
}

std::string MonitoringSourceDescriptor::toString() const {
    return "MonitoringSourceDescriptor(" + std::string(magic_enum::enum_name(metricCollectorType)) + ")";
}

SourceDescriptorPtr MonitoringSourceDescriptor::copy() {
    auto copy = MonitoringSourceDescriptor::create(waitTime, metricCollectorType);
    copy->setPhysicalSourceName(physicalSourceName);
    return copy;
}

std::chrono::milliseconds MonitoringSourceDescriptor::getWaitTime() const { return waitTime; }

Monitoring::MetricCollectorType MonitoringSourceDescriptor::getMetricCollectorType() const { return metricCollectorType; }

}// namespace NES
