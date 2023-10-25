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

#include <Configurations/Worker/PhysicalSourceTypes/MonitoringSourceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <string>

namespace NES {

MonitoringSourceType::MonitoringSourceType(const std::string& logicalSourceName,
                                           const std::string& physicalSourceName,
                                           Monitoring::MetricCollectorType metricCollectorType,
                                           std::chrono::milliseconds waitTime)
    : PhysicalSourceType(logicalSourceName, physicalSourceName, SourceType::MONITORING_SOURCE),
      metricCollectorType(metricCollectorType), waitTime(waitTime) {}

MonitoringSourceTypePtr MonitoringSourceType::create(const std::string& logicalSourceName,
                                                     const std::string& physicalSourceName,
                                                     Monitoring::MetricCollectorType metricCollectorType,
                                                     std::chrono::milliseconds waitTime) {
    return std::make_shared<MonitoringSourceType>(
        MonitoringSourceType(logicalSourceName, physicalSourceName, metricCollectorType, waitTime));
}

MonitoringSourceTypePtr MonitoringSourceType::create(const std::string& logicalSourceName,
                                                     const std::string& physicalSourceName,
                                                     Monitoring::MetricCollectorType metricCollectorType) {
    return create(logicalSourceName, physicalSourceName, metricCollectorType, DEFAULT_WAIT_TIME);
}

std::string MonitoringSourceType::toString() {
    std::stringstream ss;
    ss << "MonitoringSource Type => {\n";
    ss << "waitTimeInMs:" + std::to_string(waitTime.count());
    ss << "metricCollectorType:" + std::string(magic_enum::enum_name(Monitoring::MetricCollectorType(metricCollectorType)));
    ss << "\n}";
    return ss.str();
}

bool MonitoringSourceType::equal(const PhysicalSourceTypePtr& other) {
    if (!other->instanceOf<MonitoringSourceType>()) {
        return false;
    }
    auto otherSourceConfig = other->as<MonitoringSourceType>();
    return waitTime == otherSourceConfig->waitTime && metricCollectorType == otherSourceConfig->metricCollectorType;
}

void MonitoringSourceType::reset() { setWaitTime(DEFAULT_WAIT_TIME); }

std::chrono::milliseconds MonitoringSourceType::getWaitTime() const { return waitTime; }

void MonitoringSourceType::setWaitTime(std::chrono::milliseconds waitTime) { this->waitTime = waitTime; }

Monitoring::MetricCollectorType MonitoringSourceType::getMetricCollectorType() const { return metricCollectorType; }

void MonitoringSourceType::setMetricCollectorType(Monitoring::MetricCollectorType metricCollectorType) {
    this->metricCollectorType = metricCollectorType;
}

}// namespace NES
