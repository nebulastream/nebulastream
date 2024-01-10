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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_MONITORINGSOURCEDESCRIPTOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_MONITORINGSOURCEDESCRIPTOR_HPP_

#include <Configurations/Worker/PhysicalSourceTypes/MonitoringSourceType.hpp>
#include <Util/MetricCollectorType.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <chrono>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical source for monitoring
 */
class MonitoringSourceDescriptor : public SourceDescriptor {
  public:
    static SourceDescriptorPtr create(std::chrono::milliseconds waitTime, Monitoring::MetricCollectorType metricCollectorType);

    /**
     * @brief Return the wait time between sampling periods.
     * @return the wait time
     */
    std::chrono::milliseconds getWaitTime() const;

    /**
     * @brief Return the type of metric collector used for the monitoring source
     * @return the metric collector
     */
    Monitoring::MetricCollectorType getMetricCollectorType() const;

    [[nodiscard]] bool equal(SourceDescriptorPtr const& other) const override;
    std::string toString() const override;
    SourceDescriptorPtr copy() override;

  private:
    explicit MonitoringSourceDescriptor(std::chrono::milliseconds waitTime, Monitoring::MetricCollectorType metricCollectorType);
    std::chrono::milliseconds waitTime;
    Monitoring::MetricCollectorType metricCollectorType;
};

using MonitoringSourceDescriptorPtr = std::shared_ptr<MonitoringSourceDescriptor>;

}// namespace NES

#endif // NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_MONITORINGSOURCEDESCRIPTOR_HPP_
