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

#ifndef NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_MONITORINGSINKDESCRIPTOR_HPP_
#define NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_MONITORINGSINKDESCRIPTOR_HPP_

#include <Monitoring/MetricCollectors/MetricCollectorType.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Util/FaultToleranceType.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating a Monitoring sink as physical source
 */
class MonitoringSinkDescriptor : public SinkDescriptor {
  public:
    /**
     * @brief Factory method to create a new sink descriptor
     * @param faultToleranceType: fault tolerance type of a query
     * @param numberOfOrigins: number of origins of a given query
     * @return descriptor for Monitoring sink
     */
    static SinkDescriptorPtr create(Monitoring::MetricCollectorType collectorType,
                                    FaultToleranceType faultToleranceType = FaultToleranceType::NONE,
                                    uint64_t numberOfOrigins = 1);
    std::string toString() const override;
    [[nodiscard]] bool equal(SinkDescriptorPtr const& other) override;

    Monitoring::MetricCollectorType getCollectorType() const;
    void setCollectorType(Monitoring::MetricCollectorType collectorType);

  private:
    explicit MonitoringSinkDescriptor(Monitoring::MetricCollectorType collectorType,
                                      FaultToleranceType faultToleranceType,
                                      uint64_t numberOfOrigins);

  private:
    Monitoring::MetricCollectorType collectorType;
};

using MonitoringSinkDescriptorPtr = std::shared_ptr<MonitoringSinkDescriptor>;

}// namespace NES

#endif// NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_MONITORINGSINKDESCRIPTOR_HPP_
