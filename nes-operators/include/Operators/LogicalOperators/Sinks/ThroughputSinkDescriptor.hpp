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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_THROUGHPUTSINKDESCRIPTOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_THROUGHPUTSINKDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical print sink
 */
class ThroughputSinkDescriptor : public SinkDescriptor {

  public:
    /**
     * @brief Factory method to create a new throughput sink descriptor
     * @param counterName name of the shared throughput counter
     * @return descriptor for throughput sink
     */
    static SinkDescriptorPtr create(std::string counterName, size_t reportingThreshhold = 10000);
    [[nodiscard]] std::string toString() const override;
    [[nodiscard]] bool equal(SinkDescriptorPtr const& other) override;

    /**
     * @brief getter for shared throughput counter name
     * @return throughput counter name
     */
    [[nodiscard]] const std::string& getCounterName() const;
    [[nodiscard]] size_t getReportingThreshhold() const;

  private:
    std::string counterName;
    size_t reportingThreshhold = 10000;
    explicit ThroughputSinkDescriptor(std::string counterName, size_t reportingThreshhold);
};

using ThroughputSinkDescriptorPtr = std::shared_ptr<ThroughputSinkDescriptor>;

}// namespace NES

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_THROUGHPUTSINKDESCRIPTOR_HPP_
