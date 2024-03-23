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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_LATENCYDESCRIPTOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_LATENCYDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical print sink
 */
class LatencySinkDescriptor : public SinkDescriptor {

  public:
    /**
     * @brief Factory method to create a new throughput sink descriptor
     * @return descriptor for throughput sink
     */
    static SinkDescriptorPtr create(size_t reportingThreshholdInMs = 1);
    [[nodiscard]] std::string toString() const override;
    [[nodiscard]] bool equal(SinkDescriptorPtr const& other) override;

    [[nodiscard]] size_t getReportingThreshhold() const;

  private:
    size_t reportingThreshholdInMs = 1;
    explicit LatencySinkDescriptor(size_t reportingThreshhold);
};

using LatencySinkDescriptorPtr = std::shared_ptr<LatencySinkDescriptor>;

}// namespace NES

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_LATENCYDESCRIPTOR_HPP_
