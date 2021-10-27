/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_SINKS_PRINT_SINK_DESCRIPTOR_HPP_
#define NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_SINKS_PRINT_SINK_DESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical print sink
 */
class PrintSinkDescriptor : public SinkDescriptor {

  public:
    /**
     * @brief Factory method to create a new prink sink descriptor
     * @return descriptor for print sink
     */
    static SinkDescriptorPtr create();
    std::string toString() override;
    [[nodiscard]] bool equal(SinkDescriptorPtr const& other) override;

  private:
    explicit PrintSinkDescriptor();
};

using PrintSinkDescriptorPtr = std::shared_ptr<PrintSinkDescriptor>;

}// namespace NES

#endif  // NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_SINKS_PRINT_SINK_DESCRIPTOR_HPP_
