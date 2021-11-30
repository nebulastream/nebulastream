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

#ifndef NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_SINKS_MATERIALIZED_VIEW_SINK_DESCRIPTOR_HPP_
#define NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_SINKS_MATERIALIZED_VIEW_SINK_DESCRIPTOR_HPP_

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES::Experimental {

/**
 * @brief ***
 */
class MaterializedViewSinkDescriptor : public SinkDescriptor {

  public:
    /**
     * @brief ***
     */
    static SinkDescriptorPtr create(uint64_t mViewId, Schema schema);
    MaterializedViewSinkDescriptor(uint64_t mViewId, Schema schema);
    std::string toString() override;
    [[nodiscard]] bool equal(SinkDescriptorPtr const& other) override;
    uint64_t getMViewId();
    Schema getSchema();

  private:
    Schema schema;
    uint64_t mViewId;
};
using MaterializedViewSinkDescriptorPtr = std::shared_ptr<MaterializedViewSinkDescriptor>;
}// namespace NES::Experimental
#endif// NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_SINKS_MATERIALIZED_VIEW_SINK_DESCRIPTOR_HPP_
