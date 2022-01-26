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

#ifndef NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_SINKLOGICALOPERATORNODE_HPP_
#define NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_SINKLOGICALOPERATORNODE_HPP_

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES {

class SinkLogicalOperatorNode;
using SinkLogicalOperatorNodePtr = std::shared_ptr<SinkLogicalOperatorNode>;

/**
 * @brief Node representing logical sink operator
 */
class SinkLogicalOperatorNode : public LogicalUnaryOperatorNode {
  public:
    explicit SinkLogicalOperatorNode(OperatorId id);
    SinkLogicalOperatorNode(SinkDescriptorPtr const& sinkDescriptor, OperatorId id);
    SinkLogicalOperatorNode& operator=(const SinkLogicalOperatorNode& other);
    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    std::string toString() const override;
    SinkDescriptorPtr getSinkDescriptor();
    void setSinkDescriptor(SinkDescriptorPtr sinkDescriptor);
    OperatorNodePtr copy() override;
    bool inferSchema() override;
    void inferStringSignature() override;

  private:
    SinkDescriptorPtr sinkDescriptor;
};
}// namespace NES
#endif  // NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_SINKLOGICALOPERATORNODE_HPP_
