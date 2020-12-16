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

#ifndef SINK_LOGICAL_OPERATOR_NODE_HPP
#define SINK_LOGICAL_OPERATOR_NODE_HPP

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Operators/LogicalOperators/Arity/UnaryOperatorNode.hpp>

namespace NES {

class SinkLogicalOperatorNode;
typedef std::shared_ptr<SinkLogicalOperatorNode> SinkLogicalOperatorNodePtr;

/**
 * @brief Node representing logical sink operator
 */
class SinkLogicalOperatorNode : public UnaryOperatorNode {
  public:
    SinkLogicalOperatorNode(OperatorId id);
    SinkLogicalOperatorNode(const SinkDescriptorPtr sinkDescriptor, OperatorId id);
    SinkLogicalOperatorNode& operator=(const SinkLogicalOperatorNode& other);
    bool isIdentical(NodePtr rhs) const override;
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
    SinkDescriptorPtr getSinkDescriptor();
    void setSinkDescriptor(SinkDescriptorPtr sinkDescriptor);
    OperatorNodePtr copy() override;

  private:
    SinkDescriptorPtr sinkDescriptor;
};
}// namespace NES
#endif// SINK_LOGICAL_OPERATOR_NODE_HPP
