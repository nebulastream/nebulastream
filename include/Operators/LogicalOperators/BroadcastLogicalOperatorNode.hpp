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

#ifndef NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_BROADCAST_LOGICAL_OPERATOR_NODE_HPP_
#define NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_BROADCAST_LOGICAL_OPERATOR_NODE_HPP_

#include <Operators/AbstractOperators/Arity/ExchangeOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>

namespace NES {

/**
 * @brief broadcast operator is used for efficiently broadcasting the tuples form its upstream operator to all its downstream operators.
 */
class BroadcastLogicalOperatorNode : public ExchangeOperatorNode, public LogicalOperatorNode {
  public:
    explicit BroadcastLogicalOperatorNode(OperatorId id);
    ~BroadcastLogicalOperatorNode() override = default;

    bool inferSchema() override;
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;
    OperatorNodePtr copy() override;
    void inferStringSignature() override;
};
}// namespace NES

#endif// NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_BROADCAST_LOGICAL_OPERATOR_NODE_HPP_
