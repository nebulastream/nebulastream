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

#ifndef Merge_LOGICAL_OPERATOR_NODE_HPP
#define Merge_LOGICAL_OPERATOR_NODE_HPP

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <memory>
#include <z3++.h>

namespace NES {

class MergeLogicalOperatorNode;
typedef std::shared_ptr<MergeLogicalOperatorNode> MergeLogicalOperatorNodePtr;

/**
 * @brief Merge operator, which contains an expression as a predicate.
 */
class MergeLogicalOperatorNode : public LogicalOperatorNode {
  public:
    explicit MergeLogicalOperatorNode(OperatorId id);
    ~MergeLogicalOperatorNode() = default;

    bool isIdentical(NodePtr rhs) const override;
    const std::string toString() const override;
    //infer schema of two child operators
    bool inferSchema() override;
    OperatorNodePtr copy() override;
    bool equal(const NodePtr rhs) const override;
    z3::expr inferZ3Expression(z3::ContextPtr context) override;
};
}// namespace NES
#endif// Merge_LOGICAL_OPERATOR_NODE_HPP