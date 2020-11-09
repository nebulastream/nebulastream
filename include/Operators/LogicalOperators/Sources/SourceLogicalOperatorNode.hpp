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

#ifndef SOURCE_LOGICAL_OPERATOR_NODE_HPP
#define SOURCE_LOGICAL_OPERATOR_NODE_HPP

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <memory>

namespace NES {

/**
 * @brief Node representing logical source operator
 */
class SourceLogicalOperatorNode : public LogicalOperatorNode {
  public:
    explicit SourceLogicalOperatorNode(SourceDescriptorPtr sourceDescriptor, OperatorId id);

    /**
     * @brief Returns the source descriptor of the source operators.
     * @return SourceDescriptorPtr
     */
    SourceDescriptorPtr getSourceDescriptor();

    /**
     * @brief Sets a new source descriptor for this operator.
     * This can happen during query optimization.
     * @param sourceDescriptor
     */
    void setSourceDescriptor(SourceDescriptorPtr sourceDescriptor);

    /**
     * @brief Returns the result schema of a source operator, which is defined by the source descriptor.
     * @return SchemaPtr
     */
    bool inferSchema() override;

    bool equal(const NodePtr rhs) const override;

    bool isIdentical(NodePtr rhs) const override;
    const std::string toString() const override;
    OperatorNodePtr copy() override;
    z3::expr inferZ3Expression(z3::ContextPtr context) override;

  private:
    SourceDescriptorPtr sourceDescriptor;
};

typedef std::shared_ptr<SourceLogicalOperatorNode> SourceLogicalOperatorNodePtr;
}// namespace NES

#endif// SOURCE_LOGICAL_OPERATOR_NODE_HPP
