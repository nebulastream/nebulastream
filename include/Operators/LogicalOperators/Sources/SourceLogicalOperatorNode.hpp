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

#ifndef NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_SOURCES_SOURCE_LOGICAL_OPERATOR_NODE_HPP_
#define NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_SOURCES_SOURCE_LOGICAL_OPERATOR_NODE_HPP_

#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>

namespace NES {

/**
 * @brief Node representing logical source operator
 */
class SourceLogicalOperatorNode : public LogicalUnaryOperatorNode {
  public:
    explicit SourceLogicalOperatorNode(SourceDescriptorPtr const& sourceDescriptor, OperatorId id);

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

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;
    void inferStringSignature() override;
    OperatorNodePtr copy() override;
    void setProjectSchema(SchemaPtr schema);

  private:
    SourceDescriptorPtr sourceDescriptor;
    SchemaPtr projectSchema;
};

using SourceLogicalOperatorNodePtr = std::shared_ptr<SourceLogicalOperatorNode>;
}// namespace NES

#endif  // NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_SOURCES_SOURCE_LOGICAL_OPERATOR_NODE_HPP_
