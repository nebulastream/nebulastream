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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCELOGICALOPERATORNODE_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCELOGICALOPERATORNODE_HPP_

#include <Operators/AbstractOperators/OriginIdAssignmentOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>

namespace NES {

/**
 * @brief Node representing logical source operator
 */
class SourceLogicalOperatorNode : public LogicalUnaryOperatorNode, public OriginIdAssignmentOperator {
  public:
    explicit SourceLogicalOperatorNode(SourceDescriptorPtr const& sourceDescriptor, OperatorId id);
    explicit SourceLogicalOperatorNode(SourceDescriptorPtr const& sourceDescriptor, OperatorId id, OriginId originId);

    /**
     * @brief Returns the source descriptor of the source operators.
     * @return SourceDescriptorPtr
     */
    SourceDescriptorPtr getSourceDescriptor() const;

    /**
     * @brief Sets a new source descriptor for this operator.
     * This can happen during query optimization.
     * @param sourceDescriptor
     */
    void setSourceDescriptor(SourceDescriptorPtr sourceDescriptor);

    /**
     * @brief Returns the result schema of a source operator, which is defined by the source descriptor.
     * @param typeInferencePhaseContext needed for stamp inferring
     * @return true if schema was correctly inferred
     */
    bool inferSchema() override;

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;
    void inferStringSignature() override;
    OperatorNodePtr copy() override;
    void setProjectSchema(SchemaPtr schema);
    void inferInputOrigins() override;
    const std::vector<OriginId> getOutputOriginIds() const override;

  private:
    SourceDescriptorPtr sourceDescriptor;
    SchemaPtr projectSchema;
};

using SourceLogicalOperatorNodePtr = std::shared_ptr<SourceLogicalOperatorNode>;
}// namespace NES

#endif // NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCELOGICALOPERATORNODE_HPP_
