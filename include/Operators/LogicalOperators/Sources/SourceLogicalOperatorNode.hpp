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
    explicit SourceLogicalOperatorNode(SourceDescriptorPtr sourceDescriptor);

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

  private:
    SourceDescriptorPtr sourceDescriptor;
};

typedef std::shared_ptr<SourceLogicalOperatorNode> SourceLogicalOperatorNodePtr;
}// namespace NES

#endif// SOURCE_LOGICAL_OPERATOR_NODE_HPP
