#ifndef SOURCE_LOGICAL_OPERATOR_NODE_HPP
#define SOURCE_LOGICAL_OPERATOR_NODE_HPP

#include <memory>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>

namespace NES {

/**
 * @brief Node representing logical source operator
 */
class SourceLogicalOperatorNode : public LogicalOperatorNode {
public:
    SourceLogicalOperatorNode(const SourceDescriptorPtr sourceDescriptor);
    const NodePtr copy();
    const std::string toString() const override;
    bool equal(const NodePtr rhs) const override;
    SourceDescriptorPtr getSourceDescriptor();

private:

    SourceLogicalOperatorNode() = delete;
    SourceDescriptorPtr sourceDescriptor;
};

typedef std::shared_ptr<SourceLogicalOperatorNode> SourceLogicalOperatorNodePtr;
} // namespace NES

#endif // SOURCE_LOGICAL_OPERATOR_NODE_HPP
