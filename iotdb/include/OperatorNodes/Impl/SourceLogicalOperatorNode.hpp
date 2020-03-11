#ifndef SOURCE_LOGICAL_OPERATOR_NODE_HPP
#define SOURCE_LOGICAL_OPERATOR_NODE_HPP

#include <memory>
#include <OperatorNodes/LogicalOperatorNode.hpp>


namespace NES {
class SourceLogicalOperatorNode : public LogicalOperatorNode {
public:
    SourceLogicalOperatorNode() = delete;
    SourceLogicalOperatorNode(const DataSourcePtr);
    SourceLogicalOperatorNode(const NodePtr op);
    SourceLogicalOperatorNode(const SourceLogicalOperatorNode* source);
    const NodePtr copy();
    const std::string toString() const override;

    bool equal(const NodePtr& rhs) const override ;

private:
    DataSourcePtr source_;
};

typedef std::shared_ptr<SourceLogicalOperatorNode> SourceLogicalOperatorNodePtr;
} // namespace NES

#endif // SOURCE_LOGICAL_OPERATOR_NODE_HPP
