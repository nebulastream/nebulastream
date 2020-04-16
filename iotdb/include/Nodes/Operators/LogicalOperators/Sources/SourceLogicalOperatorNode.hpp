#ifndef SOURCE_LOGICAL_OPERATOR_NODE_HPP
#define SOURCE_LOGICAL_OPERATOR_NODE_HPP

#include <memory>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>

namespace NES {
class SourceLogicalOperatorNode : public LogicalOperatorNode {
public:
    SourceLogicalOperatorNode() = delete;
    SourceLogicalOperatorNode(const DataSourcePtr);
    const NodePtr copy();
    const std::string toString() const override;
    bool equal(const NodePtr rhs) const override;

    DataSourcePtr getDataSource();

private:
    DataSourcePtr source;
};

typedef std::shared_ptr<SourceLogicalOperatorNode> SourceLogicalOperatorNodePtr;
} // namespace NES

#endif // SOURCE_LOGICAL_OPERATOR_NODE_HPP
