#ifndef SOURCE_LOGICAL_OPERATOR_NODE_HPP
#define SOURCE_LOGICAL_OPERATOR_NODE_HPP


#include <OperatorNodes/LogicalOperatorNode.hpp>


namespace NES {
class SourceLogicalOperatorNode : public LogicalOperatorNode {
public:
    SourceLogicalOperatorNode() = delete;
    SourceLogicalOperatorNode(const DataSourcePtr);
    SourceLogicalOperatorNode(const OperatorPtr op);
    const BaseOperatorNodePtr copy();
    OperatorType getOperatorType() const override;
    const std::string toString() const override;
private:
    DataSourcePtr source_;
};

typedef std::shared_ptr<SourceLogicalOperatorNode> SourceLogicalOperatorNodePtr;
} // namespace NES

#endif // SOURCE_LOGICAL_OPERATOR_NODE_HPP
