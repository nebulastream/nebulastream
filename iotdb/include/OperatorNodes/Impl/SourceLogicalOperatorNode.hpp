#ifndef SOURCE_LOGICAL_OPERATOR_NODE_HPP
#define SOURCE_LOGICAL_OPERATOR_NODE_HPP

#include <memory>
#include <OperatorNodes/LogicalOperatorNode.hpp>


namespace NES {
class SourceLogicalOperatorNode : public LogicalOperatorNode,
                                  public std::enable_shared_from_this<SourceLogicalOperatorNode> {
public:
    SourceLogicalOperatorNode() = delete;
    SourceLogicalOperatorNode(const DataSourcePtr);
    SourceLogicalOperatorNode(const OperatorPtr op);
    const BaseOperatorNodePtr copy();
    OperatorType getOperatorType() const override;
    const std::string toString() const override;

    virtual BaseOperatorNodePtr makeShared() override { return shared_from_this(); };
    virtual bool equals(const BaseOperatorNode& rhs) const override;

private:
    DataSourcePtr source_;
};

typedef std::shared_ptr<SourceLogicalOperatorNode> SourceLogicalOperatorNodePtr;
} // namespace NES

#endif // SOURCE_LOGICAL_OPERATOR_NODE_HPP
