#ifndef JOIN_LOGICAL_OPERATOR_NODE_HPP
#define JOIN_LOGICAL_OPERATOR_NODE_HPP

#include <OperatorNodes/LogicalOperatorNode.hpp>

namespace NES {

class JoinLogicalOperatorNode : public LogicalOperatorNode {
  public:
    JoinLogicalOperatorNode(const JoinPredicatePtr join_spec);
    const std::string toString() const override;
    OperatorType getOperatorType() const override;

    virtual bool equals(const BaseOperatorNode& rhs) const override;
  private:
    JoinPredicatePtr join_spec_;
};

} // namespace NES
#endif // JOIN_LOGICAL_OPERATOR_NODE_HPP
