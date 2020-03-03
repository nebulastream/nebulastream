#ifndef KEYBY_LOGICAL_OPERATOR_NODE_HPP
#define KEYBY_LOGICAL_OPERATOR_NODE_HPP

#include <memory>
#include <OperatorNodes/BaseOperatorNode.hpp>

namespace NES {

class KeyByLogicalOperatorNode : public BaseOperatorNode,
                                 public std::enable_shared_from_this<KeyByLogicalOperatorNode> {
  public:
    KeyByLogicalOperatorNode(const Attributes& keyby_spec);
    const std::string toString() const override;
    OperatorType getOperatorType() const override;

    virtual BaseOperatorNodePtr makeShared() override { return shared_from_this(); };
    virtual bool equals(const BaseOperatorNode& rhs) const override;
  private:
    Attributes keyby_spec_;
};

} // namespace NES
#endif // KEYBY_LOGICAL_OPERATOR_NODE_HPP
