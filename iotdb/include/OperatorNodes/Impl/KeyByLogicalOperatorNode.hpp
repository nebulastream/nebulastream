#ifndef KEYBY_LOGICAL_OPERATOR_NODE_HPP
#define KEYBY_LOGICAL_OPERATOR_NODE_HPP

#include <OperatorNodes/BaseOperatorNode.hpp>

namespace NES {

class KeyByLogicalOperatorNode : public BaseOperatorNode {
  public:
    KeyByLogicalOperatorNode(const Attributes& keyby_spec);
    const std::string toString() const override;
    OperatorType getOperatorType() const override;

  private:
    Attributes keyby_spec_;
};

} // namespace NES
#endif // KEYBY_LOGICAL_OPERATOR_NODE_HPP
