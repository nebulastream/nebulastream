#ifndef KEYBY_LOGICAL_OPERATOR_NODE_HPP
#define KEYBY_LOGICAL_OPERATOR_NODE_HPP

#include <memory>
#include <Nodes/Node.hpp>
#include <API/ParameterTypes.hpp>

namespace NES {

class KeyByLogicalOperatorNode : public Node {
  public:
    KeyByLogicalOperatorNode(const Attributes& keyby_spec);
    const std::string toString() const override;

  private:
    Attributes keyby_spec_;
};

} // namespace NES
#endif // KEYBY_LOGICAL_OPERATOR_NODE_HPP
