#ifndef WINDOW_LOGICAL_OPERATOR_NODE_HPP
#define WINDOW_LOGICAL_OPERATOR_NODE_HPP

#include <OperatorNodes/BaseOperatorNode.hpp>

namespace NES {

class WindowLogicalOperatorNode : public BaseOperatorNode {
  public:

    WindowLogicalOperatorNode(const WindowDefinitionPtr& window_defintion);
    const std::string toString() const override;
    OperatorType getOperatorType() const override;
    const WindowDefinitionPtr& getWindowDefinition() const;

    virtual bool equals(const BaseOperatorNode& rhs) const override;
  private:
    WindowDefinitionPtr window_definition;
};

} // namespace NES
#endif // WINDOW_LOGICAL_OPERATOR_NODE_HPP
