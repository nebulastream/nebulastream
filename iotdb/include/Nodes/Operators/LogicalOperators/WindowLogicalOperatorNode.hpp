#ifndef WINDOW_LOGICAL_OPERATOR_NODE_HPP
#define WINDOW_LOGICAL_OPERATOR_NODE_HPP

#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>

namespace NES {

class WindowDefinition;
typedef std::shared_ptr<WindowDefinition> WindowDefinitionPtr;

class WindowLogicalOperatorNode : public Node {
  public:
    WindowLogicalOperatorNode(const WindowDefinitionPtr& windowDefinition);
    const std::string toString() const override;
    const WindowDefinitionPtr& getWindowDefinition() const;
  private:
    WindowDefinitionPtr windowDefinition;
};

} // namespace NES
#endif // WINDOW_LOGICAL_OPERATOR_NODE_HPP
