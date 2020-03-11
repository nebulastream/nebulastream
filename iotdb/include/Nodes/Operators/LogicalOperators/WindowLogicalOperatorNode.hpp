#ifndef WINDOW_LOGICAL_OPERATOR_NODE_HPP
#define WINDOW_LOGICAL_OPERATOR_NODE_HPP

#include <memory>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <API/AbstractWindowDefinition.hpp>

namespace NES {

class WindowLogicalOperatorNode : public Node {
  public:

    WindowLogicalOperatorNode(const WindowDefinitionPtr& window_defintion);
    const std::string toString() const override;
    const WindowDefinitionPtr& getWindowDefinition() const;
  private:
    WindowDefinitionPtr window_definition;
};

} // namespace NES
#endif // WINDOW_LOGICAL_OPERATOR_NODE_HPP
