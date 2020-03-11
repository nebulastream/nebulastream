#ifndef WINDOW_LOGICAL_OPERATOR_NODE_HPP
#define WINDOW_LOGICAL_OPERATOR_NODE_HPP

#include <memory>
#include <OperatorNodes/Node.hpp>
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
