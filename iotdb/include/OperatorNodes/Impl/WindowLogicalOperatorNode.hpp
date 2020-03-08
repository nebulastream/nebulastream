#ifndef WINDOW_LOGICAL_OPERATOR_NODE_HPP
#define WINDOW_LOGICAL_OPERATOR_NODE_HPP

#include <memory>
#include <OperatorNodes/Node.hpp>

namespace NES {

class WindowLogicalOperatorNode : public Node,
                                  public std::enable_shared_from_this<WindowLogicalOperatorNode> {
  public:

    WindowLogicalOperatorNode(const WindowDefinitionPtr& window_defintion);
    const std::string toString() const override;
    OperatorType getOperatorType() const override;
    const WindowDefinitionPtr& getWindowDefinition() const;

    virtual NodePtr makeShared() override { return shared_from_this(); };
    virtual bool equals(const Node& rhs) const override;
  private:
    WindowDefinitionPtr window_definition;
};

} // namespace NES
#endif // WINDOW_LOGICAL_OPERATOR_NODE_HPP
