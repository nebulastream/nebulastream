#ifndef WINDOW_LOGICAL_OPERATOR_NODE_HPP
#define WINDOW_LOGICAL_OPERATOR_NODE_HPP

#include <memory>
#include <OperatorNodes/BaseOperatorNode.hpp>

namespace NES {

class WindowLogicalOperatorNode : public BaseOperatorNode,
                                  public std::enable_shared_from_this<WindowLogicalOperatorNode> {
  public:

    WindowLogicalOperatorNode(const WindowDefinitionPtr& window_defintion);
    const std::string toString() const override;
    OperatorType getOperatorType() const override;
    const WindowDefinitionPtr& getWindowDefinition() const;

    virtual BaseOperatorNodePtr makeShared() override { return shared_from_this(); };
    virtual bool equals(const BaseOperatorNode& rhs) const override;
  private:
    WindowDefinitionPtr window_definition;
};

} // namespace NES
#endif // WINDOW_LOGICAL_OPERATOR_NODE_HPP
