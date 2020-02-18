#include <OperatorNodes/Impl/WindowLogicalOperatorNode.hpp>

namespace NES {

WindowLogicalOperatorNode::WindowLogicalOperatorNode(const WindowDefinitionPtr& window_definition)
    : window_definition(window_definition) {
}

const std::string WindowLogicalOperatorNode::toString() const {
  std::stringstream ss;
  ss << "WINDOW()";
  return ss.str();
}

OperatorType WindowLogicalOperatorNode::getOperatorType() const {
    return OperatorType::WINDOW_OP;
}

const WindowDefinitionPtr& WindowLogicalOperatorNode::getWindowDefinition() const {
    return window_definition;
}

const BaseOperatorNodePtr createWindowLogicalOperatorNode(const WindowDefinitionPtr& windowDefinitionPtr) {
  return std::make_shared<WindowLogicalOperatorNode>(windowDefinitionPtr);
}

} // namespace NES
