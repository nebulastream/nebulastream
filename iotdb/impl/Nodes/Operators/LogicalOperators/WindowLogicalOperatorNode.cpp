#include <Nodes/Operators/LogicalOperators/WindowLogicalOperatorNode.hpp>
#include <sstream>
#include <Operators/Operator.hpp>

namespace NES {

WindowLogicalOperatorNode::WindowLogicalOperatorNode(const WindowDefinitionPtr& window_definition)
    : window_definition(window_definition) {
}

const std::string WindowLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "WINDOW()";
    return ss.str();
}

const WindowDefinitionPtr& WindowLogicalOperatorNode::getWindowDefinition() const {
    return window_definition;
}

/*
 * bool WindowLogicalOperatorNode::equal(const Node& rhs) const {
    try {
        auto& rhs_ = dynamic_cast<const WindowLogicalOperatorNode&>(rhs);
        return true;
    } catch (const std::bad_cast& e) {
        return false;
    }
}
 */

const NodePtr createWindowLogicalOperatorNode(const WindowDefinitionPtr& windowDefinitionPtr) {
    return std::make_shared<WindowLogicalOperatorNode>(windowDefinitionPtr);
}

} // namespace NES
