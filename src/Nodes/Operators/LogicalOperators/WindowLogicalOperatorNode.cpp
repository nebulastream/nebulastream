#include <API/Schema.hpp>
#include <Nodes/Operators/LogicalOperators/WindowLogicalOperatorNode.hpp>
#include <sstream>

namespace NES {

WindowLogicalOperatorNode::WindowLogicalOperatorNode(const WindowDefinitionPtr& windowDefinition)
    : windowDefinition(windowDefinition) {
}

const std::string WindowLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "WINDOW(" << outputSchema->toString() << ")";
    return ss.str();
}

const WindowDefinitionPtr& WindowLogicalOperatorNode::getWindowDefinition() const {
    return windowDefinition;
}

bool WindowLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<WindowLogicalOperatorNode>()->getId() == id;
}

bool WindowLogicalOperatorNode::equal(const NodePtr rhs) const {
    return rhs->instanceOf<WindowLogicalOperatorNode>();
}

OperatorNodePtr WindowLogicalOperatorNode::copy() {
    auto copy = createWindowLogicalOperatorNode(windowDefinition);
    copy->setId(id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

LogicalOperatorNodePtr createWindowLogicalOperatorNode(const WindowDefinitionPtr windowDefinitionPtr) {
    return std::make_shared<WindowLogicalOperatorNode>(windowDefinitionPtr);
}

}// namespace NES
