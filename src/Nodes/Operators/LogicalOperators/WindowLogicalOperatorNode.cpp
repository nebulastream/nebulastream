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

bool WindowLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (this->isIdentical(rhs)) {
        return true;
    }
    if (rhs->instanceOf<WindowLogicalOperatorNode>()) {
        auto windowOperator = rhs->as<WindowLogicalOperatorNode>();
        // todo check if the window definition is the same
        return windowOperator->getId() == id;
    }
    return false;
}

OperatorNodePtr WindowLogicalOperatorNode::copy() {
    auto copy = std::make_shared<WindowLogicalOperatorNode>(windowDefinition);
    copy->setId(id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

LogicalOperatorNodePtr createWindowLogicalOperatorNode(const WindowDefinitionPtr windowDefinitionPtr) {
    return std::make_shared<WindowLogicalOperatorNode>(windowDefinitionPtr);
}

}// namespace NES
