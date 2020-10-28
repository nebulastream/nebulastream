#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>

namespace NES {

WindowOperatorNode::WindowOperatorNode(const Windowing::LogicalWindowDefinitionPtr windowDefinition, OperatorId id)
    : windowDefinition(windowDefinition), LogicalOperatorNode(id) {}

Windowing::LogicalWindowDefinitionPtr WindowOperatorNode::getWindowDefinition() const {
    return windowDefinition;
}

}// namespace NES