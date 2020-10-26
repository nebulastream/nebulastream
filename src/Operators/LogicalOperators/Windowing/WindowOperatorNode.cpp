
#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>
namespace NES{

WindowOperatorNode::WindowOperatorNode(const Windowing::LogicalWindowDefinitionPtr windowDefinition): windowDefinition(windowDefinition) {}

Windowing::LogicalWindowDefinitionPtr WindowOperatorNode::getWindowDefinition() const {
    return  windowDefinition;
}

}