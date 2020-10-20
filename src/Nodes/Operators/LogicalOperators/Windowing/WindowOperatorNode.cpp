
#include <Nodes/Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>
namespace NES{

WindowOperatorNode::WindowOperatorNode(const LogicalWindowDefinitionPtr windowDefinition): windowDefinition(windowDefinition) {}

const LogicalWindowDefinitionPtr & WindowOperatorNode::getWindowDefinition() const {
    return  windowDefinition;
}

}