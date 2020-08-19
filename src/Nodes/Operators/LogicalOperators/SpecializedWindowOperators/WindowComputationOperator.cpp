#include <Nodes/Operators/SpecializedWindowOperators/WindowComputationOperator.hpp>

namespace NES {

LogicalOperatorNodePtr createWindowComputationSpecializedOperatorNode(const WindowDefinitionPtr windowDefinition) {
    return std::make_shared<WindowComputationOperator>(windowDefinition);
}

WindowComputationOperator::WindowComputationOperator(const WindowDefinitionPtr& windowDefinition)
    : WindowLogicalOperatorNode(windowDefinition) {
}

}// namespace NES