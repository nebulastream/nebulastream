#include <Nodes/Operators/SpecializedWindowOperators/SliceCreationOperator.hpp>

namespace NES {

LogicalOperatorNodePtr createSliceCreationSpecializedOperatorNode(const WindowDefinitionPtr windowDefinition) {
    return std::make_shared<SliceCreationOperator>(windowDefinition);
}

SliceCreationOperator::SliceCreationOperator(const WindowDefinitionPtr& windowDefinition)
    : WindowLogicalOperatorNode(windowDefinition) {
}

}// namespace NES
