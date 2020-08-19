#include <Nodes/Operators/SpecializedWindowOperators/SliceMergingOperator.hpp>

namespace NES {

LogicalOperatorNodePtr createSliceMergingSpecializedOperatorNode(const WindowDefinitionPtr windowDefinition) {
    return std::make_shared<SliceMergingOperator>(windowDefinition);
}

SliceMergingOperator::SliceMergingOperator(const WindowDefinitionPtr& windowDefinition)
    : WindowLogicalOperatorNode(windowDefinition) {
}

}// namespace NES
