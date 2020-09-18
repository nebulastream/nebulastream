#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/WindowLogicalOperatorNode.hpp>
#include <Nodes/Operators/SpecializedWindowOperators/CentralWindowOperator.hpp>
#include <Nodes/Operators/SpecializedWindowOperators/SliceCreationOperator.hpp>
#include <Nodes/Operators/SpecializedWindowOperators/SliceMergingOperator.hpp>
#include <Nodes/Operators/SpecializedWindowOperators/WindowComputationOperator.hpp>

namespace NES {

LogicalOperatorNodePtr LogicalOperatorFactory::createSourceOperator(const SourceDescriptorPtr sourceDescriptor) {
    return std::make_shared<SourceLogicalOperatorNode>(sourceDescriptor);
}
LogicalOperatorNodePtr LogicalOperatorFactory::createSinkOperator(const SinkDescriptorPtr sinkDescriptor) {
    return std::make_shared<SinkLogicalOperatorNode>(sinkDescriptor);
}
LogicalOperatorNodePtr LogicalOperatorFactory::createFilterOperator(const ExpressionNodePtr predicate) {
    return std::make_shared<FilterLogicalOperatorNode>(predicate);
}
LogicalOperatorNodePtr LogicalOperatorFactory::createMapOperator(const FieldAssignmentExpressionNodePtr mapExpression) {
    return std::make_shared<MapLogicalOperatorNode>(mapExpression);
}

LogicalOperatorNodePtr LogicalOperatorFactory::createMergeOperator() {
    return std::make_shared<MergeLogicalOperatorNode>();
}

LogicalOperatorNodePtr LogicalOperatorFactory::createWindowOperator(const WindowDefinitionPtr windowDefinition) {
    return std::make_shared<WindowLogicalOperatorNode>(windowDefinition);
}
LogicalOperatorNodePtr LogicalOperatorFactory::createCentralWindowSpecializedOperator(const WindowDefinitionPtr windowDefinition) {
    return std::make_shared<CentralWindowOperator>(windowDefinition);
}
LogicalOperatorNodePtr LogicalOperatorFactory::createSliceCreationSpecializedOperator(const WindowDefinitionPtr windowDefinition) {
    return std::make_shared<SliceCreationOperator>(windowDefinition);
}
LogicalOperatorNodePtr LogicalOperatorFactory::createWindowComputationSpecializedOperator(const WindowDefinitionPtr windowDefinition) {
    return std::make_shared<WindowComputationOperator>(windowDefinition);
}

LogicalOperatorNodePtr LogicalOperatorFactory::createSliceMergingSpecializedOperator(const WindowDefinitionPtr windowDefinition) {
    return std::make_shared<SliceMergingOperator>(windowDefinition);
}

}// namespace NES
