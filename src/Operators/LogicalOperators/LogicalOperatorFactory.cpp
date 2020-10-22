#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/BroadcastLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/WindowLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/SpecializedWindowOperators/CentralWindowOperator.hpp>
#include <Operators/LogicalOperators/SpecializedWindowOperators/SliceCreationOperator.hpp>
#include <Operators/LogicalOperators/SpecializedWindowOperators/SliceMergingOperator.hpp>
#include <Operators/LogicalOperators/SpecializedWindowOperators/WindowComputationOperator.hpp>

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

LogicalOperatorNodePtr LogicalOperatorFactory::createBroadcastOperator() {
    return std::make_shared<BroadcastLogicalOperatorNode>();
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
