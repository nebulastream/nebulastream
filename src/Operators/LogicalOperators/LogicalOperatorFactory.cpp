#include <Operators/LogicalOperators/BroadcastLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/CentralWindowOperator.hpp>
#include <Operators/LogicalOperators/Windowing/SliceCreationOperator.hpp>
#include <Operators/LogicalOperators/Windowing/SliceMergingOperator.hpp>
#include <Operators/LogicalOperators/Windowing/WindowComputationOperator.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>

namespace NES {

LogicalOperatorNodePtr LogicalOperatorFactory::createSourceOperator(const SourceDescriptorPtr sourceDescriptor, OperatorId id) {
    return std::make_shared<SourceLogicalOperatorNode>(sourceDescriptor, id);
}

LogicalOperatorNodePtr LogicalOperatorFactory::createSinkOperator(const SinkDescriptorPtr sinkDescriptor, OperatorId id) {
    return std::make_shared<SinkLogicalOperatorNode>(sinkDescriptor, id);
}

LogicalOperatorNodePtr LogicalOperatorFactory::createFilterOperator(const ExpressionNodePtr predicate, OperatorId id) {
    return std::make_shared<FilterLogicalOperatorNode>(predicate, id);
}

LogicalOperatorNodePtr LogicalOperatorFactory::createMapOperator(const FieldAssignmentExpressionNodePtr mapExpression, OperatorId id) {
    return std::make_shared<MapLogicalOperatorNode>(mapExpression, id);
}

LogicalOperatorNodePtr LogicalOperatorFactory::createMergeOperator(OperatorId id) {
    return std::make_shared<MergeLogicalOperatorNode>(id);
}

LogicalOperatorNodePtr LogicalOperatorFactory::createBroadcastOperator(OperatorId id) {
    return std::make_shared<BroadcastLogicalOperatorNode>(id);
}

LogicalOperatorNodePtr LogicalOperatorFactory::createWindowOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition, OperatorId id) {
    return std::make_shared<WindowLogicalOperatorNode>(windowDefinition, id);
}

LogicalOperatorNodePtr LogicalOperatorFactory::createCentralWindowSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition, OperatorId id) {
    return std::make_shared<CentralWindowOperator>(windowDefinition, id);
}

LogicalOperatorNodePtr LogicalOperatorFactory::createSliceCreationSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition, OperatorId id) {
    return std::make_shared<SliceCreationOperator>(windowDefinition, id);
}

LogicalOperatorNodePtr LogicalOperatorFactory::createWindowComputationSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition, OperatorId id) {
    return std::make_shared<WindowComputationOperator>(windowDefinition, id);
}

LogicalOperatorNodePtr LogicalOperatorFactory::createSliceMergingSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition, OperatorId id) {
    return std::make_shared<SliceMergingOperator>(windowDefinition, id);
}

LogicalOperatorNodePtr LogicalOperatorFactory::createWatermarkAssignerOperator(const Windowing::WatermarkStrategyPtr watermarkStrategy, OperatorId id) {
    return std::make_shared<WatermarkAssignerLogicalOperatorNode>(watermarkStrategy, id);
}

}// namespace NES
