/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/StreamHashJoinOperatorHandler.hpp>
#include <Operators/LogicalOperators/BatchJoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/CEP/IterationLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/FlatMapUDFLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/InferModelLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/InferModelOperatorHandler.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LimitLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapUDFLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/CentralWindowOperator.hpp>
#include <Operators/LogicalOperators/Windowing/SliceCreationOperator.hpp>
#include <Operators/LogicalOperators/Windowing/SliceMergingOperator.hpp>
#include <Operators/LogicalOperators/Windowing/WindowComputationOperator.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/Exceptions/QueryCompilationException.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/CEP/PhysicalCEPIterationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalBatchJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalBatchJoinProbeOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalHashJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalHashJoinProbeOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalNestedLoopJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalNestedLoopJoinProbeOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFlatMapUDFOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalInferModelOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalLimitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapUDFOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/ContentBasedWindow/PhysicalThresholdWindowOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedGlobalSliceStoreAppendOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedSlidingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedThreadLocalPreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedTumblingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/NonKeyedTimeWindow/PhysicalNonKeyedSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/NonKeyedTimeWindow/PhysicalNonKeyedSlidingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/NonKeyedTimeWindow/PhysicalNonKeyedThreadLocalPreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/NonKeyedTimeWindow/PhysicalNonKeyedTumblingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/NonKeyedTimeWindow/PhysicalNonKeyedWindowSliceStoreAppendOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowSinkOperator.hpp>
#include <QueryCompiler/Phases/Translations/DefaultPhysicalOperatorProvider.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Windowing/Experimental/GlobalSliceStore.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedGlobalSliceStoreAppendOperatorHandler.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedSliceMergingOperatorHandler.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedSlidingWindowSinkOperatorHandler.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedThreadLocalPreAggregationOperatorHandler.hpp>
#include <Windowing/Experimental/NonKeyedTimeWindow/NonKeyedGlobalSliceStoreAppendOperatorHandler.hpp>
#include <Windowing/Experimental/NonKeyedTimeWindow/NonKeyedSliceMergingOperatorHandler.hpp>
#include <Windowing/Experimental/NonKeyedTimeWindow/NonKeyedSlidingWindowSinkOperatorHandler.hpp>
#include <Windowing/Experimental/NonKeyedTimeWindow/NonKeyedThreadLocalPreAggregationOperatorHandler.hpp>
#include <Windowing/JoinForwardRefs.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowHandler/BatchJoinOperatorHandler.hpp>
#include <Windowing/WindowHandler/JoinOperatorHandler.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
#include <Windowing/WindowTypes/ContentBasedWindowType.hpp>
#include <Windowing/WindowTypes/ThresholdWindow.hpp>
#include <Windowing/WindowTypes/TimeBasedWindowType.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <utility>

namespace NES::QueryCompilation {

DefaultPhysicalOperatorProvider::DefaultPhysicalOperatorProvider(QueryCompilerOptionsPtr options)
    : PhysicalOperatorProvider(options){};

PhysicalOperatorProviderPtr DefaultPhysicalOperatorProvider::create(QueryCompilerOptionsPtr options) {
    return std::make_shared<DefaultPhysicalOperatorProvider>(options);
}

bool DefaultPhysicalOperatorProvider::isDemultiplex(const LogicalOperatorNodePtr& operatorNode) {
    return operatorNode->getParents().size() > 1;
}

void DefaultPhysicalOperatorProvider::insertDemultiplexOperatorsBefore(const LogicalOperatorNodePtr& operatorNode) {
    auto operatorOutputSchema = operatorNode->getOutputSchema();
    // A demultiplex operator has the same output schema as its child operator.
    auto demultiplexOperator = PhysicalOperators::PhysicalDemultiplexOperator::create(operatorOutputSchema);
    demultiplexOperator->setOutputSchema(operatorNode->getOutputSchema());
    operatorNode->insertBetweenThisAndParentNodes(demultiplexOperator);
}

void DefaultPhysicalOperatorProvider::insertMultiplexOperatorsAfter(const LogicalOperatorNodePtr& operatorNode) {
    // the multiplex operator has the same schema as the output schema of the operator node.
    auto multiplexOperator = PhysicalOperators::PhysicalMultiplexOperator::create(operatorNode->getOutputSchema());
    operatorNode->insertBetweenThisAndChildNodes(multiplexOperator);
}

void DefaultPhysicalOperatorProvider::lower(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode) {
    if (isDemultiplex(operatorNode)) {
        insertDemultiplexOperatorsBefore(operatorNode);
    }

    if (operatorNode->isUnaryOperator()) {
        lowerUnaryOperator(queryPlan, operatorNode);
    } else if (operatorNode->isBinaryOperator()) {
        lowerBinaryOperator(queryPlan, operatorNode);
    } else if (operatorNode->isExchangeOperator()) {
        // exchange operators just vanish for now, as we already created an demultiplex operator for all incoming edges.
        operatorNode->removeAndJoinParentAndChildren();
    }
}

void DefaultPhysicalOperatorProvider::lowerUnaryOperator(const QueryPlanPtr& queryPlan,
                                                         const LogicalOperatorNodePtr& operatorNode) {

    // If a unary operator has more than one parent, we introduce an implicit multiplex operator before.
    if (operatorNode->getChildren().size() > 1) {
        insertMultiplexOperatorsAfter(operatorNode);
    }

    if (operatorNode->getParents().size() > 1) {
        throw QueryCompilationException("A unary operator should only have at most one parent.");
    }

    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        auto logicalSourceOperator = operatorNode->as<SourceLogicalOperatorNode>();
        auto physicalSourceOperator =
            PhysicalOperators::PhysicalSourceOperator::create(Util::getNextOperatorId(),
                                                              logicalSourceOperator->getOriginId(),
                                                              logicalSourceOperator->getInputSchema(),
                                                              logicalSourceOperator->getOutputSchema(),
                                                              logicalSourceOperator->getSourceDescriptor());
        physicalSourceOperator->addProperty("LogicalOperatorId", operatorNode->getId());
        operatorNode->replace(physicalSourceOperator);
    } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        auto logicalSinkOperator = operatorNode->as<SinkLogicalOperatorNode>();
        auto physicalSinkOperator = PhysicalOperators::PhysicalSinkOperator::create(logicalSinkOperator->getInputSchema(),
                                                                                    logicalSinkOperator->getOutputSchema(),
                                                                                    logicalSinkOperator->getSinkDescriptor());
        physicalSinkOperator->addProperty("LogicalOperatorId", operatorNode->getId());
        operatorNode->replace(physicalSinkOperator);
        queryPlan->replaceRootOperator(logicalSinkOperator, physicalSinkOperator);
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        auto filterOperator = operatorNode->as<FilterLogicalOperatorNode>();
        auto physicalFilterOperator = PhysicalOperators::PhysicalFilterOperator::create(filterOperator->getInputSchema(),
                                                                                        filterOperator->getOutputSchema(),
                                                                                        filterOperator->getPredicate());
        physicalFilterOperator->addProperty("LogicalOperatorId", operatorNode->getId());
        operatorNode->replace(physicalFilterOperator);
    } else if (operatorNode->instanceOf<WindowOperatorNode>()) {
        lowerWindowOperator(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
        lowerWatermarkAssignmentOperator(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        lowerMapOperator(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<InferModel::InferModelLogicalOperatorNode>()) {
#ifdef TFDEF
        lowerInferModelOperator(queryPlan, operatorNode);
#else
        NES_THROW_RUNTIME_ERROR("TFDEF is not defined but InferModelLogicalOperatorNode is used!");
#endif// TFDEF
    } else if (operatorNode->instanceOf<ProjectionLogicalOperatorNode>()) {
        lowerProjectOperator(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<IterationLogicalOperatorNode>()) {
        lowerCEPIterationOperator(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<MapUDFLogicalOperatorNode>()) {
        lowerUDFMapOperator(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<FlatMapUDFLogicalOperatorNode>()) {
        lowerUDFFlatMapOperator(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<LimitLogicalOperatorNode>()) {
        auto limitOperator = operatorNode->as<LimitLogicalOperatorNode>();
        auto physicalLimitOperator = PhysicalOperators::PhysicalLimitOperator::create(limitOperator->getInputSchema(),
                                                                                      limitOperator->getOutputSchema(),
                                                                                      limitOperator->getLimit());
        operatorNode->replace(physicalLimitOperator);
    } else {
        throw QueryCompilationException("No conversion for operator " + operatorNode->toString() + " was provided.");
    }
}

void DefaultPhysicalOperatorProvider::lowerBinaryOperator(const QueryPlanPtr& queryPlan,
                                                          const LogicalOperatorNodePtr& operatorNode) {
    if (operatorNode->instanceOf<UnionLogicalOperatorNode>()) {
        lowerUnionOperator(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<JoinLogicalOperatorNode>()) {
        lowerJoinOperator(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<Experimental::BatchJoinLogicalOperatorNode>()) {
        lowerBatchJoinOperator(queryPlan, operatorNode);
    } else {
        throw QueryCompilationException("No conversion for operator " + operatorNode->toString() + " was provided.");
    }
}

void DefaultPhysicalOperatorProvider::lowerUnionOperator(const QueryPlanPtr&, const LogicalOperatorNodePtr& operatorNode) {

    auto unionOperator = operatorNode->as<UnionLogicalOperatorNode>();
    // this assumes that we applies the ProjectBeforeUnionRule and the input across all children is the same.
    if (!unionOperator->getLeftInputSchema()->equals(unionOperator->getRightInputSchema())) {
        throw QueryCompilationException("The children of a union operator should have the same schema. Left:"
                                        + unionOperator->getLeftInputSchema()->toString() + " but right "
                                        + unionOperator->getRightInputSchema()->toString());
    }
    auto physicalMultiplexOperator = PhysicalOperators::PhysicalMultiplexOperator::create(unionOperator->getLeftInputSchema());
    operatorNode->replace(physicalMultiplexOperator);
}

void DefaultPhysicalOperatorProvider::lowerProjectOperator(const QueryPlanPtr&, const LogicalOperatorNodePtr& operatorNode) {
    auto projectOperator = operatorNode->as<ProjectionLogicalOperatorNode>();
    auto physicalProjectOperator = PhysicalOperators::PhysicalProjectOperator::create(projectOperator->getInputSchema(),
                                                                                      projectOperator->getOutputSchema(),
                                                                                      projectOperator->getExpressions());

    physicalProjectOperator->addProperty("LogicalOperatorId", projectOperator->getId());
    operatorNode->replace(physicalProjectOperator);
}

#ifdef TFDEF
void DefaultPhysicalOperatorProvider::lowerInferModelOperator(QueryPlanPtr, LogicalOperatorNodePtr operatorNode) {
    auto inferModelOperator = operatorNode->as<InferModel::InferModelLogicalOperatorNode>();
    auto inferModelOperatorHandler = InferModel::InferModelOperatorHandler::create(inferModelOperator->getDeployedModelPath());
    auto physicalInferModelOperator = PhysicalOperators::PhysicalInferModelOperator::create(inferModelOperator->getInputSchema(),
                                                                                            inferModelOperator->getOutputSchema(),
                                                                                            inferModelOperator->getModel(),
                                                                                            inferModelOperator->getInputFields(),
                                                                                            inferModelOperator->getOutputFields(),
                                                                                            inferModelOperatorHandler);
    operatorNode->replace(physicalInferModelOperator);
}
#endif// TFDEF

void DefaultPhysicalOperatorProvider::lowerMapOperator(const QueryPlanPtr&, const LogicalOperatorNodePtr& operatorNode) {
    auto mapOperator = operatorNode->as<MapLogicalOperatorNode>();
    auto physicalMapOperator = PhysicalOperators::PhysicalMapOperator::create(mapOperator->getInputSchema(),
                                                                              mapOperator->getOutputSchema(),
                                                                              mapOperator->getMapExpression());
    physicalMapOperator->addProperty("LogicalOperatorId", operatorNode->getId());
    operatorNode->replace(physicalMapOperator);
}

void DefaultPhysicalOperatorProvider::lowerUDFMapOperator(const QueryPlanPtr&, const LogicalOperatorNodePtr& operatorNode) {
    auto mapUDFOperator = operatorNode->as<MapUDFLogicalOperatorNode>();
    auto physicalMapOperator = PhysicalOperators::PhysicalMapUDFOperator::create(mapUDFOperator->getInputSchema(),
                                                                                 mapUDFOperator->getOutputSchema(),
                                                                                 mapUDFOperator->getUDFDescriptor());
    physicalMapOperator->addProperty("LogicalOperatorId", operatorNode->getId());
    operatorNode->replace(physicalMapOperator);
}

void DefaultPhysicalOperatorProvider::lowerUDFFlatMapOperator(const QueryPlanPtr&, const LogicalOperatorNodePtr& operatorNode) {
    auto flatMapUDFOperator = operatorNode->as<FlatMapUDFLogicalOperatorNode>();
    auto physicalMapOperator = PhysicalOperators::PhysicalFlatMapUDFOperator::create(flatMapUDFOperator->getInputSchema(),
                                                                                     flatMapUDFOperator->getOutputSchema(),
                                                                                     flatMapUDFOperator->getUDFDescriptor());
    physicalMapOperator->addProperty("LogicalOperatorId", operatorNode->getId());
    operatorNode->replace(physicalMapOperator);
}

void DefaultPhysicalOperatorProvider::lowerCEPIterationOperator(const QueryPlanPtr, const LogicalOperatorNodePtr operatorNode) {
    auto iterationOperator = operatorNode->as<IterationLogicalOperatorNode>();
    auto physicalCEpIterationOperator =
        PhysicalOperators::PhysicalIterationCEPOperator::create(iterationOperator->getInputSchema(),
                                                                iterationOperator->getOutputSchema(),
                                                                iterationOperator->getMinIterations(),
                                                                iterationOperator->getMaxIterations());
    operatorNode->replace(physicalCEpIterationOperator);
}

OperatorNodePtr DefaultPhysicalOperatorProvider::getJoinBuildInputOperator(const JoinLogicalOperatorNodePtr& joinOperator,
                                                                           SchemaPtr outputSchema,
                                                                           std::vector<OperatorNodePtr> children) {
    if (children.empty()) {
        throw QueryCompilationException("There should be at least one child for the join operator " + joinOperator->toString());
    }

    if (children.size() > 1) {
        auto demultiplexOperator = PhysicalOperators::PhysicalMultiplexOperator::create(std::move(outputSchema));
        demultiplexOperator->setOutputSchema(joinOperator->getOutputSchema());
        demultiplexOperator->addParent(joinOperator);
        for (const auto& child : children) {
            child->removeParent(joinOperator);
            child->addParent(demultiplexOperator);
        }
        return demultiplexOperator;
    }
    return children[0];
}

void DefaultPhysicalOperatorProvider::lowerJoinOperator(const QueryPlanPtr&, const LogicalOperatorNodePtr& operatorNode) {
    if (options->getQueryCompiler() == QueryCompilerOptions::QueryCompiler::DEFAULT_QUERY_COMPILER) {
        lowerOldDefaultQueryCompilerJoin(operatorNode);
    } else if (options->getQueryCompiler() == QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER) {
        lowerNautilusJoin(operatorNode);
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

void DefaultPhysicalOperatorProvider::lowerNautilusJoin(const LogicalOperatorNodePtr& operatorNode) {
    using namespace Runtime::Execution::Operators;

    auto joinOperator = operatorNode->as<JoinLogicalOperatorNode>();
    const auto& joinDefinition = joinOperator->getJoinDefinition();
    const auto& joinFieldNameLeft = joinDefinition->getLeftJoinKey()->getFieldName();
    const auto& joinFieldNameRight = joinDefinition->getRightJoinKey()->getFieldName();

    const auto windowType = Windowing::WindowType::asTimeBasedWindowType(joinDefinition->getWindowType());
    const auto& windowSize = windowType->getSize().getTime();
    //FIXME Once #3353 is merged, sliding window can be added
    NES_ASSERT(windowType->getTimeBasedSubWindowType() == Windowing::TimeBasedWindowType::TUMBLINGWINDOW,
               "Only a tumbling window is currently supported for StreamJoin");

    const auto [timeStampFieldNameLeft, timeStampFieldNameRight] = getTimestampLeftAndRight(joinOperator, windowType);
    const auto leftInputOperator =
        getJoinBuildInputOperator(joinOperator, joinOperator->getLeftInputSchema(), joinOperator->getLeftOperators());
    const auto rightInputOperator =
        getJoinBuildInputOperator(joinOperator, joinOperator->getRightInputSchema(), joinOperator->getRightOperators());
    const auto joinStrategy = options->getStreamJoinStratgy();

    const StreamJoinOperatorNodes streamJoinOperatorNodes(operatorNode, leftInputOperator, rightInputOperator);
    const StreamJoinConfigs streamJoinConfig(joinFieldNameLeft,
                                             joinFieldNameRight,
                                             windowSize,
                                             timeStampFieldNameLeft,
                                             timeStampFieldNameRight,
                                             joinStrategy);

    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL
        || joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCKING
        || joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCK_FREE) {
        lowerStreamingHashJoin(streamJoinOperatorNodes, streamJoinConfig);
    } else if (joinStrategy == QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN) {
        lowerStreamingNestedLoopJoin(streamJoinOperatorNodes, streamJoinConfig);
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

void DefaultPhysicalOperatorProvider::lowerStreamingNestedLoopJoin(const StreamJoinOperatorNodes& streamJoinOperatorNodes,
                                                                   const StreamJoinConfigs& streamJoinConfig) {

    using namespace Runtime::Execution;
    const auto joinOperator = streamJoinOperatorNodes.operatorNode->as<JoinLogicalOperatorNode>();
    const auto joinOperatorHandler =
        Operators::NLJOperatorHandler::create(joinOperator->getAllInputOriginIds(),
                                              joinOperator->getOutputOriginIds()[0],
                                              joinOperator->getLeftInputSchema()->getSchemaSizeInBytes(),
                                              joinOperator->getRightInputSchema()->getSchemaSizeInBytes(),
                                              Nautilus::Interface::PagedVector::PAGE_SIZE,
                                              Nautilus::Interface::PagedVector::PAGE_SIZE,
                                              streamJoinConfig.windowSize);

    auto createNLJBuildOperator = [&](const SchemaPtr& inputSchema,
                                      JoinBuildSideType buildSideType,
                                      const std::string& timeStampField,
                                      const std::string& joinFieldName) {
        return PhysicalOperators::PhysicalNestedLoopJoinBuildOperator::create(inputSchema,
                                                                              joinOperator->getOutputSchema(),
                                                                              joinOperatorHandler,
                                                                              buildSideType,
                                                                              timeStampField,
                                                                              joinFieldName);
    };

    const auto leftJoinBuildOperator = createNLJBuildOperator(joinOperator->getLeftInputSchema(),
                                                              JoinBuildSideType::Left,
                                                              streamJoinConfig.timeStampFieldNameLeft,
                                                              streamJoinConfig.joinFieldNameLeft);
    const auto rightJoinBuildOperator = createNLJBuildOperator(joinOperator->getRightInputSchema(),
                                                               JoinBuildSideType::Right,
                                                               streamJoinConfig.timeStampFieldNameRight,
                                                               streamJoinConfig.joinFieldNameRight);
    const auto joinProbeOperator =
        PhysicalOperators::PhysicalNestedLoopJoinProbeOperator::create(joinOperator->getLeftInputSchema(),
                                                                       joinOperator->getRightInputSchema(),
                                                                       joinOperator->getOutputSchema(),
                                                                       streamJoinConfig.joinFieldNameLeft,
                                                                       streamJoinConfig.joinFieldNameRight,
                                                                       joinOperator->getWindowStartFieldName(),
                                                                       joinOperator->getWindowEndFieldName(),
                                                                       joinOperator->getWindowKeyFieldName(),
                                                                       joinOperatorHandler);

    streamJoinOperatorNodes.leftInputOperator->insertBetweenThisAndParentNodes(leftJoinBuildOperator);
    streamJoinOperatorNodes.rightInputOperator->insertBetweenThisAndParentNodes(rightJoinBuildOperator);
    streamJoinOperatorNodes.operatorNode->replace(joinProbeOperator);
}

void DefaultPhysicalOperatorProvider::lowerStreamingHashJoin(const StreamJoinOperatorNodes& streamJoinOperatorNodes,
                                                             const StreamJoinConfigs& streamJoinConfig) {
    using namespace Runtime::Execution;

    // TODO we should pass this not as an enum #3900
    StreamJoinStrategy runtimeJoinStrategy;
    if (streamJoinConfig.joinStrategy == StreamJoinStrategy::HASH_JOIN_LOCAL) {
        runtimeJoinStrategy = StreamJoinStrategy::HASH_JOIN_LOCAL;
    } else if (streamJoinConfig.joinStrategy == StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCKING) {
        runtimeJoinStrategy = StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCKING;
    } else {
        runtimeJoinStrategy = StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCK_FREE;
    }

    const auto logicalJoinOperatorNode = streamJoinOperatorNodes.operatorNode->as<JoinLogicalOperatorNode>();
    const auto joinOperatorHandler =
        Operators::StreamHashJoinOperatorHandler::create(logicalJoinOperatorNode->getAllInputOriginIds(),
                                                         logicalJoinOperatorNode->getOutputOriginIds()[0],
                                                         streamJoinConfig.windowSize,
                                                         logicalJoinOperatorNode->getLeftInputSchema()->getSchemaSizeInBytes(),
                                                         logicalJoinOperatorNode->getRightInputSchema()->getSchemaSizeInBytes(),
                                                         options->getHashJoinOptions()->getTotalSizeForDataStructures(),
                                                         options->getHashJoinOptions()->getPageSize(),
                                                         options->getHashJoinOptions()->getPreAllocPageCnt(),
                                                         options->getHashJoinOptions()->getNumberOfPartitions(),
                                                         runtimeJoinStrategy);

    auto createHashJoinBuildOperator = [&](const SchemaPtr& inputSchema,
                                           JoinBuildSideType buildSideType,
                                           const std::string& timeStampField,
                                           const std::string& joinFieldName) {
        return PhysicalOperators::PhysicalHashJoinBuildOperator::create(inputSchema,
                                                                        logicalJoinOperatorNode->getOutputSchema(),
                                                                        joinOperatorHandler,
                                                                        buildSideType,
                                                                        timeStampField,
                                                                        joinFieldName);
    };

    const auto leftJoinBuildOperator = createHashJoinBuildOperator(logicalJoinOperatorNode->getLeftInputSchema(),
                                                                   JoinBuildSideType::Left,
                                                                   streamJoinConfig.timeStampFieldNameLeft,
                                                                   streamJoinConfig.joinFieldNameLeft);
    const auto rightJoinBuildOperator = createHashJoinBuildOperator(logicalJoinOperatorNode->getRightInputSchema(),
                                                                    JoinBuildSideType::Right,
                                                                    streamJoinConfig.timeStampFieldNameRight,
                                                                    streamJoinConfig.joinFieldNameRight);

    const auto joinProbeOperator =
        PhysicalOperators::PhysicalHashJoinProbeOperator::create(logicalJoinOperatorNode->getLeftInputSchema(),
                                                                 logicalJoinOperatorNode->getRightInputSchema(),
                                                                 logicalJoinOperatorNode->getOutputSchema(),
                                                                 streamJoinConfig.joinFieldNameLeft,
                                                                 streamJoinConfig.joinFieldNameRight,
                                                                 joinOperatorHandler);
    streamJoinOperatorNodes.leftInputOperator->insertBetweenThisAndParentNodes(leftJoinBuildOperator);
    streamJoinOperatorNodes.rightInputOperator->insertBetweenThisAndParentNodes(rightJoinBuildOperator);
    streamJoinOperatorNodes.operatorNode->replace(joinProbeOperator);
}

std::tuple<std::string, std::string>
DefaultPhysicalOperatorProvider::getTimestampLeftAndRight(const std::shared_ptr<JoinLogicalOperatorNode>& joinOperator,
                                                          const Windowing::TimeBasedWindowTypePtr& windowType) const {
    if (windowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::IngestionTime) {
        NES_DEBUG("Skip eventime identification as we use ingestion time");
        return {"IngestionTime", "IngestionTime"};
    } else {

        // FIXME Once #3407 is done, we can change this to get the left and right fieldname
        auto timeStampFieldName = windowType->getTimeCharacteristic()->getField()->getName();
        auto timeStampFieldNameWithoutSourceName =
            timeStampFieldName.substr(timeStampFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR));

        // Lambda function for extracting the timestamp from a schema
        auto findTimeStampFieldName = [&](const SchemaPtr& schema) {
            for (const auto& field : schema->fields) {
                if (field->getName().find(timeStampFieldNameWithoutSourceName) != std::string::npos) {
                    return field->getName();
                }
            }
            return std::string();
        };

        // Extracting the left and right timestamp
        auto timeStampFieldNameLeft = findTimeStampFieldName(joinOperator->getLeftInputSchema());
        auto timeStampFieldNameRight = findTimeStampFieldName(joinOperator->getRightInputSchema());

        NES_ASSERT(!(timeStampFieldNameLeft.empty() || timeStampFieldNameRight.empty()),
                   "Could not find timestampfieldname " << timeStampFieldNameWithoutSourceName << " in both streams!");
        NES_DEBUG("timeStampFieldNameLeft:{}  timeStampFieldNameRight:{} ", timeStampFieldNameLeft, timeStampFieldNameRight);

        return {timeStampFieldNameLeft, timeStampFieldNameRight};
    }
}

void DefaultPhysicalOperatorProvider::lowerOldDefaultQueryCompilerJoin(const LogicalOperatorNodePtr& operatorNode) {
    // create join operator handler, to establish a common Runtime object for build and prob.
    auto joinOperator = operatorNode->as<JoinLogicalOperatorNode>();
    auto joinOperatorHandler =
        Join::JoinOperatorHandler::create(joinOperator->getJoinDefinition(), joinOperator->getOutputSchema());

    auto createJoinBuildOperator =
        [&](const SchemaPtr& inputSchema, const std::vector<OperatorNodePtr>& inputOperators, JoinBuildSideType buildSideType) {
            auto joinBuildOperator = PhysicalOperators::PhysicalJoinBuildOperator::create(inputSchema,
                                                                                          joinOperator->getOutputSchema(),
                                                                                          joinOperatorHandler,
                                                                                          buildSideType);
            auto inputOperator = getJoinBuildInputOperator(joinOperator, inputSchema, inputOperators);
            inputOperator->insertBetweenThisAndParentNodes(joinBuildOperator);
        };

    // Calling above lambda function for left and right side
    createJoinBuildOperator(joinOperator->getLeftInputSchema(), joinOperator->getLeftOperators(), JoinBuildSideType::Left);
    createJoinBuildOperator(joinOperator->getRightInputSchema(), joinOperator->getRightOperators(), JoinBuildSideType::Right);

    auto joinSink = PhysicalOperators::PhysicalJoinSinkOperator::create(joinOperator->getLeftInputSchema(),
                                                                        joinOperator->getRightInputSchema(),
                                                                        joinOperator->getOutputSchema(),
                                                                        joinOperatorHandler);
    operatorNode->replace(joinSink);
}

OperatorNodePtr DefaultPhysicalOperatorProvider::getBatchJoinChildInputOperator(
    const Experimental::BatchJoinLogicalOperatorNodePtr& batchJoinOperator,
    SchemaPtr outputSchema,
    std::vector<OperatorNodePtr> children) {
    NES_ASSERT(!children.empty(), "There should be children for operator " << batchJoinOperator->toString());
    if (children.size() > 1) {
        auto demultiplexOperator = PhysicalOperators::PhysicalMultiplexOperator::create(std::move(outputSchema));
        demultiplexOperator->setOutputSchema(batchJoinOperator->getOutputSchema());
        demultiplexOperator->addParent(batchJoinOperator);
        for (const auto& child : children) {
            child->removeParent(batchJoinOperator);
            child->addParent(demultiplexOperator);
        }
        return demultiplexOperator;
    }
    return children[0];
}

void DefaultPhysicalOperatorProvider::lowerBatchJoinOperator(const QueryPlanPtr&, const LogicalOperatorNodePtr& operatorNode) {
    //  logical operator plan:
    //    childsLeft  ___ bjLogical ___ parent
    //                  /
    //    childsRight _/

    auto batchJoinOperator = operatorNode->as<Experimental::BatchJoinLogicalOperatorNode>();
    // create batch join operator handler, to establish a common Runtime object for build and probw.
    auto batchJoinOperatorHandler =
        Join::Experimental::BatchJoinOperatorHandler::create(batchJoinOperator->getBatchJoinDefinition(),
                                                             batchJoinOperator->getOutputSchema());

    auto leftInputOperator = getBatchJoinChildInputOperator(batchJoinOperator,
                                                            batchJoinOperator->getLeftInputSchema(),
                                                            batchJoinOperator->getLeftOperators());
    auto batchJoinBuildOperator =
        PhysicalOperators::PhysicalBatchJoinBuildOperator::create(batchJoinOperator->getLeftInputSchema(),
                                                                  batchJoinOperator->getOutputSchema(),
                                                                  batchJoinOperatorHandler);
    leftInputOperator->insertBetweenThisAndParentNodes(batchJoinBuildOperator);

    //  now:
    //    childsLeft __ bjPhysicalBuild __ bjLogical ___ parent
    //                                   /
    //    childsRight __________________/

    auto rightInputOperator =//    called to demultiplex, if there are multiple right childs. this var is not used further.
        getBatchJoinChildInputOperator(batchJoinOperator,
                                       batchJoinOperator->getRightInputSchema(),
                                       batchJoinOperator->getRightOperators());
    auto batchJoinProbeOperator = PhysicalOperators::Experimental::PhysicalBatchJoinProbeOperator::create(
        batchJoinOperator->getRightInputSchema(),// right = probe side!
        batchJoinOperator->getOutputSchema(),
        batchJoinOperatorHandler);
    operatorNode->replace(batchJoinProbeOperator);

    //  final:
    //    childsLeft __ bjPhysicalBuild __ bjPhysicalProbe ___ parent
    //                                   /
    //    childsRight __________________/
}

void DefaultPhysicalOperatorProvider::lowerWatermarkAssignmentOperator(const QueryPlanPtr&,
                                                                       const LogicalOperatorNodePtr& operatorNode) {
    auto logicalWatermarkAssignment = operatorNode->as<WatermarkAssignerLogicalOperatorNode>();
    auto physicalWatermarkAssignment = PhysicalOperators::PhysicalWatermarkAssignmentOperator::create(
        logicalWatermarkAssignment->getInputSchema(),
        logicalWatermarkAssignment->getOutputSchema(),
        logicalWatermarkAssignment->getWatermarkStrategyDescriptor());
    physicalWatermarkAssignment->setOutputSchema(logicalWatermarkAssignment->getOutputSchema());
    operatorNode->replace(physicalWatermarkAssignment);
}

KeyedOperatorHandlers
DefaultPhysicalOperatorProvider::createKeyedOperatorHandlers(WindowOperatorProperties& windowOperatorProperties) {

    auto& windowOperator = windowOperatorProperties.windowOperator;
    auto& windowDefinition = windowOperatorProperties.windowDefinition;
    KeyedOperatorHandlers keyedOperatorHandlers;

    if (options->getQueryCompiler() == QueryCompilerOptions::QueryCompiler::DEFAULT_QUERY_COMPILER) {
        auto smOperatorHandler = std::make_shared<Windowing::Experimental::KeyedSliceMergingOperatorHandler>(windowDefinition);

        keyedOperatorHandlers.preAggregationWindowHandler =
            std::make_shared<Windowing::Experimental::KeyedThreadLocalPreAggregationOperatorHandler>(
                windowDefinition,
                windowOperator->getInputOriginIds(),
                smOperatorHandler->getSliceStagingPtr());

        keyedOperatorHandlers.sliceMergingOperatorHandler = smOperatorHandler;
    } else if (options->getQueryCompiler() == QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER) {
        keyedOperatorHandlers.sliceMergingOperatorHandler =
            std::make_shared<Runtime::Execution::Operators::KeyedSliceMergingHandler>();

        NES_ASSERT2_FMT(windowDefinition->getWindowType()->isTimeBasedWindowType(), "window type is not time based");
        auto timeBasedWindowType = Windowing::WindowType::asTimeBasedWindowType(windowDefinition->getWindowType());
        keyedOperatorHandlers.preAggregationWindowHandler =
            std::make_shared<Runtime::Execution::Operators::KeyedSlicePreAggregationHandler>(
                timeBasedWindowType->getSize().getTime(),
                timeBasedWindowType->getSlide().getTime(),
                windowOperator->getInputOriginIds());
    }

    return keyedOperatorHandlers;
}

GlobalOperatorHandlers
DefaultPhysicalOperatorProvider::createGlobalOperatorHandlers(WindowOperatorProperties& windowOperatorProperties) {

    auto& windowOperator = windowOperatorProperties.windowOperator;
    auto& windowDefinition = windowOperatorProperties.windowDefinition;
    GlobalOperatorHandlers globalOperatorHandlers;

    if (options->getQueryCompiler() == QueryCompilerOptions::QueryCompiler::DEFAULT_QUERY_COMPILER) {
        auto smOperatorHandler = std::make_shared<Windowing::Experimental::NonKeyedSliceMergingOperatorHandler>(windowDefinition);

        globalOperatorHandlers.preAggregationWindowHandler =
            std::make_shared<Windowing::Experimental::NonKeyedThreadLocalPreAggregationOperatorHandler>(
                windowDefinition,
                windowOperator->getInputOriginIds(),
                smOperatorHandler->getSliceStagingPtr());

        globalOperatorHandlers.sliceMergingOperatorHandler = smOperatorHandler;
    } else if (options->getQueryCompiler() == QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER) {

        auto timeBasedWindowType = Windowing::WindowType::asTimeBasedWindowType(windowDefinition->getWindowType());
        timeBasedWindowType->getTimeBasedSubWindowType();
        globalOperatorHandlers.sliceMergingOperatorHandler =
            std::make_shared<Runtime::Execution::Operators::NonKeyedSliceMergingHandler>();
        NES_ASSERT2_FMT(windowDefinition->getWindowType()->isTimeBasedWindowType(), "window type is not time based");

        globalOperatorHandlers.preAggregationWindowHandler =
            std::make_shared<Runtime::Execution::Operators::NonKeyedSlicePreAggregationHandler>(
                timeBasedWindowType->getSize().getTime(),
                timeBasedWindowType->getSlide().getTime(),
                windowOperator->getInputOriginIds());
    }

    return globalOperatorHandlers;
}

std::shared_ptr<Node>
DefaultPhysicalOperatorProvider::replaceOperatorNodeTimeBasedKeyedWindow(WindowOperatorProperties& windowOperatorProperties,
                                                                         const LogicalOperatorNodePtr& operatorNode) {

    auto& windowDefinition = windowOperatorProperties.windowDefinition;
    auto& windowInputSchema = windowOperatorProperties.windowInputSchema;
    auto& windowOutputSchema = windowOperatorProperties.windowOutputSchema;
    auto timeBasedWindowType = Windowing::WindowType::asTimeBasedWindowType(windowDefinition->getWindowType());
    auto windowType = timeBasedWindowType->getTimeBasedSubWindowType();

    if (windowType == Windowing::TimeBasedWindowType::TUMBLINGWINDOW) {
        // Handle tumbling window
        return PhysicalOperators::PhysicalKeyedTumblingWindowSink::create(windowInputSchema,
                                                                          windowOutputSchema,
                                                                          windowDefinition);
    } else {
        // Handle sliding window
        auto globalSliceStore =
            std::make_shared<Windowing::Experimental::GlobalSliceStore<Windowing::Experimental::KeyedSlice>>();
        auto globalSliceStoreAppendOperator =
            std::make_shared<Windowing::Experimental::KeyedGlobalSliceStoreAppendOperatorHandler>(windowDefinition,
                                                                                                  globalSliceStore);
        auto globalSliceStoreAppend =
            PhysicalOperators::PhysicalKeyedGlobalSliceStoreAppendOperator::create(windowInputSchema,
                                                                                   windowOutputSchema,
                                                                                   globalSliceStoreAppendOperator);
        auto slidingWindowSinkOperator =
            std::make_shared<Windowing::Experimental::KeyedSlidingWindowSinkOperatorHandler>(windowDefinition, globalSliceStore);

        operatorNode->insertBetweenThisAndChildNodes(globalSliceStoreAppend);
        return PhysicalOperators::PhysicalKeyedSlidingWindowSink::create(windowInputSchema,
                                                                         windowOutputSchema,
                                                                         slidingWindowSinkOperator,
                                                                         windowDefinition);
    }
}

std::shared_ptr<Node>
DefaultPhysicalOperatorProvider::replaceOperatorNodeTimeBasedNonKeyedWindow(WindowOperatorProperties& windowOperatorProperties,
                                                                            const LogicalOperatorNodePtr& operatorNode) {

    auto& windowDefinition = windowOperatorProperties.windowDefinition;
    auto& windowInputSchema = windowOperatorProperties.windowInputSchema;
    auto& windowOutputSchema = windowOperatorProperties.windowOutputSchema;
    auto timeBasedWindowType = Windowing::WindowType::asTimeBasedWindowType(windowDefinition->getWindowType());
    auto windowType = timeBasedWindowType->getTimeBasedSubWindowType();

    if (windowType == Windowing::TimeBasedWindowType::TUMBLINGWINDOW) {
        // Handle tumbling window
        return PhysicalOperators::PhysicalNonKeyedTumblingWindowSink::create(windowInputSchema,
                                                                             windowOutputSchema,
                                                                             windowDefinition);
    } else {
        // Handle sliding window
        auto globalSliceStore =
            std::make_shared<Windowing::Experimental::GlobalSliceStore<Windowing::Experimental::NonKeyedSlice>>();
        auto globalSliceStoreAppendOperator =
            std::make_shared<Windowing::Experimental::NonKeyedGlobalSliceStoreAppendOperatorHandler>(windowDefinition,
                                                                                                     globalSliceStore);
        auto globalSliceStoreAppend =
            PhysicalOperators::PhysicalNonKeyedWindowSliceStoreAppendOperator::create(windowInputSchema,
                                                                                      windowOutputSchema,
                                                                                      globalSliceStoreAppendOperator);
        auto slidingWindowSinkOperator =
            std::make_shared<Windowing::Experimental::NonKeyedSlidingWindowSinkOperatorHandler>(windowDefinition,
                                                                                                globalSliceStore);

        operatorNode->insertBetweenThisAndChildNodes(globalSliceStoreAppend);
        return PhysicalOperators::PhysicalNonKeyedSlidingWindowSink::create(windowInputSchema,
                                                                            windowOutputSchema,
                                                                            slidingWindowSinkOperator,
                                                                            windowDefinition);
    }
}

void DefaultPhysicalOperatorProvider::lowerThreadLocalWindowOperator(const QueryPlanPtr&,
                                                                     const LogicalOperatorNodePtr& operatorNode) {

    NES_DEBUG("Create Thread local window aggregation");
    auto windowOperator = operatorNode->as<WindowOperatorNode>();
    auto windowInputSchema = windowOperator->getInputSchema();
    auto windowOutputSchema = windowOperator->getOutputSchema();
    auto windowDefinition = windowOperator->getWindowDefinition();

    auto windowOperatorProperties =
        WindowOperatorProperties(windowOperator, windowInputSchema, windowOutputSchema, windowDefinition);

    if (windowOperator->getInputOriginIds().empty()) {
        throw QueryCompilationException("The number of input origin IDs for an window operator should not be zero.");
    }
    // TODO this currently just mimics the old usage of the set of input origins.
    windowDefinition->setNumberOfInputEdges(windowOperator->getInputOriginIds().size());

    if (windowDefinition->isKeyed()) {
        // Create operator handlers for keyed windows
        KeyedOperatorHandlers operatorHandlers = createKeyedOperatorHandlers(windowOperatorProperties);

        // Translates a central window operator to ThreadLocalPreAggregationOperator and SliceMergingOperator
        auto preAggregationOperator = PhysicalOperators::PhysicalKeyedThreadLocalPreAggregationOperator::create(
            windowInputSchema,
            windowOutputSchema,
            operatorHandlers.preAggregationWindowHandler,
            windowDefinition);
        operatorNode->insertBetweenThisAndChildNodes(preAggregationOperator);

        auto mergingOperator =
            PhysicalOperators::PhysicalKeyedSliceMergingOperator::create(windowInputSchema,
                                                                         windowOutputSchema,
                                                                         operatorHandlers.sliceMergingOperatorHandler,
                                                                         windowDefinition);
        operatorNode->insertBetweenThisAndChildNodes(mergingOperator);

        if (windowDefinition->getWindowType()->isTimeBasedWindowType()) {
            auto windowSink = replaceOperatorNodeTimeBasedKeyedWindow(windowOperatorProperties, operatorNode);
            operatorNode->replace(windowSink);
        }
    } else {
        // Create operator handlers for global windows
        GlobalOperatorHandlers operatorHandlers = createGlobalOperatorHandlers(windowOperatorProperties);

        // Translates a central window operator to ThreadLocalPreAggregationOperator and SliceMergingOperator
        auto preAggregationOperator = PhysicalOperators::PhysicalNonKeyedThreadLocalPreAggregationOperator::create(
            windowInputSchema,
            windowOutputSchema,
            operatorHandlers.preAggregationWindowHandler,
            windowDefinition);
        operatorNode->insertBetweenThisAndChildNodes(preAggregationOperator);

        auto mergingOperator =
            PhysicalOperators::PhysicalNonKeyedSliceMergingOperator::create(windowInputSchema,
                                                                            windowOutputSchema,
                                                                            operatorHandlers.sliceMergingOperatorHandler,
                                                                            windowDefinition);
        operatorNode->insertBetweenThisAndChildNodes(mergingOperator);

        if (windowDefinition->getWindowType()->isTimeBasedWindowType()) {
            auto windowSink = replaceOperatorNodeTimeBasedNonKeyedWindow(windowOperatorProperties, operatorNode);
            operatorNode->replace(windowSink);
        }
    }
}

void DefaultPhysicalOperatorProvider::lowerWindowOperator(const QueryPlanPtr& plan, const LogicalOperatorNodePtr& operatorNode) {
    auto windowOperator = operatorNode->as<WindowOperatorNode>();
    auto windowInputSchema = windowOperator->getInputSchema();
    auto windowOutputSchema = windowOperator->getOutputSchema();
    auto windowDefinition = windowOperator->getWindowDefinition();
    NES_DEBUG("DefaultPhysicalOperatorProvider::lowerWindowOperator: Plan before\n{}", plan->toString());
    if (windowOperator->getInputOriginIds().empty()) {
        throw QueryCompilationException("The number of input origin IDs for an window operator should not be zero.");
    }
    // TODO this currently just mimics the old usage of the set of input origins.
    windowDefinition->setNumberOfInputEdges(windowOperator->getInputOriginIds().size());

    // create window operator handler, to establish a common Runtime object for aggregation and trigger phase.
    auto windowOperatorHandler = Windowing::WindowOperatorHandler::create(windowDefinition, windowOutputSchema);
    if (operatorNode->instanceOf<CentralWindowOperator>() || operatorNode->instanceOf<WindowLogicalOperatorNode>()) {
        // handle if threshold window
        //TODO: At this point we are already a central window, we do not want the threshold window to become a Gentral Window in the first place
        if (operatorNode->as<WindowOperatorNode>()->getWindowDefinition()->getWindowType()->isContentBasedWindowType()) {
            auto contentBasedWindowType = Windowing::WindowType::asContentBasedWindowType(windowDefinition->getWindowType());
            // check different content-based window types
            if (contentBasedWindowType->getContentBasedSubWindowType()
                == Windowing::ContentBasedWindowType::ContentBasedSubWindowType::THRESHOLDWINDOW) {
                NES_INFO("Lower ThresholdWindow");
                auto thresholdWindowPhysicalOperator =
                    PhysicalOperators::PhysicalThresholdWindowOperator::create(windowInputSchema,
                                                                               windowOutputSchema,
                                                                               windowOperatorHandler);
                thresholdWindowPhysicalOperator->addProperty("LogicalOperatorId", operatorNode->getId());

                operatorNode->replace(thresholdWindowPhysicalOperator);
                return;
            } else {
                throw QueryCompilationException("No support for this window type."
                                                + windowDefinition->getWindowType()->toString());
            }
        }
        if (options->getWindowingStrategy() == QueryCompilerOptions::WindowingStrategy::THREAD_LOCAL) {
            lowerThreadLocalWindowOperator(plan, operatorNode);
        } else {
            // Translate a central window operator in -> SlicePreAggregationOperator -> WindowSinkOperator
            auto preAggregationOperator = PhysicalOperators::PhysicalSlicePreAggregationOperator::create(windowInputSchema,
                                                                                                         windowOutputSchema,
                                                                                                         windowOperatorHandler);
            operatorNode->insertBetweenThisAndChildNodes(preAggregationOperator);
            auto windowSink = PhysicalOperators::PhysicalWindowSinkOperator::create(windowInputSchema,
                                                                                    windowOutputSchema,
                                                                                    windowOperatorHandler);
            windowSink->addProperty("LogicalOperatorId", operatorNode->getId());

            operatorNode->replace(windowSink);
        }
    } else if (operatorNode->instanceOf<SliceCreationOperator>()) {
        // Translate a slice creation operator in -> SlicePreAggregationOperator -> SliceSinkOperator
        auto preAggregationOperator = PhysicalOperators::PhysicalSlicePreAggregationOperator::create(windowInputSchema,
                                                                                                     windowOutputSchema,
                                                                                                     windowOperatorHandler);
        operatorNode->insertBetweenThisAndChildNodes(preAggregationOperator);
        auto sliceSink =
            PhysicalOperators::PhysicalSliceSinkOperator::create(windowInputSchema, windowOutputSchema, windowOperatorHandler);
        sliceSink->addProperty("LogicalOperatorId", operatorNode->getId());
        operatorNode->replace(sliceSink);
    } else if (operatorNode->instanceOf<SliceMergingOperator>()) {
        // Translate a slice merging operator in -> SliceMergingOperator -> SliceSinkOperator
        auto physicalSliceMergingOperator =
            PhysicalOperators::PhysicalSliceMergingOperator::create(windowInputSchema, windowOutputSchema, windowOperatorHandler);
        physicalSliceMergingOperator->addProperty("LogicalOperatorId", operatorNode->getId());
        operatorNode->insertBetweenThisAndChildNodes(physicalSliceMergingOperator);

        auto sliceSink =
            PhysicalOperators::PhysicalSliceSinkOperator::create(windowInputSchema, windowOutputSchema, windowOperatorHandler);
        sliceSink->addProperty("LogicalOperatorId", operatorNode->getId());

        operatorNode->replace(sliceSink);
    } else if (operatorNode->instanceOf<WindowComputationOperator>()) {
        // Translate a window computation operator in -> PhysicalSliceMergingOperator -> PhysicalWindowSinkOperator
        auto physicalSliceMergingOperator =
            PhysicalOperators::PhysicalSliceMergingOperator::create(windowInputSchema, windowOutputSchema, windowOperatorHandler);
        physicalSliceMergingOperator->addProperty("LogicalOperatorId", operatorNode->getId());

        operatorNode->insertBetweenThisAndChildNodes(physicalSliceMergingOperator);
        auto sliceSink =
            PhysicalOperators::PhysicalWindowSinkOperator::create(windowInputSchema, windowOutputSchema, windowOperatorHandler);
        sliceSink->addProperty("LogicalOperatorId", operatorNode->getId());

        operatorNode->replace(sliceSink);
    } else {
        throw QueryCompilationException("No conversion for operator " + operatorNode->toString() + " was provided.");
    }
    NES_DEBUG("DefaultPhysicalOperatorProvider::lowerWindowOperator: Plan after\n{}", plan->toString());
}

}// namespace NES::QueryCompilation
