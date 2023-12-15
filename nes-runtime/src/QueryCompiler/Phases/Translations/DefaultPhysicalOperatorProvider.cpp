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
#include <Execution/Operators/Streaming/Aggregations/Buckets/KeyedBucketPreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/Bucketing/HJOperatorHandlerBucketing.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/Slicing/HJOperatorHandlerSlicing.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Bucketing/NLJOperatorHandlerBucketing.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJOperatorHandlerSlicing.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/InferModelLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LimitLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UDFs/FlatMapUDF/FlatMapUDFLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UDFs/MapUDF/MapUDFLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/CentralWindowOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDefinition.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDefinition.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/Types/ContentBasedWindowType.hpp>
#include <Operators/LogicalOperators/Windows/Types/ThresholdWindow.hpp>
#include <Operators/LogicalOperators/Windows/Types/TimeBasedWindowType.hpp>
#include <Operators/LogicalOperators/Windows/Types/TumblingWindow.hpp>
#include <Operators/LogicalOperators/Windows/Types/WindowType.hpp>
#include <Operators/LogicalOperators/Windows/WindowLogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/Exceptions/QueryCompilationException.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalStreamJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalStreamJoinProbeOperator.hpp>
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
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowSinkOperator.hpp>
#include <QueryCompiler/Phases/Translations/DefaultPhysicalOperatorProvider.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::QueryCompilation {

DefaultPhysicalOperatorProvider::DefaultPhysicalOperatorProvider(QueryCompilerOptionsPtr options)
    : PhysicalOperatorProvider(std::move(options)){};

PhysicalOperatorProviderPtr DefaultPhysicalOperatorProvider::create(const QueryCompilerOptionsPtr& options) {
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

    if (operatorNode->instanceOf<UnaryOperatorNode>()) {
        lowerUnaryOperator(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<BinaryOperatorNode>()) {
        lowerBinaryOperator(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<ExchangeOperatorNode>()) {
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
            PhysicalOperators::PhysicalSourceOperator::create(getNextOperatorId(),
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
        lowerInferModelOperator(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<ProjectionLogicalOperatorNode>()) {
        lowerProjectOperator(queryPlan, operatorNode);
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

void DefaultPhysicalOperatorProvider::lowerInferModelOperator(QueryPlanPtr, LogicalOperatorNodePtr operatorNode) {
    auto inferModelOperator = operatorNode->as<InferModel::InferModelLogicalOperatorNode>();
    auto physicalInferModelOperator =
        PhysicalOperators::PhysicalInferModelOperator::create(inferModelOperator->getInputSchema(),
                                                              inferModelOperator->getOutputSchema(),
                                                              inferModelOperator->getModel(),
                                                              inferModelOperator->getInputFields(),
                                                              inferModelOperator->getOutputFields());
    operatorNode->replace(physicalInferModelOperator);
}

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
    lowerNautilusJoin(operatorNode);
}

void DefaultPhysicalOperatorProvider::lowerNautilusJoin(const LogicalOperatorNodePtr& operatorNode) {
    using namespace Runtime::Execution::Operators;

    auto joinOperator = operatorNode->as<JoinLogicalOperatorNode>();
    const auto& joinDefinition = joinOperator->getJoinDefinition();
    const auto& joinFieldNameLeft = joinDefinition->getLeftJoinKey()->getFieldName();
    const auto& joinFieldNameRight = joinDefinition->getRightJoinKey()->getFieldName();

    const auto windowType = Windowing::WindowType::asTimeBasedWindowType(joinDefinition->getWindowType());
    const auto& windowSize = windowType->getSize().getTime();
    const auto& windowSlide = windowType->getSlide().getTime();
    NES_ASSERT(windowType->getTimeBasedSubWindowType() == Windowing::TimeBasedWindowType::TUMBLINGWINDOW
                   || windowType->getTimeBasedSubWindowType() == Windowing::TimeBasedWindowType::SLIDINGWINDOW,
               "Only a tumbling or sliding window is currently supported for StreamJoin");

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
                                             windowSlide,
                                             timeStampFieldNameLeft,
                                             timeStampFieldNameRight,
                                             joinStrategy);

    StreamJoinOperatorHandlerPtr joinOperatorHandler;
    switch (joinStrategy) {
        case StreamJoinStrategy::HASH_JOIN_LOCAL:
        case StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCKING:
        case StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCK_FREE:
            joinOperatorHandler = lowerStreamingHashJoin(streamJoinOperatorNodes, streamJoinConfig);
            break;
        case StreamJoinStrategy::NESTED_LOOP_JOIN:
            joinOperatorHandler = lowerStreamingNestedLoopJoin(streamJoinOperatorNodes, streamJoinConfig);
            break;
    }

    auto createBuildOperator = [&](const SchemaPtr& inputSchema,
                                   JoinBuildSideType buildSideType,
                                   const std::string& timeStampField,
                                   const std::string& joinFieldName) {
        return PhysicalOperators::PhysicalStreamJoinBuildOperator::create(inputSchema,
                                                                          joinOperator->getOutputSchema(),
                                                                          joinOperatorHandler,
                                                                          buildSideType,
                                                                          timeStampField,
                                                                          joinFieldName,
                                                                          options->getStreamJoinStratgy(),
                                                                          options->getWindowingStrategy());
    };

    const auto leftJoinBuildOperator = createBuildOperator(joinOperator->getLeftInputSchema(),
                                                           JoinBuildSideType::Left,
                                                           streamJoinConfig.timeStampFieldNameLeft,
                                                           streamJoinConfig.joinFieldNameLeft);
    const auto rightJoinBuildOperator = createBuildOperator(joinOperator->getRightInputSchema(),
                                                            JoinBuildSideType::Right,
                                                            streamJoinConfig.timeStampFieldNameRight,
                                                            streamJoinConfig.joinFieldNameRight);
    const auto joinProbeOperator =
        PhysicalOperators::PhysicalStreamJoinProbeOperator::create(joinOperator->getLeftInputSchema(),
                                                                   joinOperator->getRightInputSchema(),
                                                                   joinOperator->getOutputSchema(),
                                                                   streamJoinConfig.joinFieldNameLeft,
                                                                   streamJoinConfig.joinFieldNameRight,
                                                                   joinOperator->getWindowStartFieldName(),
                                                                   joinOperator->getWindowEndFieldName(),
                                                                   joinOperator->getWindowKeyFieldName(),
                                                                   joinOperatorHandler,
                                                                   options->getStreamJoinStratgy(),
                                                                   options->getWindowingStrategy());

    streamJoinOperatorNodes.leftInputOperator->insertBetweenThisAndParentNodes(leftJoinBuildOperator);
    streamJoinOperatorNodes.rightInputOperator->insertBetweenThisAndParentNodes(rightJoinBuildOperator);
    streamJoinOperatorNodes.operatorNode->replace(joinProbeOperator);
}

Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr
DefaultPhysicalOperatorProvider::lowerStreamingNestedLoopJoin(const StreamJoinOperatorNodes& streamJoinOperatorNodes,
                                                              const StreamJoinConfigs& streamJoinConfig) {

    using namespace Runtime::Execution;
    const auto joinOperator = streamJoinOperatorNodes.operatorNode->as<JoinLogicalOperatorNode>();

    Operators::NLJOperatorHandlerPtr joinOperatorHandler;
    if (options->getWindowingStrategy() == WindowingStrategy::SLICING) {
        return Operators::NLJOperatorHandlerSlicing::create(joinOperator->getAllInputOriginIds(),
                                                            joinOperator->getOutputOriginIds()[0],
                                                            streamJoinConfig.windowSize,
                                                            streamJoinConfig.windowSlide,
                                                            joinOperator->getLeftInputSchema()->getSchemaSizeInBytes(),
                                                            joinOperator->getRightInputSchema()->getSchemaSizeInBytes(),
                                                            Nautilus::Interface::PagedVector::PAGE_SIZE,
                                                            Nautilus::Interface::PagedVector::PAGE_SIZE);
    } else if (options->getWindowingStrategy() == WindowingStrategy::BUCKETING) {
        return Operators::NLJOperatorHandlerBucketing::create(joinOperator->getAllInputOriginIds(),
                                                              joinOperator->getOutputOriginIds()[0],
                                                              streamJoinConfig.windowSize,
                                                              streamJoinConfig.windowSlide,
                                                              joinOperator->getLeftInputSchema()->getSchemaSizeInBytes(),
                                                              joinOperator->getRightInputSchema()->getSchemaSizeInBytes(),
                                                              Nautilus::Interface::PagedVector::PAGE_SIZE,
                                                              Nautilus::Interface::PagedVector::PAGE_SIZE);
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr
DefaultPhysicalOperatorProvider::lowerStreamingHashJoin(const StreamJoinOperatorNodes& streamJoinOperatorNodes,
                                                        const StreamJoinConfigs& streamJoinConfig) {
    using namespace Runtime::Execution;
    const auto joinOperator = streamJoinOperatorNodes.operatorNode->as<JoinLogicalOperatorNode>();
    Operators::HJOperatorHandlerPtr joinOperatorHandler;
    if (options->getWindowingStrategy() == WindowingStrategy::SLICING) {
        return Operators::HJOperatorHandlerSlicing::create(joinOperator->getAllInputOriginIds(),
                                                           joinOperator->getOutputOriginIds()[0],
                                                           streamJoinConfig.windowSize,
                                                           streamJoinConfig.windowSlide,
                                                           joinOperator->getLeftInputSchema()->getSchemaSizeInBytes(),
                                                           joinOperator->getRightInputSchema()->getSchemaSizeInBytes(),
                                                           streamJoinConfig.joinStrategy,
                                                           options->getHashJoinOptions()->getTotalSizeForDataStructures(),
                                                           options->getHashJoinOptions()->getPreAllocPageCnt(),
                                                           options->getHashJoinOptions()->getPageSize(),
                                                           options->getHashJoinOptions()->getNumberOfPartitions());
    } else if (options->getWindowingStrategy() == WindowingStrategy::BUCKETING) {
        return Operators::HJOperatorHandlerBucketing::create(joinOperator->getAllInputOriginIds(),
                                                             joinOperator->getOutputOriginIds()[0],
                                                             streamJoinConfig.windowSize,
                                                             streamJoinConfig.windowSlide,
                                                             joinOperator->getLeftInputSchema()->getSchemaSizeInBytes(),
                                                             joinOperator->getRightInputSchema()->getSchemaSizeInBytes(),
                                                             streamJoinConfig.joinStrategy,
                                                             options->getHashJoinOptions()->getTotalSizeForDataStructures(),
                                                             options->getHashJoinOptions()->getPreAllocPageCnt(),
                                                             options->getHashJoinOptions()->getPageSize(),
                                                             options->getHashJoinOptions()->getNumberOfPartitions());
    } else {
        NES_NOT_IMPLEMENTED();
    }
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

void DefaultPhysicalOperatorProvider::lowerTimeBasedWindowOperator(const QueryPlanPtr&,
                                                                   const LogicalOperatorNodePtr& operatorNode) {

    NES_DEBUG("Create Thread local window aggregation");
    auto windowOperator = operatorNode->as<WindowOperatorNode>();
    auto windowInputSchema = windowOperator->getInputSchema();
    auto windowOutputSchema = windowOperator->getOutputSchema();
    auto windowDefinition = windowOperator->getWindowDefinition();

    auto timeBasedWindowType = Windowing::WindowType::asTimeBasedWindowType(windowDefinition->getWindowType());
    auto windowType = timeBasedWindowType->getTimeBasedSubWindowType();

    auto windowOperatorProperties =
        WindowOperatorProperties(windowOperator, windowInputSchema, windowOutputSchema, windowDefinition);

    if (windowOperator->getInputOriginIds().empty()) {
        throw QueryCompilationException("The number of input origin IDs for an window operator should not be zero.");
    }

    // TODO this currently just mimics the old usage of the set of input origins.
    windowDefinition->setNumberOfInputEdges(windowOperator->getInputOriginIds().size());
    windowDefinition->setInputOriginIds(windowOperator->getInputOriginIds());

    auto preAggregationOperator = PhysicalOperators::PhysicalSlicePreAggregationOperator::create(getNextOperatorId(),
                                                                                                 windowInputSchema,
                                                                                                 windowOutputSchema,
                                                                                                 windowDefinition);
    operatorNode->insertBetweenThisAndChildNodes(preAggregationOperator);

    // if we have a sliding window and use slicing we have to create another slice merge operator
    if (windowType == Windowing::TimeBasedWindowType::SLIDINGWINDOW
        && options->getWindowingStrategy() == WindowingStrategy::SLICING) {
        auto mergingOperator = PhysicalOperators::PhysicalSliceMergingOperator::create(getNextOperatorId(),
                                                                                       windowInputSchema,
                                                                                       windowOutputSchema,
                                                                                       windowDefinition);
        operatorNode->insertBetweenThisAndChildNodes(mergingOperator);
    }
    auto windowSink = PhysicalOperators::PhysicalWindowSinkOperator::create(getNextOperatorId(),
                                                                            windowInputSchema,
                                                                            windowOutputSchema,
                                                                            windowDefinition);
    operatorNode->replace(windowSink);
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
    if (operatorNode->instanceOf<WindowLogicalOperatorNode>() || operatorNode->instanceOf<CentralWindowOperator>()) {
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
                                                                               windowDefinition);
                thresholdWindowPhysicalOperator->addProperty("LogicalOperatorId", operatorNode->getId());

                operatorNode->replace(thresholdWindowPhysicalOperator);
                return;
            } else {
                throw QueryCompilationException("No support for this window type."
                                                + windowDefinition->getWindowType()->toString());
            }
        } else if (operatorNode->as<WindowOperatorNode>()->getWindowDefinition()->getWindowType()->isTimeBasedWindowType()) {
            lowerTimeBasedWindowOperator(plan, operatorNode);
        }
    } else {
        throw QueryCompilationException("No conversion for operator " + operatorNode->toString() + " was provided.");
    }
    NES_DEBUG("DefaultPhysicalOperatorProvider::lowerWindowOperator: Plan after\n{}", plan->toString());
}

}// namespace NES::QueryCompilation
