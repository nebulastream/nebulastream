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
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Common.hpp>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSliceStaging.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceStaging.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/StreamHashJoinOperatorHandler.hpp>
#include <Operators/LogicalOperators/BatchJoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/CEP/IterationLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/FlatMapJavaUDFLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/InferModelLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/InferModelOperatorHandler.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapJavaUDFLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
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
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalHashJoinSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalNestedLoopJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalNestedLoopJoinSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFlatMapJavaUDFOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalInferModelOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapJavaUDFOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/ContentBasedWindow/PhysicalThresholdWindowOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/GlobalTimeWindow/PhysicalGlobalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/GlobalTimeWindow/PhysicalGlobalSlidingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/GlobalTimeWindow/PhysicalGlobalThreadLocalPreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/GlobalTimeWindow/PhysicalGlobalTumblingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/GlobalTimeWindow/PhysicalGlobalWindowSliceStoreAppendOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedGlobalSliceStoreAppendOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedSlidingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedThreadLocalPreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedTumblingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowSinkOperator.hpp>
#include <QueryCompiler/Phases/Translations/DefaultPhysicalOperatorProvider.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Windowing/Experimental/GlobalSliceStore.hpp>
#include <Windowing/Experimental/GlobalTimeWindow/GlobalSliceMergingOperatorHandler.hpp>
#include <Windowing/Experimental/GlobalTimeWindow/GlobalSlidingWindowSinkOperatorHandler.hpp>
#include <Windowing/Experimental/GlobalTimeWindow/GlobalThreadLocalPreAggregationOperatorHandler.hpp>
#include <Windowing/Experimental/GlobalTimeWindow/GlobalWindowGlobalSliceStoreAppendOperatorHandler.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedGlobalSliceStoreAppendOperatorHandler.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedSliceMergingOperatorHandler.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedSlidingWindowSinkOperatorHandler.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedThreadLocalPreAggregationOperatorHandler.hpp>
#include <Windowing/JoinForwardRefs.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
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

    // If a unary operator has more then one parent, we introduce a implicit multiplex operator before.
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
        operatorNode->replace(physicalSourceOperator);
    } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        auto logicalSinkOperator = operatorNode->as<SinkLogicalOperatorNode>();
        auto physicalSinkOperator = PhysicalOperators::PhysicalSinkOperator::create(logicalSinkOperator->getInputSchema(),
                                                                                    logicalSinkOperator->getOutputSchema(),
                                                                                    logicalSinkOperator->getSinkDescriptor());
        operatorNode->replace(physicalSinkOperator);
        queryPlan->replaceRootOperator(logicalSinkOperator, physicalSinkOperator);
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        auto filterOperator = operatorNode->as<FilterLogicalOperatorNode>();
        auto physicalFilterOperator = PhysicalOperators::PhysicalFilterOperator::create(filterOperator->getInputSchema(),
                                                                                        filterOperator->getOutputSchema(),
                                                                                        filterOperator->getPredicate());
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
    } else if (operatorNode->instanceOf<MapJavaUDFLogicalOperatorNode>()) {
        lowerJavaUDFMapOperator(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<FlatMapJavaUDFLogicalOperatorNode>()) {
        lowerJavaUDFFlatMapOperator(queryPlan, operatorNode);
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
    operatorNode->replace(physicalMapOperator);
}

void DefaultPhysicalOperatorProvider::lowerJavaUDFMapOperator(const QueryPlanPtr&, const LogicalOperatorNodePtr& operatorNode) {
    auto mapJavaUDFOperator = operatorNode->as<MapJavaUDFLogicalOperatorNode>();
    auto physicalMapOperator = PhysicalOperators::PhysicalMapJavaUDFOperator::create(mapJavaUDFOperator->getInputSchema(),
                                                                                     mapJavaUDFOperator->getOutputSchema(),
                                                                                     mapJavaUDFOperator->getJavaUDFDescriptor());
    operatorNode->replace(physicalMapOperator);
}

void DefaultPhysicalOperatorProvider::lowerJavaUDFFlatMapOperator(const QueryPlanPtr&,
                                                                  const LogicalOperatorNodePtr& operatorNode) {
    auto flatMapJavaUDFOperator = operatorNode->as<FlatMapJavaUDFLogicalOperatorNode>();
    auto physicalMapOperator =
        PhysicalOperators::PhysicalFlatMapJavaUDFOperator::create(flatMapJavaUDFOperator->getInputSchema(),
                                                                  flatMapJavaUDFOperator->getOutputSchema(),
                                                                  flatMapJavaUDFOperator->getJavaUDFDescriptor());
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
    auto joinOperator = operatorNode->as<JoinLogicalOperatorNode>();

    if (options->getQueryCompiler() == QueryCompilerOptions::QueryCompiler::DEFAULT_QUERY_COMPILER) {
        // create join operator handler, to establish a common Runtime object for build and prob.
        auto joinOperatorHandler =
            Join::JoinOperatorHandler::create(joinOperator->getJoinDefinition(), joinOperator->getOutputSchema());

        auto leftInputOperator =// the child or child group on the left input side of the join
            getJoinBuildInputOperator(joinOperator, joinOperator->getLeftInputSchema(), joinOperator->getLeftOperators());
        auto leftJoinBuildOperator = PhysicalOperators::PhysicalJoinBuildOperator::create(joinOperator->getLeftInputSchema(),
                                                                                          joinOperator->getOutputSchema(),
                                                                                          joinOperatorHandler,
                                                                                          JoinBuildSideType::Left);
        leftInputOperator->insertBetweenThisAndParentNodes(leftJoinBuildOperator);

        auto rightInputOperator =// the child or child group on the right input side of the join
            getJoinBuildInputOperator(joinOperator, joinOperator->getRightInputSchema(), joinOperator->getRightOperators());
        auto rightJoinBuildOperator = PhysicalOperators::PhysicalJoinBuildOperator::create(joinOperator->getRightInputSchema(),
                                                                                           joinOperator->getOutputSchema(),
                                                                                           joinOperatorHandler,
                                                                                           JoinBuildSideType::Right);
        rightInputOperator->insertBetweenThisAndParentNodes(rightJoinBuildOperator);

        auto joinSink = PhysicalOperators::PhysicalJoinSinkOperator::create(joinOperator->getLeftInputSchema(),
                                                                            joinOperator->getRightInputSchema(),
                                                                            joinOperator->getOutputSchema(),
                                                                            joinOperatorHandler);
        operatorNode->replace(joinSink);
    } else if (options->getQueryCompiler() == QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER) {
        using namespace Runtime::Execution::Operators;

        auto joinDefinition = joinOperator->getJoinDefinition();
        auto joinFieldNameLeft = joinDefinition->getLeftJoinKey()->getFieldName();
        auto joinFieldNameRight = joinDefinition->getRightJoinKey()->getFieldName();

        auto windowType = Windowing::WindowType::asTimeBasedWindowType(joinDefinition->getWindowType());
        //FIXME Once #3353 is merged, sliding window can be added
        NES_ASSERT(windowType->getTimeBasedSubWindowType() == Windowing::TimeBasedWindowType::TUMBLINGWINDOW,
                   "Only a tumbling window is currently supported for StreamJoin");

        std::string timeStampFieldNameLeft = "";
        std::string timeStampFieldNameRight = "";

        if (windowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::IngestionTime) {
            NES_DEBUG("Skip eventime identification as we use ingestion time");
            timeStampFieldNameLeft = "IngestionTime";
            timeStampFieldNameRight = "IngestionTime";
        } else {
            // FIXME Once #3407 is done, we can change this to get the left and right fieldname
            auto timeStampFieldName = windowType->getTimeCharacteristic()->getField()->getName();

            auto timeStampFieldNameWithoutSourceName =
                timeStampFieldName.substr(timeStampFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR));

            // Extract the schema of the right side
            auto rightSchema = joinOperator->getRightInputSchema();
            // Extract the first field from right schema and trim it to find the schema qualifier for the right side
            bool found = false;
            for (auto& field : rightSchema->fields) {
                if (field->getName().find(timeStampFieldNameWithoutSourceName) != std::string::npos) {
                    timeStampFieldNameRight = field->getName();
                    found = true;
                }
            }

            NES_ASSERT(found, " right schema does not contain a timestamp attribute");

            //Extract the schema of the right side
            auto leftSchema = joinOperator->getLeftInputSchema();
            //Extract the first field from right schema and trim it to find the schema qualifier for the right side
            found = false;
            for (auto& field : leftSchema->fields) {
                if (field->getName().find(timeStampFieldNameWithoutSourceName) != std::string::npos) {
                    timeStampFieldNameLeft = field->getName();
                    found = true;
                }
            }
            NES_ASSERT(found, " left schema does not contain a timestamp attribute");
            NES_DEBUG("leftSchema:{} rightSchema: {}", leftSchema->toString(), rightSchema->toString());
            NES_ASSERT(!(timeStampFieldNameLeft.empty() || timeStampFieldNameRight.empty()),
                       "Could not find timestampfieldname " << timeStampFieldNameWithoutSourceName << " in both streams!");
            NES_DEBUG("timeStampFieldNameLeft:{}  timeStampFieldNameRight:{} ", timeStampFieldNameLeft, timeStampFieldNameRight);
        }
        auto windowSize = windowType->getSize().getTime();
        auto numSourcesLeft = joinOperator->getLeftInputOriginIds().size();
        auto numSourcesRight = joinOperator->getRightInputOriginIds().size();

        auto leftInputOperator =
            getJoinBuildInputOperator(joinOperator, joinOperator->getLeftInputSchema(), joinOperator->getLeftOperators());
        auto rightInputOperator =
            getJoinBuildInputOperator(joinOperator, joinOperator->getRightInputSchema(), joinOperator->getRightOperators());

        NodePtr leftJoinBuildOperator;
        NodePtr rightJoinBuildOperator;
        auto joinStrategy = options->getStreamJoinStratgy();
        if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL
            || joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCKING
            || joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCK_FREE) {
            // TODO we should pass this not as an enum.
            QueryCompilation::StreamJoinStrategy runtimeJoinStrategy;
            if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL) {
                runtimeJoinStrategy = QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL;
            } else if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCKING) {
                runtimeJoinStrategy = QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCKING;
            } else {
                runtimeJoinStrategy = QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCK_FREE;
            }
            auto joinOperatorHandler =
                StreamHashJoinOperatorHandler::create(joinOperator->getAllInputOriginIds(),
                                                      windowSize,
                                                      joinOperator->getLeftInputSchema()->getSchemaSizeInBytes(),
                                                      joinOperator->getRightInputSchema()->getSchemaSizeInBytes(),
                                                      options->getHashJoinOptions()->getTotalSizeForDataStructures(),
                                                      options->getHashJoinOptions()->getPageSize(),
                                                      options->getHashJoinOptions()->getPreAllocPageCnt(),
                                                      options->getHashJoinOptions()->getNumberOfPartitions(),
                                                      runtimeJoinStrategy);

            leftJoinBuildOperator = PhysicalOperators::PhysicalHashJoinBuildOperator::create(joinOperator->getLeftInputSchema(),
                                                                                             joinOperator->getOutputSchema(),
                                                                                             joinOperatorHandler,
                                                                                             JoinBuildSideType::Left,
                                                                                             timeStampFieldNameLeft,
                                                                                             joinFieldNameLeft);
            rightJoinBuildOperator = PhysicalOperators::PhysicalHashJoinBuildOperator::create(joinOperator->getRightInputSchema(),
                                                                                              joinOperator->getOutputSchema(),
                                                                                              joinOperatorHandler,
                                                                                              JoinBuildSideType::Right,
                                                                                              timeStampFieldNameRight,
                                                                                              joinFieldNameRight);

            auto joinSinkOperator = PhysicalOperators::PhysicalHashJoinSinkOperator::create(joinOperator->getLeftInputSchema(),
                                                                                            joinOperator->getRightInputSchema(),
                                                                                            joinOperator->getOutputSchema(),
                                                                                            joinFieldNameLeft,
                                                                                            joinFieldNameRight,
                                                                                            joinOperatorHandler);
            leftInputOperator->insertBetweenThisAndParentNodes(leftJoinBuildOperator);
            rightInputOperator->insertBetweenThisAndParentNodes(rightJoinBuildOperator);
            operatorNode->replace(joinSinkOperator);
        } else if (joinStrategy == QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN) {
            auto joinOperatorHandler = NLJOperatorHandler::create(joinOperator->getAllInputOriginIds(),
                                                                  joinOperator->getLeftInputSchema()->getSize(),
                                                                  joinOperator->getRightInputSchema()->getSize(),
                                                                  Nautilus::Interface::PagedVector::PAGE_SIZE,
                                                                  Nautilus::Interface::PagedVector::PAGE_SIZE,
                                                                  windowSize);

            leftJoinBuildOperator =
                PhysicalOperators::PhysicalNestedLoopJoinBuildOperator::create(joinOperator->getLeftInputSchema(),
                                                                               joinOperator->getOutputSchema(),
                                                                               joinOperatorHandler,
                                                                               JoinBuildSideType::Left,
                                                                               timeStampFieldNameLeft,
                                                                               joinFieldNameLeft);
            rightJoinBuildOperator =
                PhysicalOperators::PhysicalNestedLoopJoinBuildOperator::create(joinOperator->getRightInputSchema(),
                                                                               joinOperator->getOutputSchema(),
                                                                               joinOperatorHandler,
                                                                               JoinBuildSideType::Right,
                                                                               timeStampFieldNameRight,
                                                                               joinFieldNameRight);

            auto joinSinkOperator =
                PhysicalOperators::PhysicalNestedLoopJoinSinkOperator::create(joinOperator->getLeftInputSchema(),
                                                                              joinOperator->getRightInputSchema(),
                                                                              joinOperator->getOutputSchema(),
                                                                              joinFieldNameLeft,
                                                                              joinFieldNameRight,
                                                                              joinOperatorHandler);
            leftInputOperator->insertBetweenThisAndParentNodes(leftJoinBuildOperator);
            rightInputOperator->insertBetweenThisAndParentNodes(rightJoinBuildOperator);
            operatorNode->replace(joinSinkOperator);
        } else {
            NES_NOT_IMPLEMENTED();
        }
    }
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
        auto sliceStaging = std::make_shared<Runtime::Execution::Operators::KeyedSliceStaging>();
        keyedOperatorHandlers.sliceMergingOperatorHandler =
            std::make_shared<Runtime::Execution::Operators::KeyedSliceMergingHandler>(sliceStaging);

        NES_ASSERT2_FMT(windowDefinition->getWindowType()->isTimeBasedWindowType(), "window type is not time based");
        auto timeBasedWindowType = Windowing::WindowType::asTimeBasedWindowType(windowDefinition->getWindowType());
        keyedOperatorHandlers.preAggregationWindowHandler =
            std::make_shared<Runtime::Execution::Operators::KeyedSlicePreAggregationHandler>(
                timeBasedWindowType->getSize().getTime(),
                timeBasedWindowType->getSlide().getTime(),
                windowOperator->getInputOriginIds(),
                sliceStaging);
    }

    return keyedOperatorHandlers;
}

GlobalOperatorHandlers
DefaultPhysicalOperatorProvider::createGlobalOperatorHandlers(WindowOperatorProperties& windowOperatorProperties) {

    auto& windowOperator = windowOperatorProperties.windowOperator;
    auto& windowDefinition = windowOperatorProperties.windowDefinition;
    GlobalOperatorHandlers globalOperatorHandlers;

    if (options->getQueryCompiler() == QueryCompilerOptions::QueryCompiler::DEFAULT_QUERY_COMPILER) {
        auto smOperatorHandler = std::make_shared<Windowing::Experimental::GlobalSliceMergingOperatorHandler>(windowDefinition);

        globalOperatorHandlers.preAggregationWindowHandler =
            std::make_shared<Windowing::Experimental::GlobalThreadLocalPreAggregationOperatorHandler>(
                windowDefinition,
                windowOperator->getInputOriginIds(),
                smOperatorHandler->getSliceStagingPtr());

        globalOperatorHandlers.sliceMergingOperatorHandler = smOperatorHandler;
    } else if (options->getQueryCompiler() == QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER) {
        auto sliceStaging = std::make_shared<Runtime::Execution::Operators::GlobalSliceStaging>();
        globalOperatorHandlers.sliceMergingOperatorHandler =
            std::make_shared<Runtime::Execution::Operators::GlobalSliceMergingHandler>(sliceStaging);

        NES_ASSERT2_FMT(windowDefinition->getWindowType()->isTimeBasedWindowType(), "window type is not time based");
        auto timeBasedWindowType = Windowing::WindowType::asTimeBasedWindowType(windowDefinition->getWindowType());
        globalOperatorHandlers.preAggregationWindowHandler =
            std::make_shared<Runtime::Execution::Operators::GlobalSlicePreAggregationHandler>(
                timeBasedWindowType->getSize().getTime(),
                timeBasedWindowType->getSlide().getTime(),
                windowOperator->getInputOriginIds(),
                sliceStaging);
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
                                                                         slidingWindowSinkOperator);
    }
}

std::shared_ptr<Node>
DefaultPhysicalOperatorProvider::replaceOperatorNodeTimeBasedGlobalWindow(WindowOperatorProperties& windowOperatorProperties,
                                                                          const LogicalOperatorNodePtr& operatorNode) {

    auto& windowDefinition = windowOperatorProperties.windowDefinition;
    auto& windowInputSchema = windowOperatorProperties.windowInputSchema;
    auto& windowOutputSchema = windowOperatorProperties.windowOutputSchema;
    auto timeBasedWindowType = Windowing::WindowType::asTimeBasedWindowType(windowDefinition->getWindowType());
    auto windowType = timeBasedWindowType->getTimeBasedSubWindowType();

    if (windowType == Windowing::TimeBasedWindowType::TUMBLINGWINDOW) {
        // Handle tumbling window
        return PhysicalOperators::PhysicalGlobalTumblingWindowSink::create(windowInputSchema,
                                                                           windowOutputSchema,
                                                                           windowDefinition);
    } else {
        // Handle sliding window
        auto globalSliceStore =
            std::make_shared<Windowing::Experimental::GlobalSliceStore<Windowing::Experimental::GlobalSlice>>();
        auto globalSliceStoreAppendOperator =
            std::make_shared<Windowing::Experimental::GlobalWindowGlobalSliceStoreAppendOperatorHandler>(windowDefinition,
                                                                                                         globalSliceStore);
        auto globalSliceStoreAppend =
            PhysicalOperators::PhysicalGlobalWindowSliceStoreAppendOperator::create(windowInputSchema,
                                                                                    windowOutputSchema,
                                                                                    globalSliceStoreAppendOperator);
        auto slidingWindowSinkOperator =
            std::make_shared<Windowing::Experimental::GlobalSlidingWindowSinkOperatorHandler>(windowDefinition, globalSliceStore);

        operatorNode->insertBetweenThisAndChildNodes(globalSliceStoreAppend);
        return PhysicalOperators::PhysicalGlobalSlidingWindowSink::create(windowInputSchema,
                                                                          windowOutputSchema,
                                                                          slidingWindowSinkOperator);
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
        auto preAggregationOperator = PhysicalOperators::PhysicalGlobalThreadLocalPreAggregationOperator::create(
            windowInputSchema,
            windowOutputSchema,
            operatorHandlers.preAggregationWindowHandler,
            windowDefinition);
        operatorNode->insertBetweenThisAndChildNodes(preAggregationOperator);

        auto mergingOperator =
            PhysicalOperators::PhysicalGlobalSliceMergingOperator::create(windowInputSchema,
                                                                          windowOutputSchema,
                                                                          operatorHandlers.sliceMergingOperatorHandler,
                                                                          windowDefinition);
        operatorNode->insertBetweenThisAndChildNodes(mergingOperator);

        if (windowDefinition->getWindowType()->isTimeBasedWindowType()) {
            auto windowSink = replaceOperatorNodeTimeBasedGlobalWindow(windowOperatorProperties, operatorNode);
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
        operatorNode->replace(sliceSink);
    } else if (operatorNode->instanceOf<SliceMergingOperator>()) {
        // Translate a slice merging operator in -> SliceMergingOperator -> SliceSinkOperator
        auto physicalSliceMergingOperator =
            PhysicalOperators::PhysicalSliceMergingOperator::create(windowInputSchema, windowOutputSchema, windowOperatorHandler);
        operatorNode->insertBetweenThisAndChildNodes(physicalSliceMergingOperator);
        auto sliceSink =
            PhysicalOperators::PhysicalSliceSinkOperator::create(windowInputSchema, windowOutputSchema, windowOperatorHandler);
        operatorNode->replace(sliceSink);
    } else if (operatorNode->instanceOf<WindowComputationOperator>()) {
        // Translate a window computation operator in -> PhysicalSliceMergingOperator -> PhysicalWindowSinkOperator
        auto physicalSliceMergingOperator =
            PhysicalOperators::PhysicalSliceMergingOperator::create(windowInputSchema, windowOutputSchema, windowOperatorHandler);
        operatorNode->insertBetweenThisAndChildNodes(physicalSliceMergingOperator);
        auto sliceSink =
            PhysicalOperators::PhysicalWindowSinkOperator::create(windowInputSchema, windowOutputSchema, windowOperatorHandler);
        operatorNode->replace(sliceSink);
    } else {
        throw QueryCompilationException("No conversion for operator " + operatorNode->toString() + " was provided.");
    }
    NES_DEBUG("DefaultPhysicalOperatorProvider::lowerWindowOperator: Plan after\n{}", plan->toString());
}

}// namespace NES::QueryCompilation
