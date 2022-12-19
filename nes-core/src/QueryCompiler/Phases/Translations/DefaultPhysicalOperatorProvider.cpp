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
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/BatchJoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/CEP/IterationLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/InferModelLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/InferModelOperatorHandler.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
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
#include <Operators/OperatorForwardDeclaration.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/Exceptions/QueryCompilationException.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/CEP/PhysicalCEPIterationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalBatchJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalBatchJoinProbeOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalExternalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalInferModelOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalScanOperator.hpp>
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
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowHandler/BatchJoinOperatorHandler.hpp>
#include <Windowing/WindowHandler/JoinOperatorHandler.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
#include <Windowing/WindowTypes/ContentBasedWindowType.hpp>
#include <Windowing/WindowTypes/ThresholdWindow.hpp>
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
    // create join operator handler, to establish a common Runtime object for build and prob.
    auto joinOperatorHandler =
        Join::JoinOperatorHandler::create(joinOperator->getJoinDefinition(), joinOperator->getOutputSchema());

    auto leftInputOperator =// the child or child group on the left input side of the join
        getJoinBuildInputOperator(joinOperator, joinOperator->getLeftInputSchema(), joinOperator->getLeftOperators());
    auto leftJoinBuildOperator = PhysicalOperators::PhysicalJoinBuildOperator::create(joinOperator->getLeftInputSchema(),
                                                                                      joinOperator->getOutputSchema(),
                                                                                      joinOperatorHandler,
                                                                                      JoinBuildSide::Left);
    leftInputOperator->insertBetweenThisAndParentNodes(leftJoinBuildOperator);

    auto rightInputOperator =// the child or child group on the right input side of the join
        getJoinBuildInputOperator(joinOperator, joinOperator->getRightInputSchema(), joinOperator->getRightOperators());
    auto rightJoinBuildOperator = PhysicalOperators::PhysicalJoinBuildOperator::create(joinOperator->getRightInputSchema(),
                                                                                       joinOperator->getOutputSchema(),
                                                                                       joinOperatorHandler,
                                                                                       JoinBuildSide::Right);
    rightInputOperator->insertBetweenThisAndParentNodes(rightJoinBuildOperator);

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

void DefaultPhysicalOperatorProvider::lowerThreadLocalWindowOperator(const QueryPlanPtr&,
                                                                     const LogicalOperatorNodePtr& operatorNode) {
    NES_DEBUG("Create Thread local window aggregation");
    auto windowOperator = operatorNode->as<WindowOperatorNode>();
    auto windowInputSchema = windowOperator->getInputSchema();
    auto windowOutputSchema = windowOperator->getOutputSchema();
    auto windowDefinition = windowOperator->getWindowDefinition();
    if (windowOperator->getInputOriginIds().empty()) {
        throw QueryCompilationException("The number of input origin IDs for an window operator should not be zero.");
    }
    // TODO this currently just mimics the old usage of the set of input origins.
    windowDefinition->setNumberOfInputEdges(windowOperator->getInputOriginIds().size());

    if (windowDefinition->isKeyed()) {
        // handle keyed window
        auto sliceMergingOperatorHandler =
            std::make_shared<Windowing::Experimental::KeyedSliceMergingOperatorHandler>(windowDefinition);

        auto preAggregationWindowHandler =
            std::make_shared<Windowing::Experimental::KeyedThreadLocalPreAggregationOperatorHandler>(
                windowDefinition,
                windowOperator->getInputOriginIds(),
                sliceMergingOperatorHandler->getSliceStagingPtr());

        // Translate a central window operator in ->
        // PhysicalKeyedThreadLocalPreAggregationOperator ->
        // PhysicalKeyedSliceMergingOperator
        auto preAggregationOperator =
            PhysicalOperators::PhysicalKeyedThreadLocalPreAggregationOperator::create(windowInputSchema,
                                                                                      windowOutputSchema,
                                                                                      preAggregationWindowHandler);
        operatorNode->insertBetweenThisAndChildNodes(preAggregationOperator);

        auto merging = PhysicalOperators::PhysicalKeyedSliceMergingOperator::create(windowInputSchema,
                                                                                    windowOutputSchema,
                                                                                    sliceMergingOperatorHandler);
        operatorNode->insertBetweenThisAndChildNodes(merging);

        if (windowDefinition->getWindowType()->isTumblingWindow()) {
            auto windowSink = PhysicalOperators::PhysicalKeyedTumblingWindowSink::create(windowInputSchema,
                                                                                         windowOutputSchema,
                                                                                         windowDefinition);
            operatorNode->replace(windowSink);
            return;
        } else if (windowDefinition->getWindowType()->isSlidingWindow()) {
            auto globalSliceStore =
                std::make_shared<Windowing::Experimental::GlobalSliceStore<Windowing::Experimental::KeyedSlice>>();
            auto slidingWindowSinkOperator =
                std::make_shared<Windowing::Experimental::KeyedSlidingWindowSinkOperatorHandler>(windowDefinition,
                                                                                                 globalSliceStore);
            auto globalSliceStoreAppendOperator =
                std::make_shared<Windowing::Experimental::KeyedGlobalSliceStoreAppendOperatorHandler>(windowDefinition,
                                                                                                      globalSliceStore);

            auto globalSliceStoreAppend =
                PhysicalOperators::PhysicalKeyedGlobalSliceStoreAppendOperator::create(windowInputSchema,
                                                                                       windowOutputSchema,
                                                                                       globalSliceStoreAppendOperator);
            operatorNode->insertBetweenThisAndChildNodes(globalSliceStoreAppend);
            auto windowSink = PhysicalOperators::PhysicalKeyedSlidingWindowSink::create(windowInputSchema,
                                                                                        windowOutputSchema,
                                                                                        slidingWindowSinkOperator);
            operatorNode->replace(windowSink);
        } else {
            throw QueryCompilationException("No support for this window type.");
        }
    } else {
        // handle global window
        auto sliceMergingOperatorHandler =
            std::make_shared<Windowing::Experimental::GlobalSliceMergingOperatorHandler>(windowDefinition);

        auto preAggregationWindowHandler =
            std::make_shared<Windowing::Experimental::GlobalThreadLocalPreAggregationOperatorHandler>(
                windowDefinition,
                windowOperator->getInputOriginIds(),
                sliceMergingOperatorHandler->getSliceStagingPtr());

        // Translate a central window operator in ->
        // PhysicalGlobalThreadLocalPreAggregationOperator ->
        // PhysicalGloballiceMergingOperator
        auto preAggregationOperator =
            PhysicalOperators::PhysicalGlobalThreadLocalPreAggregationOperator::create(windowInputSchema,
                                                                                       windowOutputSchema,
                                                                                       preAggregationWindowHandler);
        operatorNode->insertBetweenThisAndChildNodes(preAggregationOperator);

        auto merging = PhysicalOperators::PhysicalGlobalSliceMergingOperator::create(windowInputSchema,
                                                                                     windowOutputSchema,
                                                                                     sliceMergingOperatorHandler);
        operatorNode->insertBetweenThisAndChildNodes(merging);

        if (windowDefinition->getWindowType()->isTumblingWindow()) {
            auto windowSink = PhysicalOperators::PhysicalGlobalTumblingWindowSink::create(windowInputSchema,
                                                                                          windowOutputSchema,
                                                                                          windowDefinition);
            operatorNode->replace(windowSink);
            return;
        } else if (windowDefinition->getWindowType()->isSlidingWindow()) {
            auto globalSliceStore =
                std::make_shared<Windowing::Experimental::GlobalSliceStore<Windowing::Experimental::GlobalSlice>>();
            auto slidingWindowSinkOperator =
                std::make_shared<Windowing::Experimental::GlobalSlidingWindowSinkOperatorHandler>(windowDefinition,
                                                                                                  globalSliceStore);
            auto globalSliceStoreAppendOperator =
                std::make_shared<Windowing::Experimental::GlobalWindowGlobalSliceStoreAppendOperatorHandler>(windowDefinition,
                                                                                                             globalSliceStore);

            auto globalSliceStoreAppend =
                PhysicalOperators::PhysicalGlobalWindowSliceStoreAppendOperator::create(windowInputSchema,
                                                                                        windowOutputSchema,
                                                                                        globalSliceStoreAppendOperator);
            operatorNode->insertBetweenThisAndChildNodes(globalSliceStoreAppend);
            auto windowSink = PhysicalOperators::PhysicalGlobalSlidingWindowSink::create(windowInputSchema,
                                                                                         windowOutputSchema,
                                                                                         slidingWindowSinkOperator);
            operatorNode->replace(windowSink);
        } else {
            throw QueryCompilationException("No support for this window type.");
        }
    }
}

void DefaultPhysicalOperatorProvider::lowerWindowOperator(const QueryPlanPtr& plan, const LogicalOperatorNodePtr& operatorNode) {
    auto windowOperator = operatorNode->as<WindowOperatorNode>();
    auto windowInputSchema = windowOperator->getInputSchema();
    auto windowOutputSchema = windowOperator->getOutputSchema();
    auto windowDefinition = windowOperator->getWindowDefinition();
    NES_DEBUG("DefaultPhysicalOperatorProvider::lowerWindowOperator: Plan before \n" << plan->toString());
    if (windowOperator->getInputOriginIds().empty()) {
        throw QueryCompilationException("The number of input origin IDs for an window operator should not be zero.");
    }
    // TODO this currently just mimics the old usage of the set of input origins.
    windowDefinition->setNumberOfInputEdges(windowOperator->getInputOriginIds().size());

    // create window operator handler, to establish a common Runtime object for aggregation and trigger phase.
    auto windowOperatorHandler = Windowing::WindowOperatorHandler::create(windowDefinition, windowOutputSchema);
    if (operatorNode->instanceOf<CentralWindowOperator>() || operatorNode->instanceOf<WindowLogicalOperatorNode>()) {
        // handle if threshold window
        if (operatorNode->instanceOf<WindowLogicalOperatorNode>()) {
            if (operatorNode->as<WindowOperatorNode>()->getWindowDefinition()->getWindowType()->isThresholdWindow()) {
                auto thresholdWindowPhysicalOperator =
                    PhysicalOperators::PhysicalThresholdWindowOperator::create(windowInputSchema,
                                                                               windowOutputSchema,
                                                                               windowOperatorHandler);
                operatorNode->replace(thresholdWindowPhysicalOperator);
            }
        }
        if (options->getWindowingStrategy() == QueryCompilerOptions::THREAD_LOCAL) {
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
    NES_DEBUG("DefaultPhysicalOperatorProvider::lowerWindowOperator: Plan after \n" << plan->toString());
}

}// namespace NES::QueryCompilation