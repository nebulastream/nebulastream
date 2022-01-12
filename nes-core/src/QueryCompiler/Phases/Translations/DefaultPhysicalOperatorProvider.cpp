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
#include <Operators/LogicalOperators/CEP/IterationLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
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
#include <QueryCompiler/Operators/PhysicalOperators/CEP/PhysicalCEPIterationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalExternalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/EventTimeWindow/PhysicalKeyedSlidingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/EventTimeWindow/PhysicalKeyedGlobalSliceStoreAppendOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/EventTimeWindow/PhysicalKeyedSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/EventTimeWindow/PhysicalKeyedThreadLocalPreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/EventTimeWindow/PhysicalKeyedTumblingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowSinkOperator.hpp>
#include <QueryCompiler/Phases/Translations/DefaultPhysicalOperatorProvider.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Windowing/Experimental/PartitionedWindowOperator.hpp>
#include <Windowing/WindowHandler/JoinOperatorHandler.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
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

    NES_ASSERT(operatorNode->getParents().size() <= 1, "A unary operator should only have at most one parent.");

    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        auto logicalSourceOperator = operatorNode->as<SourceLogicalOperatorNode>();
        auto physicalSourceOperator =
            PhysicalOperators::PhysicalSourceOperator::create(logicalSourceOperator->getInputSchema(),
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
    } else if (operatorNode->instanceOf<ProjectionLogicalOperatorNode>()) {
        lowerProjectOperator(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<IterationLogicalOperatorNode>()) {
        lowerCEPIterationOperator(queryPlan, operatorNode);
    } else {
        NES_ERROR("No conversion for operator " + operatorNode->toString() + " was provided.");
    }
}

void DefaultPhysicalOperatorProvider::lowerBinaryOperator(const QueryPlanPtr& queryPlan,
                                                          const LogicalOperatorNodePtr& operatorNode) {

    if (operatorNode->instanceOf<UnionLogicalOperatorNode>()) {
        lowerUnionOperator(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<JoinLogicalOperatorNode>()) {
        lowerJoinOperator(queryPlan, operatorNode);
    } else {
        NES_ERROR("No conversion for operator " + operatorNode->toString() + " was provided.");
    }
}

void DefaultPhysicalOperatorProvider::lowerUnionOperator(const QueryPlanPtr&, const LogicalOperatorNodePtr& operatorNode) {

    auto unionOperator = operatorNode->as<UnionLogicalOperatorNode>();
    // this assumes that we applies the ProjectBeforeUnionRule and the input across all children is the same.
    NES_ASSERT(unionOperator->getLeftInputSchema()->equals(unionOperator->getRightInputSchema()),
               "The children of a union operator should have the same schema. Left:"
                   << unionOperator->getLeftInputSchema()->toString() << " but right "
                   << unionOperator->getRightInputSchema()->toString());
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
    NES_ASSERT(!children.empty(), "Their should be children for operator " << joinOperator->toString());
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

    auto leftInputOperator =
        getJoinBuildInputOperator(joinOperator, joinOperator->getLeftInputSchema(), joinOperator->getLeftOperators());
    auto leftJoinBuildOperator = PhysicalOperators::PhysicalJoinBuildOperator::create(joinOperator->getLeftInputSchema(),
                                                                                      joinOperator->getOutputSchema(),
                                                                                      joinOperatorHandler,
                                                                                      JoinBuildSide::Left);
    leftInputOperator->insertBetweenThisAndParentNodes(leftJoinBuildOperator);

    auto rightInputOperator =
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

void DefaultPhysicalOperatorProvider::lowerWatermarkAssignmentOperator(const QueryPlanPtr&,
                                                                       const LogicalOperatorNodePtr& operatorNode) {
    /* if (true) {
        operatorNode->removeAndJoinParentAndChildren();
        return;
    }
    */

    auto logicalWatermarkAssignment = operatorNode->as<WatermarkAssignerLogicalOperatorNode>();
    auto physicalWatermarkAssignment = PhysicalOperators::PhysicalWatermarkAssignmentOperator::create(
        logicalWatermarkAssignment->getInputSchema(),
        logicalWatermarkAssignment->getOutputSchema(),
        logicalWatermarkAssignment->getWatermarkStrategyDescriptor());
    physicalWatermarkAssignment->setOutputSchema(logicalWatermarkAssignment->getOutputSchema());
    operatorNode->replace(physicalWatermarkAssignment);
}

void DefaultPhysicalOperatorProvider::lowerWindowOperator(const QueryPlanPtr&, const LogicalOperatorNodePtr& operatorNode) {
    auto windowOperator = operatorNode->as<WindowOperatorNode>();
    auto windowInputSchema = windowOperator->getInputSchema();
    auto windowOutputSchema = windowOperator->getOutputSchema();
    auto windowDefinition = windowOperator->getWindowDefinition();
    /*
    if (true) {
        auto windowBufferManager = std::make_shared<Runtime::BufferManager>(1024 * 1024, 20000);
        uint64_t sliceSize = 0;
        if (windowDefinition->getWindowType()->isTumblingWindow()) {
            auto tumblingWindow = dynamic_pointer_cast<Windowing::TumblingWindow>(windowDefinition->getWindowType());
            // auto numberOfSlices = windowDefinition->getAllowedLateness() / tumblingWindow->getSize().getTime();
            sliceSize = tumblingWindow->getSize().getTime();
        } else {
            auto slidingWindow = dynamic_pointer_cast<Windowing::SlidingWindow>(windowDefinition->getWindowType());
            // auto numberOfSlices = windowDefinition->getAllowedLateness() / tumblingWindow->getSize().getTime();
            sliceSize = slidingWindow->getSlide().getTime();
        }
        auto windowHandler = std::make_shared<Experimental::PreAggregationWindowHandler<uint64_t, uint64_t>>(windowDefinition,
                                                                                                             windowBufferManager,
                                                                                                             sliceSize);
        auto windowAggregateOperator = std::make_shared<Experimental::WindowAggregateOperator>(windowHandler);
        auto externalWindowAggregateOperator = PhysicalOperators::PhysicalExternalOperator::create(Util::getNextOperatorId(),
                                                                                                   operatorNode->getOutputSchema(),
                                                                                                   operatorNode->getOutputSchema(),
                                                                                                   windowAggregateOperator);
        operatorNode->insertBetweenThisAndChildNodes(externalWindowAggregateOperator);

        auto mergeSliceOperator = std::make_shared<Experimental::MergeSliceWindowOperator>(windowHandler);
        auto externalMergeSliceOperator = PhysicalOperators::PhysicalExternalOperator::create(Util::getNextOperatorId(),
                                                                                              operatorNode->getOutputSchema(),
                                                                                              operatorNode->getOutputSchema(),
                                                                                              mergeSliceOperator);
        externalWindowAggregateOperator->insertBetweenThisAndChildNodes(externalMergeSliceOperator);

        auto preAggregateOperator = std::make_shared<Experimental::PreAggregateWindowOperator>(windowHandler);
        auto externalPreAggregateOperator = PhysicalOperators::PhysicalExternalOperator::create(Util::getNextOperatorId(),
                                                                                                operatorNode->getOutputSchema(),
                                                                                                operatorNode->getOutputSchema(),
                                                                                                preAggregateOperator);
        externalMergeSliceOperator->insertBetweenThisAndChildNodes(externalPreAggregateOperator);


        operatorNode->replace(
            PhysicalOperators::PhysicalScanOperator::create(Util::getNextOperatorId(), operatorNode->getOutputSchema()));
        return;
    }
*/

    // create window operator handler, to establish a common Runtime object for aggregation and trigger phase.
    auto windowOperatorHandler = Windowing::WindowOperatorHandler::create(windowDefinition, windowOutputSchema);
    if (operatorNode->instanceOf<CentralWindowOperator>() || operatorNode->instanceOf<WindowLogicalOperatorNode>()) {
        if (options->getWindowingStrategy() == QueryCompilerOptions::THREAD_LOCAL) {
            NES_DEBUG("Create Thread local window aggregation");

            auto windowHandler = std::make_shared<Windowing::Experimental::KeyedEventTimeWindowHandler>(windowDefinition);

            // Translate a central window operator in ->
            // PhysicalKeyedThreadLocalPreAggregationOperator ->
            // PhysicalKeyedSliceMergingOperator
            auto preAggregationOperator =
                PhysicalOperators::PhysicalKeyedThreadLocalPreAggregationOperator::create(windowInputSchema,
                                                                                          windowOutputSchema,
                                                                                          windowHandler);
            operatorNode->insertBetweenThisAndChildNodes(preAggregationOperator);
            auto merging = PhysicalOperators::PhysicalKeyedSliceMergingOperator::create(windowInputSchema,
                                                                                        windowOutputSchema,
                                                                                        windowHandler);
            operatorNode->insertBetweenThisAndChildNodes(merging);
            if (windowDefinition->getWindowType()->isTumblingWindow()) {
                auto windowSink = PhysicalOperators::PhysicalKeyedTumblingWindowSink::create(windowInputSchema,
                                                                                             windowOutputSchema,
                                                                                             windowHandler);
                operatorNode->replace(windowSink);
                return;
            } else {
                auto globalSliceStoreAppend = PhysicalOperators::PhysicalKeyedGlobalSliceStoreAppendOperator::create(windowInputSchema,
                                                                                            windowOutputSchema,
                                                                                            windowHandler);
                operatorNode->insertBetweenThisAndChildNodes(globalSliceStoreAppend);
                auto windowSink = PhysicalOperators::PhysicalKeyedSlidingWindowSink::create(windowInputSchema,
                                                                                             windowOutputSchema,
                                                                                             windowHandler);
                operatorNode->replace(windowSink);
            }
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
        NES_ERROR("No conversion for operator " + operatorNode->toString() + " was provided.");
    }
}

}// namespace NES::QueryCompilation