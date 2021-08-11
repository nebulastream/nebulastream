/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <QueryCompiler/Exceptions/QueryCompilationException.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/CEP/GeneratableCEPIterationOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableBufferEmit.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableBufferScan.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableFilterOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableMapOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableProjectionOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Joining/GeneratableJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Joining/GeneratableJoinSinkOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableAvgAggregation.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableCountAggregation.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableMaxAggregation.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableMinAggregation.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableSumAggregation.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/GeneratableSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/GeneratableSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/CEP/PhysicalCEPIterationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowSinkOperator.hpp>
#include <QueryCompiler/Phases/Translations/DefaultGeneratableOperatorProvider.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowAggregations/CountAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/MaxAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/MinAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
#include <utility>

class PhysicalCEPIterationOperator;
namespace NES::QueryCompilation {

GeneratableOperatorProviderPtr DefaultGeneratableOperatorProvider::create() {
    return std::make_shared<DefaultGeneratableOperatorProvider>();
}

void DefaultGeneratableOperatorProvider::lower(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode) {
    NES_DEBUG("Lower " << operatorNode->toString() << " to generatable operator");
    if (operatorNode->instanceOf<PhysicalOperators::PhysicalSourceOperator>()) {
        lowerSource(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalSinkOperator>()) {
        lowerSink(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalScanOperator>()) {
        lowerScan(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalEmitOperator>()) {
        lowerEmit(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalProjectOperator>()) {
        lowerProjection(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalFilterOperator>()) {
        lowerFilter(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalMapOperator>()) {
        lowerMap(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalIterationCEPOperator>()) {
        lowerCEPIteration(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalWatermarkAssignmentOperator>()) {
        lowerWatermarkAssignment(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalSlicePreAggregationOperator>()) {
        lowerSlicePreAggregation(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalWindowSinkOperator>()) {
        lowerWindowSink(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalSliceSinkOperator>()) {
        lowerSliceSink(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalSliceMergingOperator>()) {
        lowerSliceMerging(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalJoinBuildOperator>()) {
        lowerJoinBuild(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalJoinSinkOperator>()) {
        lowerJoinSink(queryPlan, operatorNode);
    } else {
        throw QueryCompilationException("No lowering defined for physical operator: " + operatorNode->toString());
    }
}

void DefaultGeneratableOperatorProvider::lowerSink(const QueryPlanPtr&,
                                                   const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    // a sink operator should be in a pipeline on its own.
    NES_ASSERT(operatorNode->getChildren().size(), "A sink node should have no children");
    NES_ASSERT(operatorNode->getParents().size(), "A sink node should have no parents");
}

void DefaultGeneratableOperatorProvider::lowerSource(const QueryPlanPtr&,
                                                     const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    // a source operator should be in a pipeline on its own.
    NES_ASSERT(operatorNode->getChildren().size(), "A source operator should have no children");
    NES_ASSERT(operatorNode->getParents().size(), "A source operator should have no parents");
}

void DefaultGeneratableOperatorProvider::lowerScan(const QueryPlanPtr& queryPlan,
                                                   const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto bufferScan = GeneratableOperators::GeneratableBufferScan::create(operatorNode->getOutputSchema());
    queryPlan->replaceOperator(operatorNode, bufferScan);
}

void DefaultGeneratableOperatorProvider::lowerEmit(const QueryPlanPtr& queryPlan,
                                                   const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto bufferEmit = GeneratableOperators::GeneratableBufferEmit::create(operatorNode->getOutputSchema());
    queryPlan->replaceOperator(operatorNode, bufferEmit);
}

void DefaultGeneratableOperatorProvider::lowerProjection(const QueryPlanPtr& queryPlan,
                                                         const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto physicalProjectionOperator = operatorNode->as<PhysicalOperators::PhysicalProjectOperator>();
    auto generatableProjectOperator =
        GeneratableOperators::GeneratableProjectionOperator::create(physicalProjectionOperator->getInputSchema(),
                                                                    physicalProjectionOperator->getOutputSchema(),
                                                                    physicalProjectionOperator->getExpressions());
    queryPlan->replaceOperator(physicalProjectionOperator, generatableProjectOperator);
}

void DefaultGeneratableOperatorProvider::lowerFilter(const QueryPlanPtr& queryPlan,
                                                     const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto physicalFilterOperator = operatorNode->as<PhysicalOperators::PhysicalFilterOperator>();
    auto generatableFilterOperator =
        GeneratableOperators::GeneratableFilterOperator::create(physicalFilterOperator->getInputSchema(),
                                                                physicalFilterOperator->getPredicate());
    queryPlan->replaceOperator(physicalFilterOperator, generatableFilterOperator);
}

void DefaultGeneratableOperatorProvider::lowerMap(const QueryPlanPtr& queryPlan,
                                                  const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto physicalMapOperator = operatorNode->as<PhysicalOperators::PhysicalMapOperator>();
    auto generatableMapOperator = GeneratableOperators::GeneratableMapOperator::create(physicalMapOperator->getInputSchema(),
                                                                                       physicalMapOperator->getOutputSchema(),
                                                                                       physicalMapOperator->getMapExpression());
    queryPlan->replaceOperator(physicalMapOperator, generatableMapOperator);
}

void DefaultGeneratableOperatorProvider::lowerWatermarkAssignment(const QueryPlanPtr& queryPlan,
                                                                  const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto physicalWatermarkAssignmentOperator = operatorNode->as<PhysicalOperators::PhysicalWatermarkAssignmentOperator>();
    auto generatableWatermarkAssignmentOperator = GeneratableOperators::GeneratableWatermarkAssignmentOperator::create(
        physicalWatermarkAssignmentOperator->getInputSchema(),
        physicalWatermarkAssignmentOperator->getOutputSchema(),
        physicalWatermarkAssignmentOperator->getWatermarkStrategyDescriptor());
    queryPlan->replaceOperator(physicalWatermarkAssignmentOperator, generatableWatermarkAssignmentOperator);
}

GeneratableOperators::GeneratableWindowAggregationPtr DefaultGeneratableOperatorProvider::lowerWindowAggregation(
    const Windowing::WindowAggregationDescriptorPtr& windowAggregationDescriptor) {
    switch (windowAggregationDescriptor->getType()) {
        case Windowing::WindowAggregationDescriptor::Count: {
            return GeneratableOperators::GeneratableCountAggregation::create(windowAggregationDescriptor);
        };
        case Windowing::WindowAggregationDescriptor::Max: {
            return GeneratableOperators::GeneratableMaxAggregation::create(windowAggregationDescriptor);
        };
        case Windowing::WindowAggregationDescriptor::Min: {
            return GeneratableOperators::GeneratableMinAggregation::create(windowAggregationDescriptor);
        };
        case Windowing::WindowAggregationDescriptor::Sum: {
            return GeneratableOperators::GeneratableSumAggregation::create(windowAggregationDescriptor);
        };
        case Windowing::WindowAggregationDescriptor::Avg: {
            return GeneratableOperators::GeneratableAvgAggregation::create(windowAggregationDescriptor);
        };
        default:
            throw QueryCompilationException(
                catString("TranslateToGeneratableOperatorPhase: No transformation implemented for this window aggregation type: ",
                          windowAggregationDescriptor->getType()));
    }
}

void DefaultGeneratableOperatorProvider::lowerSlicePreAggregation(const QueryPlanPtr& queryPlan,
                                                                  const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto slicePreAggregationOperator = operatorNode->as<PhysicalOperators::PhysicalSlicePreAggregationOperator>();

    auto windowAggregationDescriptor =
        slicePreAggregationOperator->getOperatorHandler()->getWindowDefinition()->getWindowAggregation();
    auto generatableWindowAggregation = lowerWindowAggregation(windowAggregationDescriptor);

    auto generatableOperator =
        GeneratableOperators::GeneratableSlicePreAggregationOperator::create(slicePreAggregationOperator->getInputSchema(),
                                                                             slicePreAggregationOperator->getOutputSchema(),
                                                                             slicePreAggregationOperator->getOperatorHandler(),
                                                                             generatableWindowAggregation);
    queryPlan->replaceOperator(slicePreAggregationOperator, generatableOperator);
}

void DefaultGeneratableOperatorProvider::lowerSliceMerging(const QueryPlanPtr& queryPlan,
                                                           const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto sliceMergingOperator = operatorNode->as<PhysicalOperators::PhysicalSliceMergingOperator>();

    auto windowAggregationDescriptor = sliceMergingOperator->getOperatorHandler()->getWindowDefinition()->getWindowAggregation();
    auto generatableWindowAggregation = lowerWindowAggregation(windowAggregationDescriptor);

    auto generatableOperator =
        GeneratableOperators::GeneratableSliceMergingOperator::create(sliceMergingOperator->getInputSchema(),
                                                                      sliceMergingOperator->getOutputSchema(),
                                                                      sliceMergingOperator->getOperatorHandler(),
                                                                      generatableWindowAggregation);
    queryPlan->replaceOperator(sliceMergingOperator, generatableOperator);
}

void DefaultGeneratableOperatorProvider::lowerWindowSink(QueryPlanPtr queryPlan,
                                                         PhysicalOperators::PhysicalOperatorPtr operatorNode) {
    // a window sink is lowered to a standard scan operator
    lowerScan(std::move(queryPlan), std::move(operatorNode));
}

void DefaultGeneratableOperatorProvider::lowerSliceSink(QueryPlanPtr queryPlan,
                                                        PhysicalOperators::PhysicalOperatorPtr operatorNode) {
    // a slice sink is lowered to a standard scan operator
    lowerScan(std::move(queryPlan), std::move(operatorNode));
}

void DefaultGeneratableOperatorProvider::lowerJoinBuild(const QueryPlanPtr& queryPlan,
                                                        const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto physicalJoinBuild = operatorNode->as<PhysicalOperators::PhysicalJoinBuildOperator>();
    auto generatableJoinOperator =
        GeneratableOperators::GeneratableJoinBuildOperator::create(physicalJoinBuild->getInputSchema(),
                                                                   physicalJoinBuild->getOutputSchema(),
                                                                   physicalJoinBuild->getJoinHandler(),
                                                                   physicalJoinBuild->getBuildSide());
    queryPlan->replaceOperator(operatorNode, generatableJoinOperator);
}

void DefaultGeneratableOperatorProvider::lowerJoinSink(const QueryPlanPtr& queryPlan,
                                                       const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto physicalJoinSink = operatorNode->as<PhysicalOperators::PhysicalJoinSinkOperator>();
    auto generatableJoinOperator = GeneratableOperators::GeneratableJoinSinkOperator::create(physicalJoinSink->getOutputSchema(),
                                                                                             physicalJoinSink->getOutputSchema(),
                                                                                             physicalJoinSink->getJoinHandler());
    queryPlan->replaceOperator(operatorNode, generatableJoinOperator);
}

void DefaultGeneratableOperatorProvider::lowerCEPIteration(QueryPlanPtr queryPlan,
                                                           PhysicalOperators::PhysicalOperatorPtr operatorNode) {
    auto physicalCEPIterationOperator = operatorNode->as<PhysicalOperators::PhysicalIterationCEPOperator>();
    auto generatableCEPIterationOperator =
        GeneratableOperators::GeneratableCEPIterationOperator::create(physicalCEPIterationOperator->getInputSchema(),
                                                                      physicalCEPIterationOperator->getOutputSchema(),
                                                                      physicalCEPIterationOperator->getMinIterations(),
                                                                      physicalCEPIterationOperator->getMaxIterations());
    queryPlan->replaceOperator(physicalCEPIterationOperator, generatableCEPIterationOperator);
}

}// namespace NES::QueryCompilation