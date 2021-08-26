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
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableBufferEmit.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableFilterOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableFilterOperatorPredicated.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/GeneratableSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Phases/PredicationOptimizationPhase.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

namespace NES::QueryCompilation {

PredicationOptimizationPhasePtr
PredicationOptimizationPhase::PredicationOptimizationPhase::create(bool predicationEnabled) {
    return std::make_shared<PredicationOptimizationPhase>(predicationEnabled);
}

PredicationOptimizationPhase::PredicationOptimizationPhase(bool predicationEnabled) : predicationEnabled(predicationEnabled) {}

PipelineQueryPlanPtr PredicationOptimizationPhase::apply(PipelineQueryPlanPtr pipelinedQueryPlan) {
    if (!predicationEnabled) {
        NES_DEBUG("PredicationOptimizationPhase: No optimization requested or applied.");
        return pipelinedQueryPlan;
    }

    for (const auto& pipeline : pipelinedQueryPlan->getPipelines()) {
        if (pipeline->isOperatorPipeline()) {
            apply(pipeline);
        }
    }
    return pipelinedQueryPlan;
}

OperatorPipelinePtr PredicationOptimizationPhase::apply(OperatorPipelinePtr operatorPipeline) {
    if (!predicationEnabled) {
        NES_DEBUG("PredicationOptimizationPhase: No optimization requested or applied.");
        return operatorPipeline;
    }

    NES_DEBUG("PredicationOptimizationPhase: Scanning pipeline for optimization potential.");
    auto queryPlan = operatorPipeline->getQueryPlan();
    auto nodes = QueryPlanIterator(queryPlan).snapshot();

    // TODO enable predication based on a cost model and data characteristics

    std::shared_ptr<GeneratableOperators::GeneratableFilterOperator> filterOperator = nullptr;
    std::shared_ptr<GeneratableOperators::GeneratableBufferEmit> emitOperator = nullptr;

    // search for the relevant nodes; abort if windowing operator is found:
    for (const auto& node : nodes) {
        if (node->instanceOf<GeneratableOperators::GeneratableFilterOperator>()) {
            filterOperator = node->as<GeneratableOperators::GeneratableFilterOperator>();
        } else if (node->instanceOf<GeneratableOperators::GeneratableBufferEmit>()) {
            emitOperator = node->as<GeneratableOperators::GeneratableBufferEmit>();
        } else if (node->instanceOf<GeneratableOperators::GeneratableSlicePreAggregationOperator>()) {
            NES_DEBUG("PredicationOptimizationPhase: No predication applied. Predication is not yet implemented for windowing queries.");
            // TODO predication for windowing queries
            return operatorPipeline;
        }
    }

    if (filterOperator == nullptr || emitOperator == nullptr) {
        NES_DEBUG("PredicationOptimizationPhase: No predication applied. Filter operator or emit operator not found.");
        return operatorPipeline;
    }

    // now, apply predication:
    auto predicatedFilterOperator =
            GeneratableOperators::GeneratableFilterOperatorPredicated::create(filterOperator->getInputSchema(),
                                                                              filterOperator->getPredicate());
    queryPlan->replaceOperator(filterOperator, predicatedFilterOperator);
    emitOperator->setIncreasesResultBufferWriteIndex(false);
    NES_DEBUG("PredicationOptimizationPhase: Replaced filter operator with equivalent predicated filter operator. Configured following emit operator to work with predicated filter operator.");

    return operatorPipeline;
}

}// namespace NES::QueryCompilation