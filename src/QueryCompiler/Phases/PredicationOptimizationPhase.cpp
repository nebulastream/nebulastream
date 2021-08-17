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
    for (const auto& pipeline : pipelinedQueryPlan->getPipelines()) {
        if (pipeline->isOperatorPipeline()) {
            apply(pipeline);
        }
    }
    return pipelinedQueryPlan;
}

OperatorPipelinePtr PredicationOptimizationPhase::apply(OperatorPipelinePtr operatorPipeline) {
    if (!predicationEnabled)
        return operatorPipeline;

    NES_DEBUG("PredicationOptimizationPhase: Scanning pipeline for optimization potential.");
    auto queryPlan = operatorPipeline->getQueryPlan();
    auto nodes = QueryPlanIterator(queryPlan).snapshot();

    // TODO enable predication based on a cost model and data characteristics
    bool isPredicated = false;
    for (const auto& node : nodes) {
        if (!isPredicated && node->instanceOf<GeneratableOperators::GeneratableFilterOperator>()) {
            auto filterOperator = node->as<GeneratableOperators::GeneratableFilterOperator>();
            auto predicatedFilterOperator =
                    GeneratableOperators::GeneratableFilterOperatorPredicated::create(filterOperator->getInputSchema(),
                                                                                      filterOperator->getPredicate());

            NES_DEBUG("PredicationOptimizationPhase: Replaced filter operator with equivalent predicated filter operator.");
            isPredicated = queryPlan->replaceOperator(filterOperator, predicatedFilterOperator);

        } else if (isPredicated && node->instanceOf<GeneratableOperators::GeneratableBufferEmit>()) {
            auto emitOperator = node->as<GeneratableOperators::GeneratableBufferEmit>();
            emitOperator->setIncreasesResultBufferWriteIndex(false);
            NES_DEBUG("PredicationOptimizationPhase: Configured following emit operator to work with predicated filter operator.");
            return operatorPipeline;
        }
    }

    return operatorPipeline;
}

}// namespace NES::QueryCompilation