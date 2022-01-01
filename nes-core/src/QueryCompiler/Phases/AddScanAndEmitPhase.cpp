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
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/Exceptions/QueryCompilationException.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalBinaryOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/AddScanAndEmitPhase.hpp>

namespace NES::QueryCompilation {

AddScanAndEmitPhasePtr AddScanAndEmitPhase::create() { return std::make_shared<AddScanAndEmitPhase>(); }

PipelineQueryPlanPtr AddScanAndEmitPhase::apply(PipelineQueryPlanPtr pipelineQueryPlan) {
    for (const auto& pipeline : pipelineQueryPlan->getPipelines()) {
        if (pipeline->isOperatorPipeline()) {
            process(pipeline);
        }
    }
    return pipelineQueryPlan;
}

OperatorPipelinePtr AddScanAndEmitPhase::process(OperatorPipelinePtr pipeline) {
    auto queryPlan = pipeline->getQueryPlan();
    auto pipelineRootOperators = queryPlan->getRootOperators();
    if (pipelineRootOperators.size() != 1) {
        throw QueryCompilationException("A pipeline should only have one root operator");
    }
    auto rootOperator = pipelineRootOperators[0];
    // insert buffer scan operator to the pipeline root if necessary
    if (!rootOperator->instanceOf<PhysicalOperators::AbstractScanOperator>()) {
        if (rootOperator->instanceOf<PhysicalOperators::PhysicalUnaryOperator>()) {
            auto binaryRoot = rootOperator->as<PhysicalOperators::PhysicalUnaryOperator>();
            auto newScan = PhysicalOperators::PhysicalScanOperator::create(binaryRoot->getInputSchema());
            pipeline->prependOperator(newScan);
        } else {
            throw QueryCompilationException("Pipeline root shot be a unary operator but was:" + rootOperator->toString());
        }
    }

    // insert emit buffer operator if necessary
    auto pipelineLeafOperators = rootOperator->getAllLeafNodes();
    for (const auto& leaf : pipelineLeafOperators) {
        auto leafOperator = leaf->as<OperatorNode>();
        if (!leafOperator->instanceOf<PhysicalOperators::AbstractEmitOperator>()) {
            auto emitOperator = PhysicalOperators::PhysicalEmitOperator::create(leafOperator->getOutputSchema());
            leafOperator->addChild(emitOperator);
        }
    }
    return pipeline;
}

}// namespace NES::QueryCompilation
