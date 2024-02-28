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
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Phases/BufferOptimizationPhase.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::QueryCompilation {

BufferOptimizationPhasePtr BufferOptimizationPhase::BufferOptimizationPhase::create(OutputBufferOptimizationLevel level) {
    return std::make_shared<BufferOptimizationPhase>(level);
}

BufferOptimizationPhase::BufferOptimizationPhase(OutputBufferOptimizationLevel level) : level(level) {}

PipelineQueryPlanPtr BufferOptimizationPhase::apply(PipelineQueryPlanPtr pipelinedQueryPlan) {
    for (const auto& pipeline : pipelinedQueryPlan->getPipelines()) {
        if (pipeline->isOperatorPipeline()) {
            apply(pipeline);
        }
    }
    return pipelinedQueryPlan;
}

bool BufferOptimizationPhase::isReadOnlyInput(OperatorPipelinePtr pipeline) {
    // We define the input of a pipeline as read only if it is shared with another pipeline.
    // To this end, we check if one of our parents has more than one child.
    for (const auto& parent : pipeline->getPredecessors()) {
        if (parent->getSuccessors().size() > 1) {
            // the parent has more than one successor. So our input is read only.
            return true;
        }
    }
    return false;
}

OperatorPipelinePtr BufferOptimizationPhase::apply(OperatorPipelinePtr operatorPipeline) { return operatorPipeline; }

}// namespace NES::QueryCompilation