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

#include <QueryCompiler/Phases/Experimental/Vectorization/LowerPhysicalToVectorizedOperatorsPhase.hpp>

#include <Nodes/Node.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Experimental/Vectorization/PhysicalVectorizedPipelineOperator.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::QueryCompilation::Experimental {

using namespace PhysicalOperators;
using namespace PhysicalOperators::Experimental;

LowerPhysicalToVectorizedOperatorsPhase::LowerPhysicalToVectorizedOperatorsPhase(const QueryCompilerOptionsPtr& options)
    : options(std::move(options))
{

}

PipelineQueryPlanPtr LowerPhysicalToVectorizedOperatorsPhase::apply(PipelineQueryPlanPtr pipelinedQueryPlan) {
    auto vectorizationEnabled = options->getVectorizationOptions()->isUsingVectorization();
    if (!vectorizationEnabled) {
        return pipelinedQueryPlan;
    }

    for (const auto& pipeline : pipelinedQueryPlan->getPipelines()) {
        if (pipeline->isOperatorPipeline()) {
            lower(pipeline);
        }
    }

    return pipelinedQueryPlan;
}

OperatorPipelinePtr LowerPhysicalToVectorizedOperatorsPhase::lower(OperatorPipelinePtr operatorPipeline) {
    auto queryPlan = operatorPipeline->getDecomposedQueryPlan();
    auto nodes = PlanIterator(queryPlan).snapshot();

    NES_DEBUG("Looking for vectorizable physical operators...");
    for (const auto& node : nodes) {
        auto physicalOperator = node->as<PhysicalOperator>();

        auto vectorizedPipelineOpt = tryLowerToVectorizedPipeline(physicalOperator);
        if (vectorizedPipelineOpt) {
            auto vectorizedPipeline = vectorizedPipelineOpt.value();
            NES_DEBUG("Lower {} to {}", physicalOperator->toString(), vectorizedPipeline->toString());
            physicalOperator->replace(vectorizedPipeline);
        }
    }

    return operatorPipeline;
}

std::optional<PhysicalOperatorPtr> LowerPhysicalToVectorizedOperatorsPhase::tryLowerToVectorizedPipeline(const PhysicalOperatorPtr&) {
    return std::nullopt;
}

} // namespace NES::QueryCompilation::Experimental
