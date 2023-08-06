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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_EXPERIMENTAL_VECTORIZATION_LOWERPHYSICALTOVECTORIZEDOPERATORSPHASE_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_EXPERIMENTAL_VECTORIZATION_LOWERPHYSICALTOVECTORIZEDOPERATORSPHASE_HPP_

#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <optional>

namespace NES::QueryCompilation::Experimental {

/**
 * @brief This phase takes a pipeline plan of physical operators and replaces suitable operators with VectorizedPipeline operators.
 */
class LowerPhysicalToVectorizedOperatorsPhase {
public:
    explicit LowerPhysicalToVectorizedOperatorsPhase(const QueryCompilerOptionsPtr& options);

    /**
     * @brief Applies the phase on a pipelined query plan.
     * @param pipelined query plan
     * @return PipelineQueryPlanPtr
     */
    PipelineQueryPlanPtr apply(PipelineQueryPlanPtr pipelinedQueryPlan);

private:
    OperatorPipelinePtr lower(OperatorPipelinePtr operatorPipeline);

    std::optional<PhysicalOperators::PhysicalOperatorPtr> tryLowerToVectorizedPipeline(const PhysicalOperators::PhysicalOperatorPtr& physicalOperator);

    QueryCompilerOptionsPtr options;
};

} // namespace NES::QueryCompilation::Experimental

#endif // NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_EXPERIMENTAL_VECTORIZATION_LOWERPHYSICALTOVECTORIZEDOPERATORSPHASE_HPP_
