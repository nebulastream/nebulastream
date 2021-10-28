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
#ifndef NES_INCLUDE_QUERY_COMPILER_PHASES_TRANSLATIONS_LOWER_PHYSICAL_TO_GENERATABLE_OPERATORS_HPP_
#define NES_INCLUDE_QUERY_COMPILER_PHASES_TRANSLATIONS_LOWER_PHYSICAL_TO_GENERATABLE_OPERATORS_HPP_

#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <vector>

namespace NES {
namespace QueryCompilation {

/**
 * @brief This phase lowers a pipeline plan of physical operators into a pipeline plan of generatable operators.
 * The lowering of individual operators is defined by the generatable operator provider to improve extendability.
 */
class LowerPhysicalToGeneratableOperators {
  public:
    /**
     * @brief Constructor to create a LowerPhysicalToGeneratableOperatorPhase
     * @param provider to lower specific physical operator to generatabale operators
     */
    explicit LowerPhysicalToGeneratableOperators(GeneratableOperatorProviderPtr provider);

    /**
     * @brief Create a LowerPhysicalToGeneratableOperatorPhase
     * @param provider to lower specific physical operator to generatabale operators
     */
    static LowerPhysicalToGeneratableOperatorsPtr create(const GeneratableOperatorProviderPtr& provider);

    /**
     * @brief Applies the phase on a pipelined query plan.
     * @param pipelined query plan
     * @return PipelineQueryPlanPtr
     */
    PipelineQueryPlanPtr apply(PipelineQueryPlanPtr pipelinedQueryPlan);

    /**
     * @brief Applies the phase on a pipelined and lower physical operator to generatable once.
     * @param pipeline
     * @return OperatorPipelinePtr
     */
    OperatorPipelinePtr apply(OperatorPipelinePtr pipeline);

  private:
    GeneratableOperatorProviderPtr provider;
};
}// namespace QueryCompilation
}// namespace NES
#endif// NES_INCLUDE_QUERY_COMPILER_PHASES_TRANSLATIONS_LOWER_PHYSICAL_TO_GENERATABLE_OPERATORS_HPP_
