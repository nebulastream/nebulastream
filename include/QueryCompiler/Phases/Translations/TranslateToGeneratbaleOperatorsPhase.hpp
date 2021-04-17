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
#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATETOGENERATABLEOPERATORS_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATETOGENERATABLEOPERATORS_HPP_

#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <vector>

namespace NES {
namespace QueryCompilation {

/**
 * @brief This phase translates a query plan of logical operators into a query plan of physical operators.
 * The translation of individual operators is defined by the physical operator provider to improve extendability.
 */
class TranslateToGeneratableOperators {
  public:
    TranslateToGeneratableOperators(GeneratableOperatorProviderPtr provider);
    static TranslateToGeneratableOperatorsPtr create(GeneratableOperatorProviderPtr provider);
    PipelineQueryPlanPtr apply(PipelineQueryPlanPtr queryPlan);

  private:
    GeneratableOperatorProviderPtr provider;
};
}// namespace QueryCompilation
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATETOGENERATABLEOPERATORS_HPP_
