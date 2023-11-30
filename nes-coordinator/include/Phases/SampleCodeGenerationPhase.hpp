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

#ifndef NES_COORDINATOR_INCLUDE_PHASES_SAMPLECODEGENERATIONPHASE_HPP_
#define NES_COORDINATOR_INCLUDE_PHASES_SAMPLECODEGENERATIONPHASE_HPP_

#include <memory>

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

namespace QueryCompilation {

class QueryCompiler;
using QueryCompilerPtr = std::shared_ptr<QueryCompiler>;
}// namespace QueryCompilation

namespace Optimizer {

class SampleCodeGenerationPhase;
using SampleCodeGenerationPhasePtr = std::shared_ptr<SampleCodeGenerationPhase>;

/**
 * @brief: This phase allows generating C++ code for each logical operator and add it to the operator as property
 */
class SampleCodeGenerationPhase {

  public:
    static SampleCodeGenerationPhasePtr create();

    /**
     * @param Iterates over the query plan, compute the C++ code for each operator, and add the generated code to the operator property
     * @param queryPlan: the input query plan
     * @return updated query plan
     */
    QueryPlanPtr execute(const QueryPlanPtr& queryPlan);

  private:
    SampleCodeGenerationPhase();

    QueryCompilation::QueryCompilerPtr queryCompiler;
};
}// namespace Optimizer
}// namespace NES
#endif  // NES_COORDINATOR_INCLUDE_PHASES_SAMPLECODEGENERATIONPHASE_HPP_
