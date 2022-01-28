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

#ifndef NES_INCLUDE_OPTIMIZER_PHASES_QUERYPLACEMENTREFINEMENTPHASE_HPP_
#define NES_INCLUDE_OPTIMIZER_PHASES_QUERYPLACEMENTREFINEMENTPHASE_HPP_

#include <Plans/Query/QueryId.hpp>
#include <memory>

namespace NES {
class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;
}// namespace NES

namespace NES::Optimizer {

class QueryPlacementRefinementPhase;
using QueryPlacementRefinementPhasePtr = std::shared_ptr<QueryPlacementRefinementPhase>;

/**
 * @brief This phase is responsible for refinement of the query plan
 */
class QueryPlacementRefinementPhase {
  public:
    static QueryPlacementRefinementPhasePtr create(GlobalExecutionPlanPtr globalPlan);

    /**
     * @brief apply changes to the global query plan
     * @param queryId
     * @return success
     */
    static bool execute(QueryId queryId);

  private:
    explicit QueryPlacementRefinementPhase(GlobalExecutionPlanPtr globalPlan);

    GlobalExecutionPlanPtr globalExecutionPlan;
};
}// namespace NES::Optimizer
#endif  // NES_INCLUDE_OPTIMIZER_PHASES_QUERYPLACEMENTREFINEMENTPHASE_HPP_
