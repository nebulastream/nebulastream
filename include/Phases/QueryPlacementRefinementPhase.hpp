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

#ifndef NES_QueryPlacementRefinementPhase_HPP
#define NES_QueryPlacementRefinementPhase_HPP

#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <memory>

namespace NES {
class QueryPlacementRefinementPhase;
typedef std::shared_ptr<QueryPlacementRefinementPhase> QueryPlacementRefinementPhasePtr;

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
    bool execute(QueryId queryId);

    ~QueryPlacementRefinementPhase();

  private:
    explicit QueryPlacementRefinementPhase(GlobalExecutionPlanPtr globalPlan);

    GlobalExecutionPlanPtr globalExecutionPlan;
};
}// namespace NES
#endif//NES_QueryPlacementRefinementPhase_HPP
