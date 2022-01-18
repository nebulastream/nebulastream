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

#ifndef NES_INCLUDE_OPTIMIZER_PHASES_QUERY_PLACEMENT_PHASE_HPP_
#define NES_INCLUDE_OPTIMIZER_PHASES_QUERY_PLACEMENT_PHASE_HPP_

#include <memory>

namespace z3 {
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;
}// namespace NES

namespace NES::Optimizer {

class QueryPlacementPhase;
using QueryPlacementPhasePtr = std::shared_ptr<QueryPlacementPhase>;

class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;
/**
 * @brief This class is responsible for placing operators of an input query plan on a global execution plan.
 */
class QueryPlacementPhase {
  public:
    /**
     *
     * @param globalExecutionPlan : an instance of global execution plan
     * @param topology : topology in which the placement is to be performed
     * @param typeInferencePhase  : a type inference phase instance
     * @param streamCatalog : a stream catalog
     * @param z3Context : context from the z3 library used for optimization
     * @return
     */
    static QueryPlacementPhasePtr create(GlobalExecutionPlanPtr globalExecutionPlan,
                                         TopologyPtr topology,
                                         TypeInferencePhasePtr typeInferencePhase,
                                         SourceCatalogPtr streamCatalog,
                                         z3::ContextPtr z3Context);

    /**
     * @brief Method takes input as a placement strategy name and input query plan and performs query operator placement based on the
     * selected query placement strategy
     * @param placementStrategy : name of the placement strategy
     * @param queryPlan : the query plan
     * @return true is placement successful.
     * @throws QueryPlacementException
     */
    bool execute(const std::string& placementStrategy, QueryPlanPtr queryPlan);

  private:
    explicit QueryPlacementPhase(GlobalExecutionPlanPtr globalExecutionPlan,
                                 TopologyPtr topology,
                                 TypeInferencePhasePtr typeInferencePhase,
                                 SourceCatalogPtr streamCatalog,
                                 z3::ContextPtr z3Context);
    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    TypeInferencePhasePtr typeInferencePhase;
    SourceCatalogPtr streamCatalog;
    z3::ContextPtr z3Context;
};
}// namespace NES::Optimizer
#endif// NES_INCLUDE_OPTIMIZER_PHASES_QUERY_PLACEMENT_PHASE_HPP_
