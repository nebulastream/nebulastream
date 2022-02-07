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

#ifndef NES_INCLUDE_OPTIMIZER_PHASES_QUERYPLACEMENTPHASE_HPP_
#define NES_INCLUDE_OPTIMIZER_PHASES_QUERYPLACEMENTPHASE_HPP_

#include <Plans/Query/QueryId.hpp>
#include <Util/PlacementStrategy.hpp>
#include <memory>
#include <vector>

namespace z3 {
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class SharedQueryPlan;
using SharedQueryPlanPtr = std::shared_ptr<SharedQueryPlan>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

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
     * This method creates an instance of query placement phase
     * @param globalExecutionPlan : an instance of global execution plan
     * @param topology : topology in which the placement is to be performed
     * @param typeInferencePhase  : a type inference phase instance
     * @param z3Context : context from the z3 library used for optimization
     * @param queryReconfiguration: should place only updates in the query plan
     * @return pointer to query placement phase
     */
    static QueryPlacementPhasePtr create(GlobalExecutionPlanPtr globalExecutionPlan,
                                         TopologyPtr topology,
                                         TypeInferencePhasePtr typeInferencePhase,
                                         z3::ContextPtr z3Context,
                                         bool queryReconfiguration);

    /**
     * @brief Method takes input as a placement strategy name and input query plan and performs query operator placement based on the
     * selected query placement strategy
     * @param placementStrategy : name of the placement strategy
     * @param sharedQueryPlan : the shared query plan to place
     * @return true is placement successful.
     * @throws QueryPlacementException
     */
    bool execute(PlacementStrategy::Value placementStrategy, const SharedQueryPlanPtr& sharedQueryPlan);

  private:
    explicit QueryPlacementPhase(GlobalExecutionPlanPtr globalExecutionPlan,
                                 TopologyPtr topology,
                                 TypeInferencePhasePtr typeInferencePhase,
                                 z3::ContextPtr z3Context,
                                 bool queryReconfiguration);

    /**
     * This method extracts the upstream pinned operators from the shared query plan. IF the reconfiguration is enabled then the
     * method brows through the change log to extract the upstream operators
     * @param sharedQueryPlan : shared query plan to investigate
     * @return collection of upstream operators
     */
    std::vector<OperatorNodePtr> getUpStreamPinnedOperators(SharedQueryPlanPtr sharedQueryPlan);

    /**
     * This method extracts the downstream pinned operators connected to the collection of upstream operators.
     * @param upStreamPinnedOperators : collection of upstream pinned operators
     * @return collection of downstream operators
     */
    std::vector<OperatorNodePtr> getDownStreamPinnedOperators(std::vector<OperatorNodePtr> upStreamPinnedOperators);

    /**
     * This method checks if the operators in the list are pinned or not
     * @param pinnedOperators: operators to check
     * @return false if one of the operator is not pinned else true
     */
    bool checkPinnedOperators(const std::vector<OperatorNodePtr>& pinnedOperators);

    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    TypeInferencePhasePtr typeInferencePhase;
    z3::ContextPtr z3Context;
    bool queryReconfiguration;
};
}// namespace NES::Optimizer
#endif// NES_INCLUDE_OPTIMIZER_PHASES_QUERYPLACEMENTPHASE_HPP_
