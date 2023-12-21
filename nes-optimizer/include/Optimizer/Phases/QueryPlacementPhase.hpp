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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_QUERYPLACEMENTPHASE_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_QUERYPLACEMENTPHASE_HPP_

#include <Identifiers.hpp>
#include <Util/PlacementStrategy.hpp>
#include <memory>
#include <set>
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

class LogicalOperatorNode;
using LogicalOperatorNodePtr = std::shared_ptr<LogicalOperatorNode>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

namespace Configurations {
class CoordinatorConfiguration;
using CoordinatorConfigurationPtr = std::shared_ptr<CoordinatorConfiguration>;
}// namespace Configurations

namespace Catalogs::Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Catalogs::Source
}// namespace NES

namespace NES::Optimizer {

class BasePlacementStrategy;
using BasePlacementStrategyPtr = std::unique_ptr<BasePlacementStrategy>;

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
     * @param coordinatorConfiguration: coordinator configuration
     * @return pointer to query placement phase
     */
    static QueryPlacementPhasePtr create(GlobalExecutionPlanPtr globalExecutionPlan,
                                         TopologyPtr topology,
                                         TypeInferencePhasePtr typeInferencePhase,
                                         Configurations::CoordinatorConfigurationPtr coordinatorConfiguration);

    /**
     * @brief Method takes input as a placement strategy name and input query plan and performs query operator placement based on the
     * selected query placement strategy
     * @param sharedQueryPlan : the shared query plan to place
     * @return true is placement successful.
     * @throws QueryPlacementException
     */
    bool execute(const SharedQueryPlanPtr& sharedQueryPlan);

  private:
    explicit QueryPlacementPhase(GlobalExecutionPlanPtr globalExecutionPlan,
                                 TopologyPtr topology,
                                 TypeInferencePhasePtr typeInferencePhase,
                                 Configurations::CoordinatorConfigurationPtr coordinatorConfiguration);

    /**
     * @brief: analyze the set and pin all unpinned sink operators
     * @param operators: set of operators to check
     */
    void pinAllSinkOperators(const std::set<LogicalOperatorNodePtr>& operators);

    /**
     * This method checks if the operators in the set are pinned or not
     * @param pinnedOperators: operators to check
     * @return false if one of the operator is not pinned else true
     */
    bool checkIfAllArePinnedOperators(const std::set<LogicalOperatorNodePtr>& pinnedOperators);

    /**
     * @brief method returns different kind of placement strategies.
     * @param placementStrategy : name of the strategy
     * @return instance of type BaseOptimizer
     */
    BasePlacementStrategyPtr getStrategy(PlacementStrategy placementStrategy);

    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    TypeInferencePhasePtr typeInferencePhase;
    z3::ContextPtr z3Context;
    Configurations::CoordinatorConfigurationPtr coordinatorConfiguration;
};
}// namespace NES::Optimizer
#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_QUERYPLACEMENTPHASE_HPP_
