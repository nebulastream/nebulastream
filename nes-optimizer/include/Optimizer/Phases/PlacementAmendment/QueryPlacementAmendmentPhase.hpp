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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_QUERYPLACEMENTAMENDMENTPHASE_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_QUERYPLACEMENTAMENDMENTPHASE_HPP_

#include <Configurations/Enums/PlacementAmendmentMode.hpp>
#include <Identifiers.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
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

class LogicalOperator;
using LogicalOperatorPtr = std::shared_ptr<LogicalOperator>;

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

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class BasePlacementAdditionStrategy;
using BasePlacementStrategyPtr = std::unique_ptr<BasePlacementAdditionStrategy>;

class QueryPlacementAmendmentPhase;
using QueryPlacementAmendmentPhasePtr = std::shared_ptr<QueryPlacementAmendmentPhase>;

class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;

class DeploymentContext;
using DeploymentContextPtr = std::shared_ptr<DeploymentContext>;

/**
 * @brief This class is responsible for placing and removing operators (depending on their status) from the
 * global execution.
 */
class QueryPlacementAmendmentPhase {
  public:
    /**
     * This method creates an instance of query placement phase
     * @param globalExecutionPlan : an instance of global execution plan
     * @param topology : topology in which the placement is to be performed
     * @param typeInferencePhase  : a type inference phase instance
     * @param coordinatorConfiguration: coordinator configuration
     * @return pointer to query placement phase
     */
    static QueryPlacementAmendmentPhasePtr create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                  TopologyPtr topology,
                                                  TypeInferencePhasePtr typeInferencePhase,
                                                  Configurations::CoordinatorConfigurationPtr coordinatorConfiguration);

    /**
     * @brief Method takes input a shared query plan and performs first query operator placement
     * removal and then addition based on the selected query placement strategy
     * @param sharedQueryPlan : the input shared query plan
     * @return true is placement amendment successful.
     * @throws QueryPlacementException
     */
    std::set<DeploymentContextPtr> execute(const SharedQueryPlanPtr& sharedQueryPlan);

  private:
    explicit QueryPlacementAmendmentPhase(GlobalExecutionPlanPtr globalExecutionPlan,
                                          TopologyPtr topology,
                                          TypeInferencePhasePtr typeInferencePhase,
                                          Configurations::CoordinatorConfigurationPtr coordinatorConfiguration);

    /**
     * @brief: analyze the set and pin all unpinned sink operators
     * @param operators: set of operators to check
     */
    void pinAllSinkOperators(const std::set<LogicalOperatorPtr>& operators);

    /**
     * This method checks if the operators in the set are pinned or not
     * @param pinnedOperators: operators to check
     * @return false if one of the operator is not pinned else true
     */
    bool containsOnlyPinnedOperators(const std::set<LogicalOperatorPtr>& pinnedOperators);

    /**
     * @brief Check if in the provided set at least one operator is in the state To_Be_Placed or Placed
     * @param operatorsToCheck the logical operator nodes
     * @return true if at least one operator passes the condition
     */
    bool containsOperatorsForPlacement(const std::set<LogicalOperatorPtr>& operatorsToCheck);

    /**
     * @brief Check if in the provided set at least one operator is in the state To_Be_RePlaced, Placed, or To_Be_Removed
     * Note: TODO
     * @param operatorsToCheck the logical operator nodes
     * @return true if at least one operator passes the condition
     */
    bool containsOperatorsForRemoval(const std::set<LogicalOperatorPtr>& operatorsToCheck);

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
    PlacementAmendmentMode placementAmendmentMode;
};
}// namespace NES::Optimizer
#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_QUERYPLACEMENTAMENDMENTPHASE_HPP_
