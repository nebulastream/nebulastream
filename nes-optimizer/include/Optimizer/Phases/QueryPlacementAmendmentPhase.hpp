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
#include <Identifiers/Identifiers.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Util/FaultToleranceType.hpp>
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

class ChangeLogEntry;
using ChangeLogEntryPtr = std::shared_ptr<ChangeLogEntry>;

/**
 * @brief This struct holds information about locations and plans of operator that needs to be migrated
 */
struct MigrateOperatorProperties {
  private:
    /// id of decomposed query id of operator before replacement
    DecomposedQueryId oldDecomposedQueryId;
    /// id of worker id of operator before replacement
    WorkerId oldWorkerId;
    /// id of worker id after replacement
    WorkerId newWorkerId;

  public:
    static std::shared_ptr<MigrateOperatorProperties>
    create(DecomposedQueryId oldDecomposedQueryId, WorkerId oldWorkerId, WorkerId newWorkerId) {
        return std::make_shared<MigrateOperatorProperties>(oldDecomposedQueryId, oldWorkerId, newWorkerId);
    }

    MigrateOperatorProperties(DecomposedQueryId oldDecomposedQueryId, WorkerId oldWorkerId, WorkerId newWorkerId)
        : oldDecomposedQueryId(oldDecomposedQueryId), oldWorkerId(oldWorkerId), newWorkerId(newWorkerId) {}

    /**
     * @brief Get the id of decomposed plan from the old operator location node
     * @return the id of decomposed query plan
     */
    DecomposedQueryId getOldDecomposedQueryId() const { return oldDecomposedQueryId; }

    /**
     * @brief Get the id of worker, where operator was located
     * @return the id of worker id
     */
    WorkerId getOldWorkerId() const { return oldWorkerId; }

    /**
     * @brief Get the id of worker, where operator is located
     * @return the id of worker id
     */
    WorkerId getNewWorkerId() const { return newWorkerId; }
};

/**
 * @brief This struct wraps around the information about the reconfiguration marker units that are
 */
struct ReconfigurationMarkerUnit {
  public:
    ReconfigurationMarkerUnit(const std::string address,
                              WorkerId workerId,
                              SharedQueryId sharedQueryId,
                              DecomposedQueryId decomposedQueryId)
        : address(address), workerId(workerId), sharedQueryId(sharedQueryId), decomposedQueryId(decomposedQueryId){};

    bool operator<(ReconfigurationMarkerUnit otherReconfigurationMarkerUnit) const {
        return decomposedQueryId.getRawValue() < otherReconfigurationMarkerUnit.decomposedQueryId.getRawValue();
    }

    const std::string address;
    const WorkerId workerId;
    const SharedQueryId sharedQueryId;
    const DecomposedQueryId decomposedQueryId;
};

/**
 */

/**
 * @brief This struct holds the placement removal and addition deployment contexts. In addition a set of reconfigurationMarkerUnits need to be sent.
 * This is necessary as after a placement amendment we might want to restart a running query by first removing the existing placements, then adding new one, and
 * using reconfiguration markers to terminate plans that are marked for removal and starting newly deployed or updated plans.
 */
struct DeploymentUnit {
  public:
    DeploymentUnit(std::set<DeploymentContextPtr> deploymentRemovalContexts,
                   std::set<DeploymentContextPtr> deploymentAdditionContexts,
                   std::set<ReconfigurationMarkerUnit> reconfigurationMarkerUnits)
        : deploymentRemovalContexts(deploymentRemovalContexts), deploymentAdditionContexts(deploymentAdditionContexts),
          reconfigurationMarkerUnits(reconfigurationMarkerUnits){};

    bool containsDeploymentContext() { return !deploymentAdditionContexts.empty() || !deploymentRemovalContexts.empty(); }

    std::set<DeploymentContextPtr> getAllDeploymentContexts() {
        std::set<DeploymentContextPtr> combinedDeploymentContexts;
        combinedDeploymentContexts.insert(deploymentRemovalContexts.begin(), deploymentRemovalContexts.end());
        combinedDeploymentContexts.insert(deploymentAdditionContexts.begin(), deploymentAdditionContexts.end());
        return combinedDeploymentContexts;
    }

    const std::set<DeploymentContextPtr> deploymentRemovalContexts;
    const std::set<DeploymentContextPtr> deploymentAdditionContexts;
    const std::set<ReconfigurationMarkerUnit> reconfigurationMarkerUnits;
};

static std::atomic_uint64_t counter{0};

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
    DeploymentUnit execute(const SharedQueryPlanPtr& sharedQueryPlan);

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
    BasePlacementStrategyPtr getStrategy(PlacementStrategy placementStrategy, FaultToleranceType faultTolerance);

    /**
     * @brief perform placement removal for all operators marked for deletion
     * @param sharedQueryId: id of the shared query plan
     * @param upstreamOperators: the upstream operators
     * @param downstreamOperators: the downstream operators
     * @param nextDecomposedQueryPlanVersion: next decomposed query plan version
     * @param deploymentContexts: map containing un-deployment contexts for removed placements
     */
    void handlePlacementRemoval(SharedQueryId sharedQueryId,
                                const std::set<LogicalOperatorPtr>& upstreamOperators,
                                const std::set<LogicalOperatorPtr>& downstreamOperators,
                                DecomposedQueryPlanVersion& nextDecomposedQueryPlanVersion,
                                std::map<DecomposedQueryId, DeploymentContextPtr>& deploymentContexts);

    /**
     * @brief perform placement addition for all operators with missing placements
     * @param placementStrategy: the placement strategy
     * @param upstreamOperators: upstream operators
     * @param downstreamOperators: downstream operators
     * @param nextDecomposedQueryPlanVersion: next decomposed query plan version
     * @param deploymentContexts: map containing deployment contexts for newly added placements
     */
    void handlePlacementAddition(PlacementStrategy placementStrategy,
                                 SharedQueryId sharedQueryId,
                                 const std::set<LogicalOperatorPtr>& upstreamOperators,
                                 const std::set<LogicalOperatorPtr>& downstreamOperators,
                                 DecomposedQueryPlanVersion& nextDecomposedQueryPlanVersion,
                                 std::map<DecomposedQueryId, DeploymentContextPtr>& deploymentContexts,
                                 FaultToleranceType faultTolerance);

    /**
     * @brief Method to identify the location where reconfiguration marker needs to be pushed.
     * @param sharedQueryId: the id of the shared query plan
     * @param changeLogEntry : the change log entry
     * @param reconfigurationMarkerUnitComparator : the collection of reconfiguration makers
     */
    void
    computeReconfigurationMarkerDeploymentUnit(SharedQueryId& sharedQueryId,
                                               const ChangeLogEntryPtr& changeLogEntry,
                                               std::set<ReconfigurationMarkerUnit>& reconfigurationMarkerUnitComparator) const;

    /**
     * @brief perform placement for migrating operators
     * @param placementStrategy: the placement strategy
     * @param migratingOperatorToProperties: operator id to operator properties (old and new node id, old decomposed plan id)
     * @param planIdToCopy: decomposed plan copy saved by operator id
     * @param sharedQueryId: id of the shared query plan
     * @param nextDecomposedQueryPlanVersion: next decomposed query plan version
     * @param deploymentContexts: map containing deployment contexts
     */
    void handleMigrationPlacement(
        Optimizer::PlacementStrategy placementStrategy,
        const std::unordered_map<OperatorId, std::shared_ptr<MigrateOperatorProperties>>& migratingOperatorToProperties,
        std::unordered_map<OperatorId, std::string>& migratingOperatorToFileSink,
        std::unordered_map<DecomposedQueryId, std::shared_ptr<DecomposedQueryPlan>> planIdToCopy,
        SharedQueryId sharedQueryId,
        DecomposedQueryPlanVersion& nextDecomposedQueryPlanVersion,
        std::map<DecomposedQueryId, DeploymentContextPtr>& deploymentContexts);

    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    TypeInferencePhasePtr typeInferencePhase;
    z3::ContextPtr z3Context;
    Configurations::CoordinatorConfigurationPtr coordinatorConfiguration;
};
}// namespace NES::Optimizer
#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_QUERYPLACEMENTAMENDMENTPHASE_HPP_
