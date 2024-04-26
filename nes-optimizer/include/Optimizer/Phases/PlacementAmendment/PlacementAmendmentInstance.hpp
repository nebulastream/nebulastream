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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_PLACEMENTAMENDMENT_PLACEMENTAMENDMENTINSTANCE_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_PLACEMENTAMENDMENT_PLACEMENTAMENDMENTINSTANCE_HPP_

#include <future>
#include <memory>

namespace NES {

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class SharedQueryPlan;
using SharedQueryPlanPtr = std::shared_ptr<SharedQueryPlan>;

namespace Configurations {
class CoordinatorConfiguration;
using CoordinatorConfigurationPtr = std::shared_ptr<CoordinatorConfiguration>;
}// namespace Configurations

namespace Catalogs::Query {
class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;
}// namespace Catalogs::Query

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

namespace Optimizer {

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;

class PlacementAmendmentInstance;
using PlacementAmendmentInstancePtr = std::shared_ptr<PlacementAmendmentInstance>;

/**
 * @brief class representing the placement amendment instance
 */
class PlacementAmendmentInstance {
  public:
    static PlacementAmendmentInstancePtr create(SharedQueryPlanPtr sharedQueryPlan,
                                                Optimizer::GlobalExecutionPlanPtr globalExecutionPlan,
                                                TopologyPtr topology,
                                                TypeInferencePhasePtr typeInferencePhase,
                                                Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                                                Catalogs::Query::QueryCatalogPtr queryCatalog);

    PlacementAmendmentInstance(SharedQueryPlanPtr sharedQueryPlan,
                               Optimizer::GlobalExecutionPlanPtr globalExecutionPlan,
                               TopologyPtr topology,
                               TypeInferencePhasePtr typeInferencePhase,
                               Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                               Catalogs::Query::QueryCatalogPtr queryCatalog);

    /**
     * @brief Get promise to check if the amendment instance was processed
     * @return
     */
    std::future<bool> getFuture();

    /**
     * @brief Perform the placement amendment
     * @return true if success else false
     */
    void execute();

  private:
    SharedQueryPlanPtr sharedQueryPlan;
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    TypeInferencePhasePtr typeInferencePhase;
    Configurations::CoordinatorConfigurationPtr coordinatorConfiguration;
    Catalogs::Query::QueryCatalogPtr queryCatalog;
    std::promise<bool> completionPromise;
};
}// namespace Optimizer
}// namespace NES
#endif // NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_PLACEMENTAMENDMENT_PLACEMENTAMENDMENTINSTANCE_HPP_
