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

#ifndef NES_PLACEMENTAMENDMENTHANDLER_HPP
#define NES_PLACEMENTAMENDMENTHANDLER_HPP

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <folly/concurrency/UnboundedQueue.h>
#include <thread>

namespace NES::Optimizer {

class PlacementAmemderInstance;
using PlacementAmemderInstancePtr = std::shared_ptr<PlacementAmemderInstance>;

class PlacementAmendmentHandler {

  public:
    PlacementAmendmentHandler(uint16_t numOfHandler, folly::UMPMCQueue<PlacementAmemderInstancePtr, false> amendmentRequestQueue);

    /**
     * @brief Start processing amendment instances
     */
    void start();

    void handleRequest();

    /**
     * @brief Shutdown the handler
     */
    void shutDown();

  private:
    uint16_t numOfHandler;
    folly::UMPMCQueue<PlacementAmemderInstancePtr, false> amendmentRequestQueue;
    std::vector<std::thread> amendmentRunners;
    bool running;
};

/**
 * @brief class representing the placement amender instance
 */
class PlacementAmemderInstance {
  public:
    static PlacementAmemderInstancePtr create(SharedQueryPlanPtr sharedQueryPlan,
                                              Optimizer::GlobalExecutionPlanPtr globalExecutionPlan,
                                              TopologyPtr topology,
                                              Optimizer::TypeInferencePhasePtr typeInferencePhase,
                                              Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                                              Catalogs::Query::QueryCatalogPtr queryCatalog);

    PlacementAmemderInstance(SharedQueryPlanPtr sharedQueryPlan,
                             Optimizer::GlobalExecutionPlanPtr globalExecutionPlan,
                             TopologyPtr topology,
                             Optimizer::TypeInferencePhasePtr typeInferencePhase,
                             Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                             Catalogs::Query::QueryCatalogPtr queryCatalog);

    /**
     * @brief Perform the placement amendment
     * @return true if success else false
     */
    bool execute();

  private:
    SharedQueryPlanPtr sharedQueryPlan;
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
    Configurations::CoordinatorConfigurationPtr coordinatorConfiguration;
    Catalogs::Query::QueryCatalogPtr queryCatalog;
};

}// namespace NES::Optimizer
#endif//NES_PLACEMENTAMENDMENTHANDLER_HPP
