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

#include <folly/concurrency/UnboundedQueue.h>
#include <future>
#include <thread>

namespace NES {

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class SharedQueryPlan;
using SharedQueryPlanPtr = std::shared_ptr<SharedQueryPlan>;

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

class PlacementAmemderInstance;
using PlacementAmemderInstancePtr = std::shared_ptr<PlacementAmemderInstance>;

class PlacementAmemderInstance;
using PlacementAmemderInstancePtr = std::shared_ptr<PlacementAmemderInstance>;

using UMPMCAmendmentQueuePtr = std::shared_ptr<folly::UMPMCQueue<NES::Optimizer::PlacementAmemderInstancePtr, false>>;

/**
 * @brief The placement amendment handler class is responsible for processing placement amendments of updated shared query plans.
 * To this end, we can initialize the handles with a pre-configured number of handler threads.
 */
class PlacementAmendmentHandler {

  public:
    PlacementAmendmentHandler(uint16_t numOfHandler, UMPMCAmendmentQueuePtr amendmentRequestQueue);

    /**
     * @brief Start processing amendment instances
     */
    void start();

    /**
     * @brief Processed requests queued in the amendment request queue
     */
    void handleRequest();

    /**
     * @brief Shutdown the handler
     */
    void shutDown();

  private:
    bool running;
    uint16_t numOfHandler;
    UMPMCAmendmentQueuePtr amendmentQueue;
    std::vector<std::thread> amendmentRunners;
};

/**
 * @brief class representing the placement amender instance
 */
class PlacementAmemderInstance {
  public:
    static PlacementAmemderInstancePtr create(SharedQueryPlanPtr sharedQueryPlan,
                                              Optimizer::GlobalExecutionPlanPtr globalExecutionPlan,
                                              TopologyPtr topology,
                                              TypeInferencePhasePtr typeInferencePhase,
                                              Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                                              Catalogs::Query::QueryCatalogPtr queryCatalog);

    PlacementAmemderInstance(SharedQueryPlanPtr sharedQueryPlan,
                             Optimizer::GlobalExecutionPlanPtr globalExecutionPlan,
                             TopologyPtr topology,
                             TypeInferencePhasePtr typeInferencePhase,
                             Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                             Catalogs::Query::QueryCatalogPtr queryCatalog);

    /**
     * @brief Get promise to check if the amender instance was processed
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
#endif//NES_PLACEMENTAMENDMENTHANDLER_HPP
