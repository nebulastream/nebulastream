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

#ifndef NES_COORDINATOR_INCLUDE_SERVICES_PLACEMENTAMENDMENT_PLACEMENTAMENDMENTHANDLER_HPP_
#define NES_COORDINATOR_INCLUDE_SERVICES_PLACEMENTAMENDMENT_PLACEMENTAMENDMENTHANDLER_HPP_

#include <folly/concurrency/UnboundedQueue.h>
#include <future>
#include <thread>

namespace NES::Optimizer {

class PlacementAmendmentInstance;
using PlacementAmendmentInstancePtr = std::shared_ptr<PlacementAmendmentInstance>;

using UMPMCAmendmentQueuePtr = std::shared_ptr<folly::UMPMCQueue<NES::Optimizer::PlacementAmendmentInstancePtr, true>>;

/**
 * @brief The placement amendment handler class is responsible for processing placement amendments of updated shared query plans.
 * To this end, we can initialize the handles with a pre-configured number of handler threads.
 */
class PlacementAmendmentHandler {

  public:
    /**
     * @brief Ctor
     * @param numOfHandler: number of handler threads to spawn
     */
    PlacementAmendmentHandler(uint16_t numOfHandler);

    /**
     * @brief Dtor
     */
    virtual ~PlacementAmendmentHandler();

    /**
     * @brief Start processing amendment instances
     */
    void start();

    /**
     * @brief Shutdown the handler
     */
    void shutDown();

    /**
     * @brief Method allows to enqueue the amendment instance for execution in the queue
     * @param placementAmendmentInstance : the object containing required payload and execution logic for amending invalid or
     * missing placements of a shared query plan
     */
    virtual void enqueueRequest(const NES::Optimizer::PlacementAmendmentInstancePtr& placementAmendmentInstance);

  private:
    /**
     * @brief Processed requests queued in the amendment request queue
     */
    void handleRequest();

    std::atomic<bool> running;
    uint16_t numOfHandler;
    UMPMCAmendmentQueuePtr placementAmendmentQueue;
    std::vector<std::thread> amendmentRunners;
};
using PlacementAmendmentHandlerPtr = std::shared_ptr<PlacementAmendmentHandler>;
}// namespace NES::Optimizer
#endif// NES_COORDINATOR_INCLUDE_SERVICES_PLACEMENTAMENDMENT_PLACEMENTAMENDMENTHANDLER_HPP_
