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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_PLACEMENTAMENDMENT_PLACEMENTAMENDMENTHANDLER_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_PLACEMENTAMENDMENT_PLACEMENTAMENDMENTHANDLER_HPP_

#include <folly/concurrency/UnboundedQueue.h>
#include <future>
#include <thread>

namespace NES::Optimizer {

class PlacementAmendmentInstance;
using PlacementAmendmentInstancePtr = std::shared_ptr<PlacementAmendmentInstance>;

using UMPMCAmendmentQueuePtr = std::shared_ptr<folly::UMPMCQueue<NES::Optimizer::PlacementAmendmentInstancePtr, false>>;

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

}// namespace NES::Optimizer
#endif // NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_PLACEMENTAMENDMENT_PLACEMENTAMENDMENTHANDLER_HPP_
