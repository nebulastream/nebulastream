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

#include <Services/PlacementAmendment/PlacementAmendmentHandler.hpp>
#include <Services/PlacementAmendment/PlacementAmendmentInstance.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer {
PlacementAmendmentHandler::PlacementAmendmentHandler(uint16_t numOfHandler) : running(false), numOfHandler(numOfHandler) {
    placementAmendmentQueue = std::make_shared<folly::UMPMCQueue<Optimizer::PlacementAmendmentInstancePtr, true>>();
}

PlacementAmendmentHandler::~PlacementAmendmentHandler() {
    NES_INFO("Called ~PlacementAmendmentHandler()");
    shutDown();
}

void PlacementAmendmentHandler::start() {
    if (running.exchange(true)) {
        NES_WARNING("Trying to start already running placement amendment handler. Skipping remaining operation.");
        return;
    }
    // Initiate amendment runners
    NES_INFO("Initializing placement amendment handler {}", numOfHandler);
    for (uint16_t i = 0; i < numOfHandler; i++) {
        amendmentRunners.emplace_back(std::thread([this]() {
            handleRequest();
        }));
    }
}

void PlacementAmendmentHandler::shutDown() {
    NES_INFO("Shutting down the placement amendment handler");
    if (running.exchange(false)) {
        //Join all runners and wait them to be completed before returning the call
        for (auto& amendmentRunner : amendmentRunners) {
            if (amendmentRunner.joinable()) {
                amendmentRunner.join();
            }
        }
    } else {
        NES_WARNING("Placement amendment handler not running. Skipped shutdown operation.")
        return;
    };
    NES_INFO("Placement amendment handler shutdown completed !!!");
}

void PlacementAmendmentHandler::enqueueRequest(const NES::Optimizer::PlacementAmendmentInstancePtr& placementAmendmentInstance) {
    // Enqueue the request to the multi-producer multi-consumer queue.
    placementAmendmentQueue->enqueue(placementAmendmentInstance);
}

void PlacementAmendmentHandler::handleRequest() {
    NES_INFO("Initializing New Handler");
    while (running) {
        PlacementAmendmentInstancePtr placementAmendmentInstance;
        if (placementAmendmentQueue->try_dequeue_for(placementAmendmentInstance, std::chrono::milliseconds(100))) {
            placementAmendmentInstance->execute();
        }
    }
    NES_ERROR("Exiting the thread is running {}", running);
}

}// namespace NES::Optimizer
