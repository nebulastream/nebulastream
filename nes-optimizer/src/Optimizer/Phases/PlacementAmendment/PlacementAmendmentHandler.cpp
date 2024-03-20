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

#include <Optimizer/Phases/PlacementAmendment/PlacementAmendmentHandler.hpp>
#include <Optimizer/Phases/PlacementAmendment/PlacementAmendmentInstance.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer {
PlacementAmendmentHandler::PlacementAmendmentHandler(uint16_t numOfHandler, UMPMCAmendmentQueuePtr amendmentQueue)
    : running(true), numOfHandler(numOfHandler), amendmentQueue(amendmentQueue) {}

void PlacementAmendmentHandler::start() {
    // Initiate amendment runners
    NES_INFO("Initializing placement amendment handler {}", numOfHandler);
    for (size_t i = 0; i < numOfHandler; ++i) {
        amendmentRunners.emplace_back(std::thread([this]() {
            handleRequest();
        }));
    }
}

void PlacementAmendmentHandler::handleRequest() {
    while (running) {
        PlacementAmendmentInstancePtr placementAmendmentInstance;
        if (!amendmentQueue->try_dequeue(placementAmendmentInstance)) {
            continue;
        }
        placementAmendmentInstance->execute();
    }
}

void PlacementAmendmentHandler::shutDown() {
    running = false;
    //Join all runners and wait them to be completed before returning the call
    for (auto& amendmentRunner : amendmentRunners) {
        if (amendmentRunner.joinable()) {
            amendmentRunner.join();
        }
    }
}
}// namespace NES::Optimizer
