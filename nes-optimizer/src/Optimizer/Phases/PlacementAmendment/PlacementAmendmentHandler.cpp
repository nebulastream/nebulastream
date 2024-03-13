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

namespace NES::Optimizer {

PlacementAmendmentHandler::PlacementAmendmentHandler(uint16_t numOfHandler,
                                                     folly::UMPMCQueue<PlacementAmemderInstancePtr, false> amendmentRequestQueue)
    : numOfHandler(numOfHandler), amendmentRequestQueue(amendmentRequestQueue) {}

void PlacementAmendmentHandler::start() {

    // Initiate amendment runners
    for (size_t i = 0; i < numOfHandler; ++i) {
        amendmentRunners.emplace_back(std::thread([this]() {
            handleRequest();
        }));
    }
}

void PlacementAmendmentHandler::handleRequest() {

    while (running) {
    }
}

void PlacementAmendmentHandler::shutDown() { running == false; }

}// namespace NES::Optimizer
