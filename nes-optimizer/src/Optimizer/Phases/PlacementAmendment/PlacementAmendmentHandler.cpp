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

namespace NES::Optimizer
{
PlacementAmendmentHandler::PlacementAmendmentHandler(uint16_t numOfHandler) : running(false), numOfHandler(numOfHandler)
{
    placementAmendmentQueue = std::make_shared<folly::UMPMCQueue<Optimizer::PlacementAmendmentInstancePtr, false>>();
}

PlacementAmendmentHandler::~PlacementAmendmentHandler()
{
    NES_INFO("Called ~PlacementAmendmentHandler()");
    shutDown();
}

void PlacementAmendmentHandler::start()
{
    std::unique_lock lock(mutex);
    if (running)
    {
        NES_WARNING("Trying to start already running placement amendment handler. Skipping remaining operation.");
        return;
    }
    // Mark handler as running
    running = true;
    // Initiate amendment runners
    NES_INFO("Initializing placement amendment handler {}", numOfHandler);
    for (uint16_t i = 0; i < numOfHandler; i++)
    {
        amendmentRunners.emplace_back(std::thread([this]() { handleRequest(); }));
    }
}

void PlacementAmendmentHandler::shutDown()
{
    NES_INFO("Shutting down the placement amendment handler");
    std::unique_lock lock(mutex);
    running = false;
    lock.unlock();
    cv.notify_all();
    //Join all runners and wait them to be completed before returning the call
    for (auto& amendmentRunner : amendmentRunners)
    {
        if (amendmentRunner.joinable())
        {
            amendmentRunner.join();
        }
    }
    NES_INFO("Placement amendment handler shutdown completed !!!");
}

void PlacementAmendmentHandler::enqueueRequest(const NES::Optimizer::PlacementAmendmentInstancePtr& placementAmendmentInstance)
{
    // Enqueue the request to the multi-producer multi-consumer queue.
    placementAmendmentQueue->enqueue(placementAmendmentInstance);
    //Notify all waiting handlers that new work is available
    cv.notify_all();
}

void PlacementAmendmentHandler::handleRequest()
{
    NES_INFO("Initializing New Handler");
    while (running)
    {
        PlacementAmendmentInstancePtr placementAmendmentInstance;
        if (!placementAmendmentQueue->try_dequeue(placementAmendmentInstance))
        {
            std::unique_lock lock(mutex);
            // Note: we do not care about spurious starts as we use a concurrent queue and do not require to lock the mutex
            // to perform the read operation. This also simplifies this code.
            cv.wait(lock);
            continue;
        }
        placementAmendmentInstance->execute();
    }
    NES_ERROR("Exiting the thread is running {}", running);
}

} // namespace NES::Optimizer
