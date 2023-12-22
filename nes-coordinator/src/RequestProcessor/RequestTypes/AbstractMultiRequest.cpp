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
#include <Exceptions/RequestExecutionException.hpp>
#include <Identifiers.hpp>
#include <RequestProcessor/RequestTypes/AbstractMultiRequest.hpp>
#include <RequestProcessor/RequestTypes/AbstractSubRequest.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::RequestProcessor {

AbstractMultiRequest::AbstractMultiRequest(const std::vector<ResourceType>& requiredResources, const uint8_t maxRetries)
    : AbstractRequest(requiredResources, maxRetries) {}

std::vector<AbstractRequestPtr> AbstractMultiRequest::execute(const StorageHandlerPtr& storageHandle) {
    std::vector<AbstractRequestPtr> result;

    //let the first acquired thread execute the main thread and set the done member variable afterwards
    bool expected = false;
    if (initialThreadAcquired.compare_exchange_strong(expected, true)) {
        this->storageHandle = storageHandle;
        result = executeRequestLogic();
        //NES_ASSERT(subRequestQueue.empty(), "Multi request main logic terminated but sub request queue is not empty");
        if (!subRequestQueue.empty()) {
            for (auto& subRequedt: subRequestQueue) {
                NES_ASSERT(subRequedt->executionHasStarted(), "Not all scheduled requests were executed");
            }
        }
        std::unique_lock lock(workMutex);
        done = true;
        lock.unlock();
        cv.notify_all();
        return result;
    }

    while (!done) {
        executeSubRequest();
    }
    return {};
}

bool AbstractMultiRequest::isDone() { return done; }

bool AbstractMultiRequest::executeSubRequest() {
    std::unique_lock lock(workMutex);
    while (subRequestQueue.empty()) {
        if (done) {
            return false;
        }
        cv.wait(lock);
    }
    const AbstractSubRequestPtr subRequest = subRequestQueue.front();
    subRequestQueue.pop_front();
    lock.unlock();
    subRequest->execute();
    return true;
}

void AbstractMultiRequest::executeSubRequestWhileQueueNotEmpty() {
    while (true) {
        std::unique_lock lock(workMutex);
        if (subRequestQueue.empty()) {
            break;
        }
        AbstractSubRequestPtr subRequest = subRequestQueue.front();
        subRequestQueue.pop_front();
        lock.unlock();

        subRequest->execute();
    }
}

SubRequestFuture AbstractMultiRequest::scheduleSubRequest(AbstractSubRequestPtr subRequest) {
    NES_ASSERT(!done, "Cannot schedule sub request is parent request is marked as done");
    std::promise<std::any> promise;
    auto future = promise.get_future();
    subRequest->setPromise(std::move(promise));

    subRequest->setStorageHandler(storageHandle);

    std::unique_lock lock(workMutex);
    subRequestQueue.emplace_back(subRequest);
    cv.notify_all();
    return SubRequestFuture(subRequest, std::move(future));

}
}// namespace NES::RequestProcessor::Experimental
