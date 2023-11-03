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

namespace NES::RequestProcessor::Experimental {

AbstractMultiRequest::AbstractMultiRequest(const std::vector<ResourceType>& requiredResources, const uint8_t maxRetries) : AbstractRequest(requiredResources, maxRetries) {}
std::vector<AbstractRequestPtr> AbstractMultiRequest::execute(const StorageHandlerPtr& storageHandle) {
    std::vector<AbstractRequestPtr> result;

    //let the first acquired thread execute the main thread and set the done memver afterwards
    bool expected = false;
    if (initialThreadAcquired.compare_exchange_strong(expected, true)) {
        //result = executeMainRequestLogic(storageHandle);
        result = AbstractRequest::execute(storageHandle);
        NES_ASSERT(subRequestQueue.empty(), "Multi request main logic terminated but sub request queue is not empty");
        done = true;
        cv.notify_all();
        return result;
    }

    while (!done) {
        result = executeSubRequest(storageHandle);
        if (!result.empty()) {
            break;
        }
    }
    return result;
}

bool AbstractMultiRequest::isDone() { return done; }

std::vector<AbstractRequestPtr> AbstractMultiRequest::executeSubRequest(const StorageHandlerPtr& storageHandle) {
    std::unique_lock lock(workMutex);
    while (subRequestQueue.empty()) {
        cv.wait(lock);
        if (done) {
            return {};
        }
    }
        AbstractSubRequestPtr subRequest = subRequestQueue.front();
        subRequestQueue.pop_front();
        lock.unlock();
        return subRequest->execute(storageHandle);
}

std::vector<AbstractRequestPtr> AbstractMultiRequest::executeSubRequestIfExists(const StorageHandlerPtr& storageHandle) {
    std::unique_lock lock(workMutex);
    if (subRequestQueue.empty()) {
        return {};
    }
    AbstractSubRequestPtr subRequest = subRequestQueue.front();
    subRequestQueue.pop_front();
    lock.unlock();

    return subRequest->execute(storageHandle);
}

std::future<std::any> AbstractMultiRequest::scheduleSubRequest(AbstractSubRequestPtr subRequest) {
    NES_ASSERT(!done, "Cannot schedule sub request is parent request is marked as done");
    auto future = subRequest->getFuture();
    subRequestQueue.emplace_back(std::move(subRequest));
    cv.notify_all();
    return future;
}
}// namespace NES::RequestProcessor::Experimental
