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

#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Enums/StorageHandlerType.hpp>
#include <Identifiers/Identifiers.hpp>
#include <RequestProcessor/AsyncRequestProcessor.hpp>
#include <RequestProcessor/RequestTypes/AbstractMultiRequest.hpp>
#include <RequestProcessor/StorageHandles/SerialStorageHandler.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <RequestProcessor/StorageHandles/TwoPhaseLockingStorageHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <deque>
#include <numeric>

namespace NES::RequestProcessor {

AsyncRequestProcessor::AsyncRequestProcessor(StorageDataStructures& storageDataStructures) : running(true) {

    numOfThreads = storageDataStructures.coordinatorConfiguration->requestExecutorThreads.getValue();
    auto storageHandlerType = storageDataStructures.coordinatorConfiguration->storageHandlerType.getValue();
    switch (storageHandlerType) {
        case StorageHandlerType::SerialHandler: storageHandler = SerialStorageHandler::create(storageDataStructures); break;
        case StorageHandlerType::TwoPhaseLocking:
            storageHandler = TwoPhaseLockingStorageHandler::create(storageDataStructures);
            break;
        default: NES_THROW_RUNTIME_ERROR("Unknown storage type supplied. Failed to initialize request executor.");
    }

    for (uint32_t i = 0; i < numOfThreads; ++i) {
        std::promise<bool> promise;
        completionFutures.emplace_back(promise.get_future());
        auto thread = std::thread([this, i, promise = std::move(promise)]() mutable {
            try {
                setThreadName("RequestExecThr-%d", i);
                runningRoutine();
                promise.set_value(true);
            } catch (const std::exception& ex) {
                promise.set_exception(std::make_exception_ptr(ex));
            }
        });
        thread.detach();
    }
}

RequestId AsyncRequestProcessor::runAsync(AbstractRequestPtr request) {
    NES_ERROR("Queueing request in executor");
    if (!running) {
        NES_WARNING("Cannot execute request, Async request executor is not running");
        return INVALID_REQUEST_ID;
    }
    NES_ERROR("Generating request id");
    RequestId requestId = storageHandler->generateRequestId();
    request->setId(requestId);
    {
        NES_ERROR("Getting lock and emplacing request in queue");
        std::unique_lock lock(workMutex);
        asyncRequestQueue.emplace_back(std::move(request));
        NES_ERROR("Request added to queue. {} requests in queue", asyncRequestQueue.size());
    }
    cv.notify_all();
    return requestId;
}

bool AsyncRequestProcessor::stop() {
    bool expected = true;
    if (running.compare_exchange_strong(expected, false)) {
        try {
            {
                std::unique_lock lock(workMutex);
                for (uint32_t i = 0; i < numOfThreads; ++i) {
                    asyncRequestQueue.emplace_back(std::make_shared<FlushRequest>());
                }
                cv.notify_all();
            }
            auto result = std::accumulate(completionFutures.begin(), completionFutures.end(), true, [](bool acc, auto& future) {
                return acc && future.get();
            });
            NES_ASSERT(result, "Cannot shut down AsyncRequestExecutor");
            {
                std::unique_lock lock(workMutex);
                asyncRequestQueue.clear();
            }
            return result;
        } catch (std::exception const& ex) {
            NES_ASSERT(false, "Cannot shut down AsyncRequestExecutor");
        }
    }
    return false;
}

void AsyncRequestProcessor::runningRoutine() {
    while (true) {
        std::unique_lock lock(workMutex);
        while (asyncRequestQueue.empty()) {
            NES_ERROR("Async request queue is empty, waiting for requests")
            cv.wait(lock);
        }
        if (running) {
            NES_ERROR("Queue not empty, executing request")
            AbstractRequestPtr abstractRequest = asyncRequestQueue.front();

            auto multiRequest = std::dynamic_pointer_cast<AbstractMultiRequest>(abstractRequest);
            if (multiRequest) {
                // remove the multi request from the queue only if it is done,
                // otherwise leave it at the front of the queue and acquire more threads for concurrent execution
                if (multiRequest->isDone()) {
                    asyncRequestQueue.pop_front();
                    lock.unlock();
                    cv.notify_all();
                    NES_ERROR("Multi request removed from queue. {} remain requests in queue", asyncRequestQueue.size());
                    continue;
                }
                NES_ERROR("Multi request selected in queue, request will remaing in queue. {} remain requests in queue", asyncRequestQueue.size());
            } else {
                // if the request is not a multirequest it will not have to acquire more threads than the current one.
                // it can be removed from the queue
                asyncRequestQueue.pop_front();
                NES_ERROR("Uni request retrieved from queue. {} remain requests in queue", asyncRequestQueue.size());
            }

            lock.unlock();
            cv.notify_all();

            NES_ERROR("Executing request {}", abstractRequest->getId())
            //execute request logic
            std::vector<AbstractRequestPtr> nextRequests = abstractRequest->execute(storageHandler);
            NES_ERROR("Finished Executing request {}, {} follow up requests", abstractRequest->getId(), nextRequests.size())

            //queue follow up requests
            for (auto followUpRequest : nextRequests) {
                runAsync(std::move(followUpRequest));
            }
            NES_DEBUG("Follow up requests queued")
        } else {
            cv.notify_all();
            break;
        }
    }
}

AsyncRequestProcessor::~AsyncRequestProcessor() { stop(); }

}// namespace NES::RequestProcessor
