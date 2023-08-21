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
#ifndef NES_ASYNCREQUESTPROCESSOR_HPP
#define NES_ASYNCREQUESTPROCESSOR_HPP

#include <Common/Identifiers.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <RequestProcessor/RequestTypes/AbstractRequest.hpp>
#include <RequestProcessor/StorageHandles/SerialStorageHandler.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <RequestProcessor/StorageHandles/StorageHandlerType.hpp>
#include <RequestProcessor/StorageHandles/TwoPhaseLockingStorageHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <climits>
#include <deque>
#include <future>
#include <memory>
#include <numeric>
#include <vector>

namespace NES::RequestProcessor::Experimental {

static constexpr RequestId MAX_REQUEST_ID = INT_MAX;
class StorageHandler;
using AbstractRequestPtr = std::shared_ptr<AbstractRequest>;

/**
 * @brief This class asynchronously processes request to be executed by the coordinator. Requests can spawn new requests
 * if the logic requires follow up actions.
 */
class AsyncRequestProcessor {
    //define an empty request type that does nothing and is used only for flushing the executor
    class FlushRequest : public AbstractRequest {
      public:
        FlushRequest() : AbstractRequest({}, 0) {}
        std::vector<AbstractRequestPtr> executeRequestLogic(const StorageHandlerPtr&) override { return {}; }
        std::vector<AbstractRequestPtr> rollBack(RequestExecutionException&, const StorageHandlerPtr&) override { return {}; }

      protected:
        void preRollbackHandle(const RequestExecutionException&, const StorageHandlerPtr&) override {}
        void postRollbackHandle(const RequestExecutionException&, const StorageHandlerPtr&) override {}
        void postExecution(const StorageHandlerPtr&) override {}
    };

  public:
    /**
     * @brief constructor
     * @param storageDataStructures a struct containing pointers to the data structures on which the requests operate
     */
    AsyncRequestProcessor(const StorageDataStructures& storageDataStructures) : running(true), nextFreeRequestId(1) {

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

    /**
     * @brief Submits a request to the executor to be executed when a thread picks it up
     * @param request the request to execute
     * @return the id that was assigned to the request
     */
    RequestId runAsync(AbstractRequestPtr request) {
        if (!running) {
            NES_WARNING("Cannot execute request, Async request executor is not running");
            return INVALID_REQUEST_ID;
        }
        std::unique_lock lock(workMutex);
        auto requestId = nextFreeRequestId;
        request->setId(requestId);
        nextFreeRequestId = (nextFreeRequestId % MAX_REQUEST_ID) + 1;
        asyncRequestQueue.emplace_back(std::move(request));
        cv.notify_all();
        return requestId;
    }

    /**
     * @brief stop execution, clear queue and terminate threads. Only the first invocation has an effect. Subsequent
     * calls have no effect.
     * @return true if the executor was running and has been stopped
     */
    bool stop() {
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
                auto result =
                    std::accumulate(completionFutures.begin(), completionFutures.end(), true, [](bool acc, auto& future) {
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

    /**
     * @brief destructor, calls destroy()
     */
    ~AsyncRequestProcessor() { stop(); }

  private:
    /**
     * @brief The routine executed by each thread. Retrieves requests from the queue, executes them and inserts
     * follow up requests into the queue. This is repeated until the executor is shut down by calling destroy()
     */
    void runningRoutine() {
        while (true) {
            std::unique_lock lock(workMutex);
            while (asyncRequestQueue.empty()) {
                cv.wait(lock);
            }
            if (running) {
                AbstractRequestPtr abstractRequest = asyncRequestQueue.front();
                asyncRequestQueue.pop_front();
                lock.unlock();

                //execute request logic
                std::vector<AbstractRequestPtr> nextRequests = abstractRequest->execute(storageHandler);

                //queue follow up requests
                for (auto followUpRequest : nextRequests) {
                    runAsync(std::move(followUpRequest));
                }
            } else {
                break;
            }
        }
    }

    std::mutex workMutex;
    std::condition_variable cv;
    std::atomic<bool> running;
    std::vector<std::future<bool>> completionFutures;
    std::deque<AbstractRequestPtr> asyncRequestQueue;
    uint32_t numOfThreads;
    RequestId nextFreeRequestId;
    StorageHandlerPtr storageHandler;
};

using AsyncRequestProcessorPtr = std::shared_ptr<AsyncRequestProcessor>;
}// namespace NES::RequestProcessor::Experimental
#endif//NES_ASYNCREQUESTPROCESSOR_HPP
