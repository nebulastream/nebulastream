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
#ifndef NES_ASYNCREQUESTEXECUTOR_HPP
#define NES_ASYNCREQUESTEXECUTOR_HPP
#include <Util/ThreadNaming.hpp>
#include <WorkQueues/RequestTypes/AbstractRequest.hpp>
#include <WorkQueues/StorageHandles/StorageDataStructures.hpp>
#include <deque>
#include <future>
#include <memory>
#include <numeric>
#include <vector>

namespace NES {
constexpr uint64_t INVALID_REQUEST_ID = 0;
class StorageHandler;
namespace Experimental {
    //class AbstractRequest;
    using AbstractRequestPtr = std::shared_ptr<AbstractRequest>;

    template <typename T>
    concept ConceptStorageHandler = std::is_base_of<StorageHandler, T>::value;

    //using StorageHandlerPtr = std::shared_ptr<T>;

    template <ConceptStorageHandler T>
    class AsyncRequestExecutor {

        //define an empty request type that does nothing and is used only for flushing the executor
        class FlushRequest : public AbstractRequest {
          public:
            FlushRequest() : AbstractRequest(INVALID_REQUEST_ID, {}, 0, {}) {}
            std::vector<AbstractRequestPtr> executeRequestLogic(NES::StorageHandler&) override { return {}; }
            std::vector<AbstractRequestPtr> rollBack(const RequestExecutionException&, StorageHandler&) override { return {}; }
          protected:
            void preRollbackHandle(const RequestExecutionException&, StorageHandler&) override {}
            void postRollbackHandle(const RequestExecutionException&, StorageHandler&) override {}
            void postExecution(StorageHandler&) override {}
        };

      public:
        //AsyncRequestExecutor(uint32_t numOfThreads, StorageHandlerPtr storageHandler);
        AsyncRequestExecutor(uint32_t numOfThreads, StorageDataStructures storageDataStructures);
        std::future<AbstractRequestResponsePtr> runAsync(AbstractRequestPtr request);
        bool destroy();
        ~AsyncRequestExecutor();

      private:
        /// the inner thread routine that executes async tasks
        void runningRoutine();

        std::mutex workMutex;
        std::condition_variable cv;
        std::atomic<bool> running;
        std::vector<std::future<bool>> completionFutures;
        std::deque<AbstractRequestPtr> asyncRequestQueue;
        uint32_t numOfThreads;
        //StorageHandlerPtr storageHandler;
        T storageHandler;
        //todo: would we need that?
        //std::vector<std::shared_ptr<std::thread>> runningThreads;
    };

    template <ConceptStorageHandler T>
    NES::Experimental::AsyncRequestExecutor<T>::AsyncRequestExecutor(uint32_t numOfThreads, StorageDataStructures storageDataStructures) : running(true), numOfThreads(numOfThreads), storageHandler(T(storageDataStructures)) {
        for (uint32_t i = 0; i < numOfThreads; ++i) {
            std::promise<bool> promise;
            completionFutures.emplace_back(promise.get_future());
            //todo: is there a reason why the task implementation was passing ptrs to promises?
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


    template <ConceptStorageHandler T>
    void NES::Experimental::AsyncRequestExecutor<T>::runningRoutine() {
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

template <ConceptStorageHandler T>
std::future<AbstractRequestResponsePtr> NES::Experimental::AsyncRequestExecutor<T>::runAsync(AbstractRequestPtr request) {
    if (!running) {
            NES_THROW_RUNTIME_ERROR("Cannot execute request, Async request executor is not runnign");
    }
    auto future = request->makeFuture();
    {
            std::unique_lock lock(workMutex);
            asyncRequestQueue.emplace_back(std::move(request));
            cv.notify_all();
    }
    return future;
}

template <ConceptStorageHandler T>
bool NES::Experimental::AsyncRequestExecutor<T>::destroy() {
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
template <ConceptStorageHandler T>
NES::Experimental::AsyncRequestExecutor<T>::~AsyncRequestExecutor() {
    destroy();
}

    template <typename T>
    using AsyncRequestExecutorPtr = std::shared_ptr<AsyncRequestExecutor<T>>;
}
}
#endif//NES_ASYNCREQUESTEXECUTOR_HPP
