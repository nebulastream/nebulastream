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
#include <deque>
#include <future>
#include <memory>
#include <vector>
#include <WorkQueues/RequestTypes/AbstractRequest.hpp>
#include <Util/ThreadNaming.hpp>

namespace NES {
class StorageHandler;
namespace Experimental {
    //class AbstractRequest;
    using AbstractRequestPtr = std::shared_ptr<AbstractRequest>;

    template <typename T>
    concept ConceptStorageHandler = std::is_base_of<StorageHandler, T>::value;

    //using StorageHandlerPtr = std::shared_ptr<T>;

    template <ConceptStorageHandler T>
    class AsyncRequestExecutor {
        //AsyncRequestExecutor(uint32_t numOfThreads, StorageHandlerPtr storageHandler);
        AsyncRequestExecutor(uint32_t numOfThreads, T storageHandler);
        std::future<AbstractRequestResponsePtr> runAsync(AbstractRequestPtr request);

      private:
        /// the inner thread routine that executes async tasks
        void runningRoutine();

        std::mutex workMutex;
        std::condition_variable cv;
        std::atomic<bool> running;
        std::vector<std::future<bool>> completionFutures;
        std::deque<AbstractRequestPtr> asyncRequestQueue;
        //StorageHandlerPtr storageHandler;
        T storageHandler;
        //todo: would we need that?
        //std::vector<std::shared_ptr<std::thread>> runningThreads;
    };

    template <ConceptStorageHandler T>
    NES::Experimental::AsyncRequestExecutor<T>::AsyncRequestExecutor(uint32_t numOfThreads, T storageHandler) : running(true), storageHandler(std::move(storageHandler)) {
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

                //todo: call error handling inside execute or here?

                //todo: make execute function accept ref to const ptr instead of the ref here?
                //todo: to avaid the abstract type trouble, we either need to use a template or make execute accept pointers

                //todo: add return type
                //todo: queue new functions
                std::vector<AbstractRequestPtr> nextRequests = abstractRequest->execute(storageHandler);


                //todo: allow execute to return follow up reqeust or execute the follow up as part of execute?
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
    {
            std::unique_lock lock(workMutex);
            asyncRequestQueue.emplace_back(std::move(request));
            cv.notify_all();
    }
}
    template <typename T>
    using AsyncRequestExecutorPtr = std::shared_ptr<AsyncRequestExecutor<T>>;
}
}
#endif//NES_ASYNCREQUESTEXECUTOR_HPP
