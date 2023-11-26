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
#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_ASYNCREQUESTPROCESSOR_HPP_
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_ASYNCREQUESTPROCESSOR_HPP_

#include <RequestProcessor/RequestTypes/AbstractRequest.hpp>
#include <deque>
#include <limits.h>

namespace NES::RequestProcessor {

struct StorageDataStructures;
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
        //request type uses exception_ptr so it can set the exception state on its response promise without casting or slicing
        std::vector<AbstractRequestPtr> rollBack(std::exception_ptr, const StorageHandlerPtr&) override { return {}; }

      protected:
        void preRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) override {}
        void postRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) override {}
        void postExecution(const StorageHandlerPtr&) override {}
    };

  public:
    /**
     * @brief constructor
     * @param storageDataStructures a struct containing pointers to the data structures on which the requests operate
     */
    AsyncRequestProcessor(const StorageDataStructures& storageDataStructures);

    /**
     * @brief Submits a request to the executor to be executed when a thread picks it up
     * @param request the request to execute
     * @return the id that was assigned to the request
     */
    RequestId runAsync(AbstractRequestPtr request);

    /**
     * @brief stop execution, clear queue and terminate threads. Only the first invocation has an effect. Subsequent
     * calls have no effect.
     * @return true if the executor was running and has been stopped
     */
    bool stop();

    /**
     * @brief destructor, calls destroy()
     */
    ~AsyncRequestProcessor();

  private:
    /**
     * @brief The routine executed by each thread. Retrieves requests from the queue, executes them and inserts
     * follow up requests into the queue. This is repeated until the executor is shut down by calling destroy()
     */
    void runningRoutine();

    std::mutex workMutex;
    std::condition_variable cv;
    std::atomic<bool> running;
    std::vector<std::future<bool>> completionFutures;
    std::deque<AbstractRequestPtr> asyncRequestQueue;
    uint32_t numOfThreads;
    StorageHandlerPtr storageHandler;
};

using AsyncRequestProcessorPtr = std::shared_ptr<AsyncRequestProcessor>;
}// namespace NES::RequestProcessor::Experimental
#endif// NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_ASYNCREQUESTPROCESSOR_HPP_
