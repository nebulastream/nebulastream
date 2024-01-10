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
#ifndef NES_CORE_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ABSTRACTMULTITHREADEDREQUEST_HPP_
#define NES_CORE_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ABSTRACTMULTITHREADEDREQUEST_HPP_
#include <RequestProcessor/RequestTypes/AbstractRequest.hpp>
#include <any>
#include <deque>

namespace NES::RequestProcessor {
class AbstractSubRequest;
using AbstractSubRequestPtr = std::shared_ptr<AbstractSubRequest>;

class SubRequestFuture;

/**
 * A multi request can acquire multiple threads and has its own internal sub-request queue which it uses to schedule the
 * concurrent execution of of sub-requests
 */
class AbstractMultiRequest : public std::enable_shared_from_this<AbstractMultiRequest>, public AbstractRequest {

  public:
    /**
     * @brief Constructor
     * @param maxRetries The maximum amount of retries in case of an error
     */
    explicit AbstractMultiRequest(uint8_t maxRetries);

    /**
     * @brief Executes the request logic. The first thread to execute this function for this request will be in charge
     * of scheduling tasks for the following threads
     * @param storageHandle: a handle to access the coordinators data structures which might be needed for executing the
     * sub-requests
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr> execute(const StorageHandlerPtr& storageHandle) final;

    /**
     * @brief Indicates if this request has finished its execution
     * @return true if all work is done and the request can be removed from the queue and destroyed
     */
    bool isDone();

  protected:
    /**
     * @brief schedule a sub-request to be executed
     * @param subRequest the request to be executed
     * @return a wrapper around the future into which the scheduled request will put the results of its computations
     */
    SubRequestFuture scheduleSubRequest(AbstractSubRequestPtr subRequest);

    /**
     * @brief executes the logic of the main thread. sub-requests can be scheduled from withing this function. Access to
     * any data structure via the storage handler requires scheduling a sub-request which will make the resource access
     * @return a list of follow up requests to returned to the request executor (can be empty if no further actions are required)
     */
    virtual std::vector<AbstractRequestPtr> executeRequestLogic() = 0;

  private:
    /**
     * @brief Execute a sub-request. If the request queue is empty, this function will block until a request is scheduled
     * @param storageHandle the storage handle used to lock and access resources
     * @return true if a sub-request was executed. False if this request was already marked as done and no sub-request was
     * executed
     */
    bool executeSubRequest();

    std::atomic<bool> done = false;
    std::atomic<bool> initialThreadAcquired = false;

    std::deque<AbstractSubRequestPtr> subRequestQueue;
    std::mutex workMutex;
    std::condition_variable cv;
    StorageHandlerPtr storageHandle;
};
}// namespace NES::RequestProcessor
#endif// NES_CORE_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ABSTRACTMULTITHREADEDREQUEST_HPP_
