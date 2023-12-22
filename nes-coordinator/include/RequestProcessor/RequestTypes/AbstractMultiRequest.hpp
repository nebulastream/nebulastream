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
 * A multi request can acquire multiple threads and has its own internal subrequest queue which it uses to schedule the
 * concurrent execution of of subrequests
 */
class AbstractMultiRequest : public std::enable_shared_from_this<AbstractMultiRequest>, public AbstractRequest {

  public:
    /**
     * @brief Constructor
     * @param requiredResources A list of the resource on which this request will acquire and hold a lock during the
     * entirety of its execution. Additional resources might be acquired by subrequests created by this request
     * @param maxRetries The maximum amount of retries in case of an error
     */
     //todo: do not pass resources
    AbstractMultiRequest(const std::vector<ResourceType>& requiredResources, uint8_t maxRetries);

    /**
     * @brief Executes the request logic. The first thread to execute this function for this request will be in charge
     * of scheduling tasks for the following threads
     * @param storageHandle: a handle to access the coordinators data structures which might be needed for executing the
     * request
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr> execute(const StorageHandlerPtr& storageHandle) override;

    /**
     * @brief Indicates if this request has finished its execution
     * @return true if all work is done and the request can be removed fro mthe queue and destroyed
     */
    bool isDone();

    /**
     * @brief Takes subrequests from the queue and executes them and returns when there is no more work to do. This
     * function has to be called by the main thread of a request before waiting on the return value of a subrequest, if
     * not, the thread might starve if the request processor is single threaded
     * @param storageHandle the storage handle used to lock and access resources
     */
     //todo: make this protected again and a fried function of subrequest future
    void executeSubRequestWhileQueueNotEmpty();

  protected:
    /**
     * @brief schedule a subrequest to be executed
     * @param subRequest the request to be executed
     * @param storageHandle the storage handle used to lock and access resources
     * @return a future into which the scheduled request will put the results of its computations
     */
    SubRequestFuture scheduleSubRequest(AbstractSubRequestPtr subRequest);

    virtual std::vector<AbstractRequestPtr> executeRequestLogic() = 0;

  private:
    /**
     * @brief Execute a subrequest. If the request queue is empty, this function will block until a request is scheduled
     * @param storageHandle the storage handle used to lock and access resources
     * @return true if a subrequest was executed. False is the request was marked as done and no subrequest was
     * executed
     */
    bool executeSubRequest();

    std::atomic<bool> done;
    std::atomic<bool> initialThreadAcquired = false;

    std::deque<AbstractSubRequestPtr> subRequestQueue;
    std::mutex workMutex;
    std::condition_variable cv;
    StorageHandlerPtr storageHandle;
};
}// namespace NES::RequestProcessor::Experimental
#endif// NES_CORE_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ABSTRACTMULTITHREADEDREQUEST_HPP_
