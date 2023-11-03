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

namespace NES::RequestProcessor::Experimental {
class AbstractSubRequest;
using AbstractSubRequestPtr = std::shared_ptr<AbstractSubRequest>;
class AbstractMultiRequest : public AbstractRequest {

  public:
    AbstractMultiRequest(const std::vector<ResourceType>& requiredResources, const uint8_t maxRetries);
    //todo: adjust function doc
    /**
     * @brief Executes the request logic.
     * @param storageHandle: a handle to access the coordinators data structures which might be needed for executing the
     * request
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr> execute(const StorageHandlerPtr& storageHandle) override;

    bool isDone();

protected:
    //virtual std::vector<AbstractRequestPtr> executeMainRequestLogic(const StorageHandlerPtr& storageHandle) = 0;
    std::vector<AbstractRequestPtr> executeSubRequestIfExists(const StorageHandlerPtr& storageHandle);
    std::future<std::any> scheduleSubRequest(AbstractSubRequestPtr subRequest);

  private:
    std::vector<AbstractRequestPtr> executeSubRequest(const StorageHandlerPtr& storageHandle);
    std::atomic<bool> done;
    std::atomic<bool> initialThreadAcquired = false;

    std::deque<AbstractSubRequestPtr> subRequestQueue;
    std::mutex workMutex;
    std::condition_variable cv;
};
}
#endif// NES_CORE_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ABSTRACTMULTITHREADEDREQUEST_HPP_
