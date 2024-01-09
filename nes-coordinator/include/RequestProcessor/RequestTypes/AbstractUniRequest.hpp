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
#ifndef NES_ABSTRACTUNIREQUEST_HPP
#define NES_ABSTRACTUNIREQUEST_HPP
#include <RequestProcessor/RequestTypes/AbstractRequest.hpp>

namespace NES::RequestProcessor {

/**
 * @brief This class serves as the base class for all coordinator side request logic that is to executed serially
 * inside a single (uni) thread of execution.
 * Requests are queued into the async request processor and then picked up and executed by a thread
 */
class AbstractUniRequest : public AbstractRequest, public StorageResourceLocker {
  public:
    /**
     * @brief constructor
     * @param requiredResources: a list of resource types which indicates which resources will be accessed to execute
     * this request
     * @param maxRetries: amount of retries to execute the request after execution failed due to errors
     */
    explicit AbstractUniRequest(const std::vector<ResourceType>& requiredResources, uint8_t maxRetries);

    /**
     * @brief Acquires locks on the needed resources and executes the request logic
     * @param storageHandle: a handle to access the coordinators data structures which might be needed for executing the
     * request
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr> execute(const StorageHandlerPtr& storageHandle) final;

  protected:
    /**
     * @brief Executes the request logic.
     * @param storageHandle: a handle to access the coordinators data structures which might be needed for executing the
     * request
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    virtual std::vector<AbstractRequestPtr> executeRequestLogic(const StorageHandlerPtr& storageHandle) = 0;

    /**
     * @brief get an id that identifies this object to lock resources for exclusive use by this object
     * @return the id
     */
    RequestId getResourceLockingId() override;
};
}
#endif//NES_ABSTRACTUNIREQUEST_HPP
