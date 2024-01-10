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
#ifndef NES_ABSTRACTSUBREQUEST_HPP
#define NES_ABSTRACTSUBREQUEST_HPP
#include <RequestProcessor/RequestTypes/StorageResourceLocker.hpp>
#include <any>
#include <future>
#include <memory>
#include <vector>

namespace NES::RequestProcessor {
class AbstractRequest;
using AbstractRequestPtr = std::shared_ptr<AbstractRequest>;

class AbstractMultiRequest;
using AbstractMultiRequestPtr = std::shared_ptr<AbstractMultiRequest>;

class AbstractSubRequest;
using AbstractSubRequestPtr = std::shared_ptr<AbstractSubRequest>;

class StorageHandler;
using StorageHandlerPtr = std::shared_ptr<StorageHandler>;

/**
 * This class encapsulates parts of a coordinator side requests logic that are to be executed concurrently.
 * Subrequests are scheduled and executed as part of the execution of a MultiRequest
 */
class AbstractSubRequest : public StorageResourceLocker {
  public:
    /**
     * @brief Constructor
     * @param requiredResources the resources required to be locked by this request.
     */
    explicit AbstractSubRequest(std::vector<ResourceType> requiredResources);

    /**
     * @brief destructor
     */
    virtual ~AbstractSubRequest() = default;

    /**
     * @brief lock resources, execute this sub request's logic and release resources
     */
    //todo #4433: move to common base class with abstract request
    bool execute();

    /**
     * @brief obtain a future into which the results of this sub request's execution will be placed on completion
     * @return a future containing the results
     */
    std::future<std::any> getFuture();

    /**
     * @brief set the storage handler to be used for resource access by this sub request
     * @param storageHandler a pointer to the storage handler
     */
    void setStorageHandler(StorageHandlerPtr storageHandler);

    /**
     * @brief check if this request has already been picked up by a thread to execute it
     * @return true if execution has started or already finished, false otherwise
     */
    bool executionHasStarted();

  protected:
    /**
     * @brief Execute this sub request's logic
     * @param storageHandler the storage handler to be used for resource access
     */
    virtual std::any executeSubRequestLogic(const StorageHandlerPtr& storageHandler) = 0;

    /**
     * @brief get an id that identifies this request to lock resources for exclusive use by this object
     * @return the id
     */
    RequestId getResourceLockingId() override;

    std::promise<std::any> responsePromise;
    StorageHandlerPtr storageHandler;
    std::atomic<bool> executionStarted{false};
    RequestId requestId{INVALID_REQUEST_ID};
};
}// namespace NES::RequestProcessor

#endif//NES_ABSTRACTSUBREQUEST_HPP
