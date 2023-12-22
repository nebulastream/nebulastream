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
#include <any>
#include <future>
#include <memory>
#include <vector>
#include <RequestProcessor/RequestTypes/StorageResourceLocker.hpp>

namespace NES::RequestProcessor {
class AbstractRequest;
using AbstractRequestPtr = std::shared_ptr<AbstractRequest>;

class AbstractMultiRequest;
using AbstractMultiRequestPtr = std::shared_ptr<AbstractMultiRequest>;

class AbstractSubRequest;
using AbstractSubRequestPtr = std::shared_ptr<AbstractSubRequest>;

class StorageHandler;
using StorageHandlerPtr = std::shared_ptr<StorageHandler>;

class SubRequestFuture {
  public:
    explicit SubRequestFuture(AbstractSubRequestPtr parentRequest, std::future<std::any> future);
    std::any get();
  private:
    AbstractSubRequestPtr request;
    std::future<std::any> future;
};
/**
 * This class encapsulates parts of a coordinator side requests logic that are to be executed concurrently.
 * Subrequests are scheduled and executed as part of the execution of a MultiRequest
 */
class AbstractSubRequest : public StorageResourceLocker {
  public:
    /**
     * @brief Constructor
     * @param requiredResources the resources required by this request. The parent request has to ensure that it does
     * not create subrequests that try to lock resources that have already been locked by the parent request
     */
    explicit AbstractSubRequest(std::vector<ResourceType> requiredResources);

    /**
     * @brief destructor
     */
    virtual ~AbstractSubRequest() = default;

    /**
     * @brief lock resources, execute this subrequests logic and release resources
     * @param requiredResources the resources required by this request. Must not be already locked by the parent request.
     */
    //todo #4433: move to common base class with abstract request
    bool execute();

    /**
     * @brief obtain a future into which the results of this subrequests execution will be placed on completion
     * @return a future containing the results
     */
    //std::future<std::any> getFuture();

    void setPromise(std::promise<std::any> promise);

    void setStorageHandler(StorageHandlerPtr storageHandler);

    bool executionHasStarted();
  protected:
    /**
     * @brief Execute this subrequests logic
     * @param storageHandler
     */
    virtual std::any executeSubRequestLogic(const StorageHandlerPtr& storageHandler) = 0;

    std::promise<std::any> responsePromise;
    StorageHandlerPtr storageHandler;
    std::atomic<bool> executionStarted{false};
};
}// namespace NES::RequestProcessor::Experimental

#endif//NES_ABSTRACTSUBREQUEST_HPP
