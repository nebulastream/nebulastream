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
#ifndef NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_ABSTRACTREQUEST_HPP_
#define NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_ABSTRACTREQUEST_HPP_

#include <Exceptions/RequestExecutionException.hpp>
#include <WorkQueues/StorageHandles/ResourceType.hpp>
#include <WorkQueues/StorageHandles/StorageHandler.hpp>
#include <memory>
#include <vector>
#include <future>

namespace NES {
using Exceptions::RequestExecutionException;

namespace Configurations {
class OptimizerConfiguration;
}

//the base class for the responses to be given to the creator of the request
struct AbstractRequestResponse {};

//constrain template parameter to be a subclass of AbastractRequestResponse
template <typename T>
concept ConceptResponse = std::is_base_of<AbstractRequestResponse, T>::value;

class StorageHandler;
class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

/**
 * @brief is the abstract base class for any kind of coordinator side request to deploy or undeploy queries, change the topology or perform
 * other actions. Specific request types are implemented as subclasses of this request.
 */

/*
 * A serial execution loop in the request executor will look like this:
 * for (auto request : requests) {
 *     try {
 *         request->execute(storageAccessHandle);
 *     } catch (std::exception& ex) {
 *         request->handleError(ex, storageAccessHandle);
 *         if (request->retry()) {
 *             //queue again
 *          }
 *      }
 * }
 */
template<ConceptResponse ResponseType>
class AbstractRequest {
  public:
    /**
     * @brief constructor
     * @param requestId: the id of this request
     * @param requiredResources: as list of resource types which indicates which resources will be accessed t oexecute the request
     * @param maxRetries: amount of retries to execute the request after execution failed due to errors
     * @param responsePromise: a promise used to send responses to the client that initiated the creation of this request
     */
    explicit AbstractRequest(RequestId requestId, const std::vector<ResourceType>& requiredResources, uint8_t maxRetries, std::promise<ResponseType> responsePromise);

    /**
     * @brief Acquires locks on the needed resources and executes the request logic
     * @param storageHandle: a handle to access the coordinators data structures which might be needed for executing the
     * request
     */
    void execute(StorageHandler& storageHandle);

    /**
     * @brief Roll back any changes made by a request that did not complete due to errors.
     * @param ex: The exception thrown during request execution.
     * @param storageHandle: The storage access handle that was used by the request to modify the system state.
     */
    virtual void rollBack(const RequestExecutionException& ex, StorageHandler& storageHandle) = 0;

    /**
     * @brief Calls rollBack and executes additional error handling based on the exception if necessary
     * @param ex: The exception thrown during request execution.
     * @param storageHandle: The storage access handle that was used by the request to modify the system state.
     */
    void handleError(const RequestExecutionException& ex, StorageHandler& storageHandle);

    /**
     * @brief Check if the request has already reached the maximum allowed retry attempts or if it can be retried again. If the
     * maximum is not reached yet, increase the amount of recorded actual retries.
     * @return true if the actual retries are less than the allowed maximum
     */
    bool retry();

    /**
     * @brief destructor
     */
    virtual ~AbstractRequest() = default;

  protected:
    /**
     * @brief Performs request specific error handling to be done before changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    virtual void preRollbackHandle(const RequestExecutionException& ex, StorageHandler& storageHandle) = 0;

    /**
     * @brief Performs request specific error handling to be done after changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    virtual void postRollbackHandle(const RequestExecutionException& ex, StorageHandler& storageHandle) = 0;

    /**
     * @brief Performs steps to be done before execution of the request logic, e.g. locking the required data structures
     * @param storageHandle: The storage access handle used by the request
     */
    virtual void preExecution(StorageHandler& storageHandle);

    /**
     * @brief Performs steps to be done after execution of the request logic, e.g. unlocking the required data structures
     * @param storageHandle: The storage access handle used by the request
     * @param requiredResources: The resources required during the execution phase
     */
    virtual void postExecution(StorageHandler& storageHandle) = 0;

    /**
     * @brief Executes the request logic.
     * @param storageHandle: a handle to access the coordinators data structures which might be needed for executing the
     * request
     */
    virtual void executeRequestLogic(StorageHandler& storageHandle) = 0;

  protected:
    RequestId requestId;
    std::promise<ResponseType> responsePromise;

  private:
    uint8_t maxRetries;
    uint8_t actualRetries;
    std::vector<ResourceType> requiredResources;
};

template<ConceptResponse ResponseType>
AbstractRequest<ResponseType>::AbstractRequest(RequestId requestId,
                                               const std::vector<ResourceType>& requiredResources,
                                               const uint8_t maxRetries,
                                               std::promise<ResponseType> responsePromise)
    : requestId(requestId), responsePromise(std::move(responsePromise)),
      maxRetries(maxRetries),
      actualRetries(0),
      requiredResources(requiredResources) {}

template<ConceptResponse ResponseType>
void AbstractRequest<ResponseType>::handleError(const RequestExecutionException& ex, StorageHandler& storageHandle) {
    //error handling to be performed before rolling back
    preRollbackHandle(ex, storageHandle);

    //roll back the changes made by the failed request
    rollBack(ex, storageHandle);

    //error handling to be performed after rolling back
    postRollbackHandle(ex, storageHandle);
}

template<ConceptResponse ResponseType>
bool AbstractRequest<ResponseType>::retry() { return actualRetries++ < maxRetries; }

template<ConceptResponse ResponseType>
void AbstractRequest<ResponseType>::execute(StorageHandler& storageHandle) {
    //acquire locks and perform other tasks to prepare for execution
    preExecution(storageHandle);

    //execute the request logic
    executeRequestLogic(storageHandle);

    //release locks
    postExecution(storageHandle);
}

template<ConceptResponse ResponseType>
void AbstractRequest<ResponseType>::preExecution(StorageHandler& storageHandle) {
    storageHandle.acquireResources(requestId, requiredResources);
}
}// namespace NES
#endif// NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_ABSTRACTREQUEST_HPP_
