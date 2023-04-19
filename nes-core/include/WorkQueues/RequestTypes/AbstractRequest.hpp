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

#include <WorkQueues/StorageHandles/StorageHandler.hpp>
#include <WorkQueues/StorageHandles/StorageHandlerResourceType.hpp>
#include <exception>
#include <memory>
#include <vector>

namespace NES {

namespace Configurations {
class OptimizerConfiguration;
}

class StorageHandler;
using StorageHandlerPtr = std::shared_ptr<StorageHandler>;
class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;
class AbstractRequest;
using AbstractRequestPtr = std::shared_ptr<AbstractRequest>;

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

class AbstractRequest {
  public:
    explicit AbstractRequest(size_t maxRetries);

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
    virtual void rollBack(std::exception& ex, StorageHandler& storageHandle) = 0;

    /**
     * @brief Calls rollBack and executes additional error handling based on the exception if necessary
     * @param ex: The exception thrown during request execution.
     * @param storageHandle: The storage access handle that was used by the request to modify the system state.
     */
    void handleError(std::exception ex, StorageHandler& storageHandle);

    /**
     * @brief Check if the request has already reached the maximum allowed retry attempts or if it can be retried again. If the
     * maximum is not reached yet, increase the amount of recorded actual retries.
     * @return true if the actual retries are less than the allowed maximum
     */
    bool retry();

  protected:
    /**
     * @brief Performs request specific error handling to be done before changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    virtual void preRollbackHandle(std::exception ex, StorageHandler& storageHandle) = 0;

    /**
     * @brief Performs request specific error handling to be done after changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    virtual void postRollbackHandle(std::exception ex, StorageHandler& storageHandle) = 0;

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

  private:
    size_t maxRetries;
    size_t actualRetries;
    std::vector<StorageHandlerResourceType> requiredResources;
};
}// namespace NES
#endif//NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_ABSTRACTREQUEST_HPP_
