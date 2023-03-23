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
#ifndef NES_ABSTRACTREQUEST_HPP
#define NES_ABSTRACTREQUEST_HPP

#include <exception>
#include <memory>
#include <vector>
#include <WorkQueues/StorageHandles/ResourceType.hpp>

namespace NES {

namespace Configurations {
class OptimizerConfiguration;
}

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;
class AbstractRequest;
using AbstractRequestPtr = std::shared_ptr<AbstractRequest>;

/**
 * This is the abstract base class for any kind of coordinator side request to deploy or undeploy queries, change the topology or perform
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
    explicit AbstractRequest(size_t maxRetries, std::vector<ResourceType> requiredResources);
    /**
     * Acquires locks on the need resources and executes the request logic
     * @param storageHandle: a handle to access the coordinators data structures which might be needed for executing the
     * request
     */
    virtual void execute(const StorageAccessHandlePtr& storageHandle);

    /**
     * Roll back any changes made by a request that did not complete due to errors.
     * @param ex: The exception thrown during request execution.
     * @param storageAccessHandle: The storage access handle that was used by the request to modify the system state.
     */
    virtual void rollBack(std::exception& ex, StorageAccessHandlePtr storageAccessHandle) = 0;

    /**
     * Calls rollBack and executes additional error handling based on the exception if necessary
     * @param ex: The exception thrown during request execution.
     * @param storageHandle: The storage access handle that was used by the request to modify the system state.
     */
    void handleError(std::exception ex, const StorageAccessHandlePtr& storageHandle);

    /**
     * Check if the request has already reached the maximum allowed retry attempts or if it can be retried again. If the
     * maximum is not reached yet, increase the amount of recorded actual retries.
     * @return true if the actual retries are less than the allowed maximum
     */
    bool retry();

  protected:
    /**
     * Performs request specific error handling to be done before changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageAccessHandle: The storage access handle used by the request
     */
    virtual void preRollbackHandle(std::exception ex, StorageAccessHandlePtr storageAccessHandle) = 0;

    /**
     * Performs request specific error handling to be done after changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageAccessHandle: The storage access handle used by the request
     * @param workerRpcClient: the rpc client to communicate with the worker nodes
     */
    virtual void postRollbackHandle(std::exception ex, StorageAccessHandlePtr storageAccessHandle) = 0;

    /**
     * Performs steps to be done before execution of the request logic, e.g. locking the required data structures
     * @param requiredResources: The resources required during the execution phase
     */
    virtual void preExecution(StorageAccessHandlePtr storageAccessHandle, std::vector<ResourceType> requiredResources) = 0;

    /**
     * Performs steps to be done after execution of the request logic, e.g. unlocking the required data structures
     * @param requiredResources: The resources required during the execution phase
     */
    virtual void postExecution(StorageAccessHandlePtr storageAccessHandle, std::vector<ResourceType> requiredResources) = 0;

    /**
     * Executes the request logic.
     * @param storageAccessHandle: a handle to access the coordinators data structures which might be needed for executing the
     * request
     */
    virtual void executeRequestLogic(StorageAccessHandlePtr storageAccessHandle) = 0;


  private:
    size_t maxRetries;
    size_t actualRetries;
    std::vector<ResourceType> requiredResources;
};
}
#endif//NES_ABSTRACTREQUEST_HPP
