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

#include <memory>
#include <vector>
#include <future>

namespace NES {
namespace Exceptions {
class RequestExecutionException;
}
using Exceptions::RequestExecutionException;
using RequestId = uint64_t;
enum class ResourceType : uint8_t;

namespace Configurations {
class OptimizerConfiguration;
}

//the base class for the responses to be given to the creator of the request
struct AbstractRequestResponse {};
using AbstractRequestResponsePtr = std::shared_ptr<AbstractRequestResponse>;

//constrain template parameter to be a subclass of AbastractRequestResponse
//template <typename T>
//concept ConceptResponse = std::is_base_of<AbstractRequestResponse, T>::value;

class StorageHandler;
class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

template<ConceptResponse ResponseType>
class AbstractRequest;
using AbstractRequestPtr = std::shared_ptr<AbstractRequest<AbstractRequestResponse>>;

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
//template<ConceptResponse ResponseType>
class AbstractRequest;
using AbstractRequestPtr = std::shared_ptr<AbstractRequest>;

class AbstractRequest {
  public:
    /**
     * @brief constructor
     * @param requestId: the id of this request
     * @param requiredResources: as list of resource types which indicates which resources will be accessed t oexecute the request
     * @param maxRetries: amount of retries to execute the request after execution failed due to errors
     * @param responsePromise: a promise used to send responses to the client that initiated the creation of this request
     */
    explicit AbstractRequest(RequestId requestId, const std::vector<ResourceType>& requiredResources, uint8_t maxRetries, std::promise<AbstractRequestResponsePtr> responsePromise);

    /**
     * @brief Acquires locks on the needed resources and executes the request logic
     * @param storageHandle: a handle to access the coordinators data structures which might be needed for executing the
     * request
     */
    std::vector<AbstractRequestPtr> execute(StorageHandler& storageHandle);

    /**
     * @brief Roll back any changes made by a request that did not complete due to errors.
     * @param ex: The exception thrown during request execution.
     * @param storageHandle: The storage access handle that was used by the request to modify the system state.
     */
    virtual std::vector<AbstractRequestPtr> rollBack(const RequestExecutionException& ex, StorageHandler& storageHandle) = 0;

    /**
     * @brief Calls rollBack and executes additional error handling based on the exception if necessary
     * @param ex: The exception thrown during request execution.
     * @param storageHandle: The storage access handle that was used by the request to modify the system state.
     * @return A list of requests that should be called because of failure
     */
    std::vector<AbstractRequestPtr> handleError(const RequestExecutionException& ex, StorageHandler& storageHandle);

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

    /**
     * @brief Checks if this object is of type AbstractRequest
     * @tparam RequestType: a subclass ob AbstractRequest
     * @return bool true if object is of type AbstractRequest
     */
    template<class RequestType>
    bool instanceOf() {
        if (dynamic_cast<RequestType*>(this)) {
            return true;
        }
        return false;
    };

    /**
    * @brief Dynamically casts the exception to the given type
    * @tparam RequestType: a subclass ob AbstractRequest
    * @return returns a shared pointer of the given type
    */
    template<class RequestType>
    std::shared_ptr<RequestType> as() {
        if (instanceOf<RequestType>()) {
            return std::dynamic_pointer_cast<RequestType>(this->shared_from_this());
        }
        throw std::logic_error("Exception:: we performed an invalid cast of exception");
    }

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
    virtual std::vector<AbstractRequestPtr> executeRequestLogic(StorageHandler& storageHandle) = 0;

  protected:
    RequestId requestId;
    std::promise<AbstractRequestResponsePtr> responsePromise;

  private:
    uint8_t maxRetries;
    uint8_t actualRetries;
    std::vector<ResourceType> requiredResources;
};

}// namespace NES
#endif// NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_ABSTRACTREQUEST_HPP_
