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
#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ABSTRACTREQUEST_HPP_
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ABSTRACTREQUEST_HPP_

#include <RequestProcessor/RequestTypes/StorageResourceLocker.hpp>
#include <future>
#include <memory>
#include <vector>

namespace NES {
namespace Exceptions {
class RequestExecutionException;
}
using Exceptions::RequestExecutionException;

namespace Configurations {
class OptimizerConfiguration;
}

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

namespace RequestProcessor {

//the base class for the responses to be given to the creator of the request
struct AbstractRequestResponse {};
using AbstractRequestResponsePtr = std::shared_ptr<AbstractRequestResponse>;

/**
 * @brief is the abstract base class for any kind of coordinator side request to deploy or undeploy queries, change the topology or perform
 * other actions. Specific request types are implemented as subclasses of this request.
 */
class AbstractRequest;
using AbstractRequestPtr = std::shared_ptr<AbstractRequest>;

const uint8_t DEFAULT_RETRIES = 1;

class AbstractRequest : public std::enable_shared_from_this<AbstractRequest> {
  public:
    /**
     * @brief constructor
     * @param maxRetries: amount of retries to execute the request after execution failed due to errors
     */
    explicit AbstractRequest(uint8_t maxRetries);

    /**
     * @brief set the id of this object. This has to be done before any resource is locked.
     * @param requestId
     */
    void setId(RequestId requestId);

    /**
     * @brief Acquires locks on the needed resources and executes the request logic
     * @param storageHandle: a handle to access the coordinators data structures which might be needed for executing the
     * request
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    virtual std::vector<AbstractRequestPtr> execute(const StorageHandlerPtr& storageHandle) = 0;

    /**
     * @brief Roll back any changes made by a request that did not complete due to errors.
     * @param ex: The exception thrown during request execution. std::exception_ptr is used to be able to allow setting an
     * exception state on the requests response promise without losing data to slicing in case the request cannot handle the
     * exception itself
     * @param storageHandle: The storage access handle that was used by the request to modify the system state.
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    virtual std::vector<AbstractRequestPtr> rollBack(std::exception_ptr ex, const StorageHandlerPtr& storageHandle) = 0;

    /**
     * @brief Calls rollBack and executes additional error handling based on the exception if necessary.
     * @param ex: The exception thrown during request execution. std::exception_ptr is used to be able to allow setting an
     * exception state on the requests response promise without losing data to slicing in case the request cannot handle the
     * exception itself
     * @param storageHandle: The storage access handle that was used by the request to modify the system state.
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr> handleError(const std::exception_ptr& ex, const StorageHandlerPtr& storageHandle);

    /**
     * @brief Check if the request has already reached the maximum allowed retry attempts or if it can be retried again. If the
     * maximum is not reached yet, increase the amount of recorded actual retries.
     * @return true if the actual retries are less than the allowed maximum
     */
    bool retry();

    /**
     * @brief creates a future which will contain the response supplied by this request
     * @return a future containing a pointer to the response object
     */
    std::future<AbstractRequestResponsePtr> getFuture();

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
    //todo #4457: write unit test for this
    template<class RequestType>
    std::shared_ptr<RequestType> as() {
        if (instanceOf<RequestType>()) {
            return std::dynamic_pointer_cast<RequestType>(this->shared_from_this());
        }
        throw std::logic_error("Exception:: we performed an invalid cast of exception");
    }

    RequestId getId();

  protected:
    /**
     * @brief Performs request specific error handling to be done before changes to the storage are rolled back
     * @param ex: The exception thrown during request execution. std::exception_ptr is used to be able to allow setting an
     * exception state on the requests response promise without losing data to slicing in case the request cannot handle the
     * exception itself
     * @param storageHandle: The storage access handle used by the request
     */
    virtual void preRollbackHandle(std::exception_ptr ex, const StorageHandlerPtr& storageHandle) = 0;

    /**
     * @brief Performs request specific error handling to be done after changes to the storage are rolled back
     * @param ex: The exception thrown during request execution. std::exception_ptr is used to be able to allow setting an
     * exception state on the requests response promise without losing data to slicing in case the request cannot handle the
     * exception itself
     * @param storageHandle: The storage access handle used by the request
     */
    virtual void postRollbackHandle(std::exception_ptr ex, const StorageHandlerPtr& storageHandle) = 0;

    /**
     * @brief if no exception or value has been set in this requests response promise yet, set the supplied exception. If a value has
     * already been set, this method has no effect
     * @param exception the exception to give to the promise
     */
    void trySetExceptionInPromise(std::exception_ptr exception);

    /**
     * @brief set and exception on the response promise if no value or exception has been set yet. If the promise already has a
     * value or exception set, rethrow the exception in thsi thread
     * @param exception the exception to give to the promise
     */
    void setExceptionInPromiseOrRethrow(std::exception_ptr exception);

    RequestId requestId{INVALID_REQUEST_ID};
    std::promise<AbstractRequestResponsePtr> responsePromise;

  private:
    uint8_t maxRetries;
    uint8_t actualRetries{0};
};
}// namespace RequestProcessor
}// namespace NES
#endif// NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ABSTRACTREQUEST_HPP_
