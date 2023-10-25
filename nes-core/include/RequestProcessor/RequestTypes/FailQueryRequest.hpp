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
#ifndef NES_CORE_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_FAILQUERYREQUEST_HPP_
#define NES_CORE_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_FAILQUERYREQUEST_HPP_

#include <Identifiers.hpp>
#include <RequestProcessor/RequestTypes/AbstractRequest.hpp>

namespace NES {
class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

namespace RequestProcessor::Experimental {
class FailQueryRequest;
using FailQueryRequestPtr = std::shared_ptr<FailQueryRequest>;

//a response to the creator of the request
struct FailQueryResponse : public AbstractRequestResponse {
    explicit FailQueryResponse(SharedQueryId sharedQueryId) : sharedQueryId(sharedQueryId){};
    SharedQueryId sharedQueryId;
};

class FailQueryRequest : public AbstractRequest {
  public:
    /**
     * @brief Constructor
     * @param queryId: The id of the query that failed
     * @param failedSubPlanId: The id of the subplan that caused the failure
     * @param maxRetries: Maximum number of retry attempts for the request
     */
    FailQueryRequest(NES::QueryId queryId, NES::QuerySubPlanId failedSubPlanId, uint8_t maxRetries);

    /**
    * @brief creates a new FailQueryRequest object
    * @param queryId: The id of the query that failed
    * @param failedSubPlanId: The id of the subplan that caused the failure
    * @param maxRetries: Maximum number of retry attempts for the request
    * @return a smart pointer to the newly created object
    */
    static FailQueryRequestPtr create(NES::QueryId queryId, NES::QuerySubPlanId failedSubPlanId, uint8_t maxRetries);

  protected:
    /**
     * @brief Performs request specific error handling to be done before changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    void preRollbackHandle(const RequestExecutionException& ex, const StorageHandlerPtr& storageHandler) override;

    /**
     * @brief Roll back any changes made by a request that did not complete due to errors.
     * @param ex: The exception thrown during request execution.
     * @param storageHandle: The storage access handle that was used by the request to modify the system state.
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr> rollBack(RequestExecutionException& ex, const StorageHandlerPtr& storageHandle) override;

    /**
     * @brief Performs request specific error handling to be done after changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    void postRollbackHandle(const RequestExecutionException& ex, const StorageHandlerPtr& storageHandler) override;

    /**
     * @brief Performs steps to be done after execution of the request logic, e.g. unlocking the required data structures
     * @param storageHandle: The storage access handle used by the request
     * @param requiredResources: The resources required during the execution phase
     */
    void postExecution(const StorageHandlerPtr& storageHandler) override;

    /**
     * @brief Executes the request logic.
     * @param storageHandle: a handle to access the coordinators data structures which might be needed for executing the
     * request
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr> executeRequestLogic(const StorageHandlerPtr& storageHandler) override;

  private:
    QueryId queryId;
    QuerySubPlanId querySubPlanId;
    GlobalQueryPlanPtr globalQueryPlan;
    QueryCatalogServicePtr queryCatalogService;
    TopologyPtr topology;
    GlobalExecutionPlanPtr globalExecutionPlan;
};
}// namespace RequestProcessor::Experimental
}// namespace NES
#endif// NES_CORE_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_FAILQUERYREQUEST_HPP_
