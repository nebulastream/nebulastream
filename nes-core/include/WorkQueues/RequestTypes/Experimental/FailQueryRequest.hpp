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
#ifndef NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_EXPERIMENTAL_FAILQUERYREQUEST_HPP_
#define NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_EXPERIMENTAL_FAILQUERYREQUEST_HPP_
#include <Common/Identifiers.hpp>
#include <WorkQueues/RequestTypes/AbstractRequest.hpp>
namespace NES {
class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

namespace Experimental {

//a response to the creator of the request
struct FailQueryResponse : public AbstractRequestResponse {
    SharedQueryId sharedQueryId;
};

class FailQueryRequest : public AbstractRequest<FailQueryResponse> {
  public:
    /**
     * @brief Constructor
     * @param requestId: the id assigned to this request
     * @param queryId: The id of the query that failed
     * @param failedSubPlanId: The id of the subplan that caused the failure
     * @param maxRetries: Maximum number of retry attempts for the request
     * @param workerRpcClient: The worker rpc client to be used during undeployment
     * @param responsePromise: a promise used to send responses to the client that initiated the creation of this request
     */
    FailQueryRequest(RequestId requestId, NES::QueryId queryId,
                     NES::QuerySubPlanId failedSubPlanId,
                     uint8_t maxRetries,
                     NES::WorkerRPCClientPtr workerRpcClient, std::promise<FailQueryResponse> responsePromise);

    /**
    * @brief creates a new FailQueryRequest object
    * @param requestId: the id assigned to this request
    * @param queryId: The id of the query that failed
    * @param failedSubPlanId: The id of the subplan that caused the failure
    * @param maxRetries: Maximum number of retry attempts for the request
    * @param workerRpcClient: The worker rpc client to be used during undeployment
    * @param responsePromise: a promise used to send responses to the client that initiated the creation of this request
    * @return a smart pointer to the newly created object
    */
    static std::shared_ptr<FailQueryRequest> create(RequestId requestId, NES::QueryId queryId,
                     NES::QuerySubPlanId failedSubPlanId,
                     uint8_t maxRetries,
                     NES::WorkerRPCClientPtr workerRpcClient, std::promise<FailQueryResponse> responsePromise);

    /**
     * @brief Constructor to be used if no failed sub plan id can be provided
     * @param queryId: The id of the query that failed
     * @param maxRetries: Maximum number of retry attempts for the request
     * @param workerRpcClient: The worker rpc client to be used during undeployment
     */
    FailQueryRequest(QueryId queryId, uint8_t maxRetries, WorkerRPCClientPtr workerRpcClient);

  protected:
    /**
     * @brief Performs request specific error handling to be done before changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    void preRollbackHandle(const RequestExecutionException& ex, StorageHandler& storageHandler) override;

    /**
     * @brief Roll back any changes made by a request that did not complete due to errors.
     * @param ex: The exception thrown during request execution.
     * @param storageHandle: The storage access handle that was used by the request to modify the system state.
     */
    void rollBack(const RequestExecutionException& ex, StorageHandler& storageHandle) override;

    /**
     * @brief Performs request specific error handling to be done after changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    void postRollbackHandle(const RequestExecutionException& ex, StorageHandler& storageHandler) override;

    /**
     * @brief Performs steps to be done after execution of the request logic, e.g. unlocking the required data structures
     * @param storageHandle: The storage access handle used by the request
     * @param requiredResources: The resources required during the execution phase
     */
    void postExecution(StorageHandler& storageHandler) override;

    /**
     * @brief Executes the request logic.
     * @param storageHandle: a handle to access the coordinators data structures which might be needed for executing the
     * request
     */
    void executeRequestLogic(StorageHandler& storageHandler) override;

  private:
   /**
    * @brief stop and undeploy the query
    * @param storageHandler: a handle to access the coordinators data structures which might be needed for executing the
    * @param sharedQueryId: the shraed query id of the query to be undeployed
    */
    void undeployQueries(StorageHandler& storageHandler, SharedQueryId sharedQueryId);

    /**
    * @brief stop and undeploy the query
    * @param sharedQueryId: the shraed query id of the query to be undeployed
    */
    void postUndeployment(SharedQueryId sharedQueryId);
    QueryId queryId;
    QuerySubPlanId querySubPlanId;
    WorkerRPCClientPtr workerRpcClient;
    GlobalQueryPlanPtr globalQueryPlan;
    QueryCatalogServicePtr queryCatalogService;
    TopologyPtr topology;
    GlobalExecutionPlanPtr globalExecutionPlan;
};
}// namespace Experimental
}// namespace NES
#endif// NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_EXPERIMENTAL_FAILQUERYREQUEST_HPP_
