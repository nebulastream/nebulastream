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

class FailQueryRequest : public AbstractRequest {
  public:
    /**
     * @brief Constructor
     * @param queryId: The id of the query that failed
     * @param failedSubPlanId: The id of the subplan that caused the failure
     * @param maxRetries: Maximum number of retry attempts for the request
     * @param workerRpcClient: The worker rpc client to be used during undeployment
     */
    FailQueryRequest(RequestId requestId, NES::QueryId queryId,
                     NES::QuerySubPlanId failedSubPlanId,
                     uint8_t maxRetries,
                     NES::WorkerRPCClientPtr workerRpcClient);

  protected:
    /**
     * @brief Performs request specific error handling to be done before changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    void preRollbackHandle(RequestExecutionException& ex, StorageHandler& storageHandler) override;

    /**
     * @brief Roll back any changes made by a request that did not complete due to errors.
     * @param ex: The exception thrown during request execution.
     * @param storageHandle: The storage access handle that was used by the request to modify the system state.
     */
    void rollBack(RequestExecutionException& ex, StorageHandler& storageHandle) override;

    /**
     * @brief Performs request specific error handling to be done after changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    void postRollbackHandle(RequestExecutionException& ex, StorageHandler& storageHandler) override;

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
