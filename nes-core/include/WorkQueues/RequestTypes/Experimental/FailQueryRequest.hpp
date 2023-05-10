#ifndef NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_EXPERIMENTAL_STOPQUERYREQUEST_HPP_
#define NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_EXPERIMENTAL_STOPQUERYREQUEST_HPP_
#include <WorkQueues/RequestTypes/AbstractRequest.hpp>
namespace NES {
class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;
using SharedQueryId = uint64_t;
using QuerySubPlanId = uint64_t;

namespace Experimental {
using QueryId = uint64_t;

class FailQueryRequest : public AbstractRequest {
  public:
    FailQueryRequest(unsigned long queryId, unsigned long failedSubPlanId, size_t maxRetries, WorkerRPCClientPtr workerRpcClient);

  protected:
    /**
     * @brief Performs request specific error handling to be done before changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    void preRollbackHandle(std::exception ex, StorageHandler& storageHandler) override;

    /**
     * @brief Roll back any changes made by a request that did not complete due to errors.
     * @param ex: The exception thrown during request execution.
     * @param storageHandle: The storage access handle that was used by the request to modify the system state.
     */
    void rollBack(std::exception& ex, StorageHandler& storageHandle) override;

    /**
     * @brief Performs request specific error handling to be done after changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    void postRollbackHandle(std::exception ex, StorageHandler& storageHandler) override;

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
}

}

#endif//NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_EXPERIMENTAL_STOPQUERYREQUEST_HPP_
