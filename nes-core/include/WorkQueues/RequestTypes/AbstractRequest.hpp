#ifndef NES_ABSTRACTREQUEST_HPP
#define NES_ABSTRACTREQUEST_HPP

#include <exception>
#include <memory>
#include <vector>
namespace NES {
class RequestQueue;
class StorageManagerHandle;
class AbstractRequest;
using CoordinatorActionRequestPtr = std::shared_ptr<AbstractRequest>;
class AbstractRequest {

  public:

    /**
     * another possible implementation that abstracts away the concurrency control
     * @param queue
     * @param storageManagerHandle
     * @return
     */
    virtual bool execute(RequestQueue queue, StorageManagerHandle storageManagerHandle);

    /**
     * roll back any changes made by an incomplete transaction
     * @param requestQueue
     * @param storageManagerHandle
     */
    virtual void rollBack(RequestQueue requestQueue, StorageManagerHandle storageManagerHandle) {
        if (storageManagerHandle->requireseRollback()) {
            //roll back changes
            //use info in exception to see where execution left of, so we know how much we have to roll back
        }
    }

    /**
     * calls rollBack and executes additional error handling based on the exception if necessary
     * @param ex
     * @param requestQueue
     * @param storageManagerHandle
     */
    void handleError(std::exception ex, RequestQueue requestQueue, StorageManagerHandle storageManagerHandle) {
        preRollbackErrorHandling(ex, requestQueue, storageManagerHandle);

        rollBack(requestQueue, storageManagerHandle);

        afterRollbackErrorHandling(ex, requestQueue, storageManagerHandle);

        queueRetry(requestQueue);
    }

    //todo: remove this
    /**
     * @brief executes the request content. Ressources will be acquired using 2 phase locking. on failure to acquiere necessary resources,
     * function will throw an exception.
     * @param queue
     * @param topology
     * @param queryCatalogService
     * @param sourceCatalog
     * @param globalQueryPlan
     * @param z3Context
     * @param optimizerConfiguration
     * @param udfCatalog
     * @return true if all operations succeded, false if some locks could not be acquired and action needs to be retried
     */
    /*
    void execute(RequestQueue queue, //for inserting new request if follow up action is desired
                 TopologyPtr topology,
                 QueryCatalogServicePtr queryCatalogService,
                 Catalogs::Source::SourceCatalogPtr sourceCatalog,
                 GlobalQueryPlanPtr globalQueryPlan,
                 z3::ContextPtr z3Context,
                 const Configurations::OptimizerConfiguration optimizerConfiguration,
                 Catalogs::UDF::UdfCatalogPtr udfCatalog);
                 */

  protected:
    /**
     * If follow up action is required, que the necessary requests
     * @param requestQueue
     * @return
     */
    bool queueFollowingRequests(RequestQueue requestQueue);

    /**
     * insert the same request again into the queue in order to retry it
     * @param requestQueue
     */
    void queueRetry(RequestQueue requestQueue);

    void preRollbackErrorHandling(Excecption ex, RequestQueue requestQueue, StorageManagerHandle storageManagerHandle);

    void afterRollbackErrorHandling(Excecption ex, RequestQueue requestQueue, StorageManagerHandle storageManagerHandle);

    std::vector<CoordinatorActionRequestPtr>  followUpRequests;
};
}
#endif//NES_ABSTRACTREQUEST_HPP
