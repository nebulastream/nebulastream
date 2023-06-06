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
#ifndef NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_TWOPHASELOCKINGSTORAGEHANDLER_HPP_
#define NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_TWOPHASELOCKINGSTORAGEHANDLER_HPP_

#include <WorkQueues/StorageHandles/ResourceType.hpp>
#include <WorkQueues/StorageHandles/StorageHandler.hpp>
#include <atomic>
#include <vector>

namespace NES {
class LockManager;
using TwoPhaseLockManagerPtr = std::shared_ptr<LockManager>;

/**
 * @brief Resource handles created by this class ensure that the resource has been locked in the growing phase and stay locked
 * until the handle goes out of scope.
 */
class TwoPhaseLockingStorageHandler : public StorageHandler {
  public:
    /**
     * @brief Constructor
     * @param lockManager a pointer to the lock manager which maintains the mutexes for all of the above data structures
     */
    explicit TwoPhaseLockingStorageHandler(GlobalExecutionPlanPtr globalExecutionPlan,
                         TopologyPtr topology,
                         QueryCatalogServicePtr queryCatalogService,
                         GlobalQueryPlanPtr globalQueryPlan,
                         Catalogs::Source::SourceCatalogPtr sourceCatalog,
                         Catalogs::UDF::UDFCatalogPtr udfCatalog);

    /**
     * @brief factory to create a two phase locking storage manager object
     * @return shared pointer to the two phase locking storage manager
     */
    static std::shared_ptr<TwoPhaseLockingStorageHandler> create(GlobalExecutionPlanPtr globalExecutionPlan,
                         TopologyPtr topology,
                         QueryCatalogServicePtr queryCatalogService,
                         GlobalQueryPlanPtr globalQueryPlan,
                         Catalogs::Source::SourceCatalogPtr sourceCatalog,
                         Catalogs::UDF::UDFCatalogPtr udfCatalog);

    /**
     * @brief Locks the specified resources ordered after the corresponding enum variants in ResourceType beginning
     * with the first variant. Locking the resources in a specified order will prevent deadlocks.
     * This function can only be executed once during the lifetime of the storage handle. Will throw an
     * exception on second execution attempt. Resources which are not locked using this function can not be locked later on.
     * @param requiredResources the types of the resources to be locked
     */
    void acquireResources(RequestId requestId, std::vector<ResourceType> requiredResources) override;

    /**
     * @brief Obtain a mutable global execution plan handle. Will throw an exception if the resource has not been locked in the
     * acquireResources function
     * @return a handle to the global execution plan.
     */
    GlobalExecutionPlanHandle getGlobalExecutionPlanHandle(RequestId requestId) override;

    /**
     * @brief Obtain a mutable topology handle. Will throw an exception if the resource has not been locked in the
     * acquireResources function
     * @return a handle to the topology
     */
    TopologyHandle getTopologyHandle(RequestId requestId) override;

    /**
     * @brief Obtain a mutable query catalog handle. Will throw an exception if the resource has not been locked in the
     * acquireResources function
     * @return a handle to the query catalog.
     */
    QueryCatalogServiceHandle getQueryCatalogServiceHandle(RequestId requestId) override;

    /**
     * @brief Obtain a mutable global query plan handle. Will throw an exception if the resource has not been locked in the
     * acquireResources function
     * @return a handle to the global query plan.
     */
    GlobalQueryPlanHandle getGlobalQueryPlanHandle(RequestId requestId) override;

    /**
     * @brief Obtain a mutable source catalog handle. Will throw an exception if the resource has not been locked in the
     * acquireResources function
     * @return a handle to the source catalog.
     */
    SourceCatalogHandle getSourceCatalogHandle(RequestId requestId) override;

    /**
     * @brief Obtain a mutable udf catalog handle. Will throw an exception if the resource has not been locked in the
     * acquireResources function
     * @return a handle to the udf catalog.
     */
    UDFCatalogHandle getUDFCatalogHandle(RequestId requestId) override;

  private:
    /**
     * @brief Locks the mutex corresponding to a resource and maintains the lock until the storage handle object is destructed
     * @param requestId: The id of the request instance which is trying to lock the resource
     * @param resourceType: The type of resource to be locked
     */
    void lockResource(ResourceType resourceType, RequestId requestId);

    //todo: probably not needed
    bool checkNoLockAfter(ResourceType resourceType, RequestId requestId);

    TwoPhaseLockManagerPtr lockManager;
    bool resourcesLocked;

    //resource pointers
    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    QueryCatalogServicePtr queryCatalogService;
    GlobalQueryPlanPtr globalQueryPlan;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;

    //lock holder ids
    std::atomic<RequestId> globalExecutionPlanHolder;
    std::atomic<RequestId> topologyHolder;
    std::atomic<RequestId> queryCatalogServiceHolder;
    std::atomic<RequestId> globalQueryPlanHolder;
    std::atomic<RequestId> sourceCatalogHolder;
    std::atomic<RequestId> udfCatalogHolder;

    /*
    std::mutex topologyMutex;
    std::mutex queryCatalogMutex;
    std::mutex sourceCatalogMutex;
    std::mutex globalExecutionPlanMutex;
    std::mutex globalQueryPlanMutex;
    std::mutex udfCatalogMutex;

    std::unique_lock<std::mutex> topologyLock;
    std::unique_lock<std::mutex> queryCatalogLock;
    std::unique_lock<std::mutex> sourceCatalogLock;
    std::unique_lock<std::mutex> globalExecutionPlanLock;
    std::unique_lock<std::mutex> globalQueryPlanLock;
    std::unique_lock<std::mutex> udfCatalogLock;
     */

    static void lockResource(std::atomic<uint64_t>& holder, RequestId requestId);
    std::atomic<RequestId>& getHolder(ResourceType resourceType);
    std::vector<ResourceType> getResourcesHeldByRequest(RequestId requestId);
    bool isLockHolder(RequestId requestId);
};
}// namespace NES

#endif// NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_TWOPHASELOCKINGSTORAGEHANDLER_HPP_
