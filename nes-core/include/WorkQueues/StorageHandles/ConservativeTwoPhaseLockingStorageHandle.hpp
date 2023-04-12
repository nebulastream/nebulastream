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
#ifndef NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_CONSERVATIVETWOPHASELOCKINGSTORAGEHANDLE_HPP
#define NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_CONSERVATIVETWOPHASELOCKINGSTORAGEHANDLE_HPP

#include <vector>
#include <WorkQueues/StorageHandles/StorageHandle.hpp>
#include <WorkQueues/StorageHandles/StorageHandleResourceType.hpp>

namespace NES {
class ConservativeTwoPhaseLockManager;
using ConservativeTwoPhaseLockManagerPtr = std::shared_ptr<ConservativeTwoPhaseLockManager>;

class ConservativeTwoPhaseLockingStorageHandle : public StorageHandle {
  public:
    /**
     * @brief Constructor
     * @param globalExecutionPlan a pointer to the global execution plan
     * @param topology a pointer to the topology
     * @param queryCatalogService a pointer to the query catalog service
     * @param globalQueryPlan a pointer to the global query plan
     * @param sourceCatalog a pointer to the source catalor
     * @param udfCatalog a pointer to the udf catalor
     * @param lockManager a pointer to the lock manager which maintains the mutexes for all of the above data structures
     */
    ConservativeTwoPhaseLockingStorageHandle(GlobalExecutionPlanPtr globalExecutionPlan,
                                             TopologyPtr topology,
                                             QueryCatalogServicePtr queryCatalogService,
                                             GlobalQueryPlanPtr globalQueryPlan,
                                             Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                             Catalogs::UDF::UdfCatalogPtr udfCatalog,
                                             ConservativeTwoPhaseLockManagerPtr lockManager);

    static std::shared_ptr<ConservativeTwoPhaseLockingStorageHandle> create(const GlobalExecutionPlanPtr&  globalExecutionPlan,
                                                                const TopologyPtr&  topology,
                                                                const QueryCatalogServicePtr&  queryCatalogService,
                                                                const GlobalQueryPlanPtr&  globalQueryPlan,
                                                                const Catalogs::Source::SourceCatalogPtr&  sourceCatalog,
                                                                const Catalogs::UDF::UdfCatalogPtr&  udfCatalog,
                                                                ConservativeTwoPhaseLockManagerPtr lockManager);

    /**
     * @brief Locks the specified resources ordered after the corresponding enum variants in StorageHanldeResourceType beginning
     * with the first variant. Locking the resources in a specified order will prevent deadlocks.
     * This function can only be executed once during the lifetime of the storage handle. Will throw an
     * exception on second execution attempt. Resources which are not locked using this function can not be locked later on.
     * @param requiredResources the types of the resources to be locked
     */
    void preExecution(std::vector<StorageHandleResourceType> requiredResources) override;

    /**
     * @brief Obtain a mutable global execution plan handle. Will throw an exception if the resource has not been locked in the
     * preExecution function
     * @return a handle to the global execution plan.
     */
    GlobalExecutionPlanHandle getGlobalExecutionPlanHandle() override;

    /**
     * @brief Obtain a mutable topology handle. Will throw an exception if the resource has not been locked in the
     * preExecution function
     * @return a handle to the topology
     */
    TopologyHandle getTopologyHandle() override;

    /**
     * @brief Obtain a mutable query catalog handle. Will throw an exception if the resource has not been locked in the
     * preExecution function
     * @return a handle to the query catalog.
     */
    QueryCatalogServiceHandle getQueryCatalogHandle() override;

    /**
     * @brief Obtain a mutable global query plan handle. Will throw an exception if the resource has not been locked in the
     * preExecution function
     * @return a handle to the global query plan.
     */
    GlobalQueryPlanHandle getGlobalQueryPlanHandle() override;

    /**
     * @brief Obtain a mutable source catalog handle. Will throw an exception if the resource has not been locked in the
     * preExecution function
     * @return a handle to the source catalog.
     */
    SourceCatalogHandle getSourceCatalogHandle() override;

    /**
     * @brief Obtain a mutable udf catalog handle. Will throw an exception if the resource has not been locked in the
     * preExecution function
     * @return a handle to the udf catalog.
     */
    UdfCatalogHandle getUdfCatalogHandle() override;

  private:
    /**
     * @brief Locks the mutex corresponding to a resource and maintains the lock until the storage handle object is destructed
     * @param resourceType: The type of resource to be locked
     */
    void lockResource(StorageHandleResourceType resourceType);

    ConservativeTwoPhaseLockManagerPtr lockManager;
    bool resourcesLocked;

    std::unique_lock<std::mutex> topologyLock;
    std::unique_lock<std::mutex> queryCatalogLock;
    std::unique_lock<std::mutex> sourceCatalogLock;
    std::unique_lock<std::mutex> globalExecutionPlanLock;
    std::unique_lock<std::mutex> globalQueryPlanLock;
    std::unique_lock<std::mutex> udfCatalogLock;
};
}

#endif//NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_CONSERVATIVETWOPHASELOCKINGSTORAGEHANDLE_HPP
