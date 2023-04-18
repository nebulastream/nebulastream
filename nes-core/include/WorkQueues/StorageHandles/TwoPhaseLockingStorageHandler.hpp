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

#include <WorkQueues/StorageHandles/StorageHandler.hpp>
#include <WorkQueues/StorageHandles/StorageHandlerResourceType.hpp>
#include <vector>

namespace NES {
class LockManager;
using TwoPhaseLockManagerPtr = std::shared_ptr<LockManager>;

//todo #3674: creating one handler per request might degrade performance under the currenct design
/**
 * @brief Resource handles created by this class ensure that the resource has been locked in the growing phase and stay locked
 * until the handle goes out of scope.
 */
class TwoPhaseLockingStorageHandler : public StorageHandler {
  public:
    /**
     * @brief Constructor
     * @param globalExecutionPlan a pointer to the global execution plan
     * @param topology a pointer to the topology
     * @param queryCatalogService a pointer to the query catalog service
     * @param globalQueryPlan a pointer to the global query plan
     * @param sourceCatalog a pointer to the source catalog
     * @param udfCatalog a pointer to the udf catalog
     * @param lockManager a pointer to the lock manager which maintains the mutexes for all of the above data structures
     */
    TwoPhaseLockingStorageHandler(GlobalExecutionPlanPtr globalExecutionPlan,
                                  TopologyPtr topology,
                                  QueryCatalogServicePtr queryCatalogService,
                                  GlobalQueryPlanPtr globalQueryPlan,
                                  Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                  Catalogs::UDF::UdfCatalogPtr udfCatalog,
                                  TwoPhaseLockManagerPtr lockManager);

    /**
     * @brief factory to create a two phase locking storage manager object
     * @param globalExecutionPlan a pointer to the global execution plan
     * @param topology a pointer to the topology
     * @param queryCatalogService a pointer to the query catalog service
     * @param globalQueryPlan a pointer to the global query plan
     * @param sourceCatalog a pointer to the source catalog
     * @param udfCatalog a pointer to the udf catalog
     * @param lockManager a pointer to the lock manager which maintains the mutexes for all of the above data structures
     * @return shared pointer to the two phase locking storage manager
     */
    static std::shared_ptr<TwoPhaseLockingStorageHandler> create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                                 const TopologyPtr& topology,
                                                                 const QueryCatalogServicePtr& queryCatalogService,
                                                                 const GlobalQueryPlanPtr& globalQueryPlan,
                                                                 const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                                                 const Catalogs::UDF::UdfCatalogPtr& udfCatalog,
                                                                 const TwoPhaseLockManagerPtr& lockManager);

    /**
     * @brief Locks the specified resources ordered after the corresponding enum variants in StorageHandlerResourceType beginning
     * with the first variant. Locking the resources in a specified order will prevent deadlocks.
     * This function can only be executed once during the lifetime of the storage handle. Will throw an
     * exception on second execution attempt. Resources which are not locked using this function can not be locked later on.
     * @param requiredResources the types of the resources to be locked
     */
    void acquireResources(std::vector<StorageHandlerResourceType> requiredResources) override;

    /**
     * @brief Obtain a mutable global execution plan handle. Will throw an exception if the resource has not been locked in the
     * acquireResources function
     * @return a handle to the global execution plan.
     */
    GlobalExecutionPlanHandle getGlobalExecutionPlanHandle() override;

    /**
     * @brief Obtain a mutable topology handle. Will throw an exception if the resource has not been locked in the
     * acquireResources function
     * @return a handle to the topology
     */
    TopologyHandle getTopologyHandle() override;

    /**
     * @brief Obtain a mutable query catalog handle. Will throw an exception if the resource has not been locked in the
     * acquireResources function
     * @return a handle to the query catalog.
     */
    QueryCatalogServiceHandle getQueryCatalogHandle() override;

    /**
     * @brief Obtain a mutable global query plan handle. Will throw an exception if the resource has not been locked in the
     * acquireResources function
     * @return a handle to the global query plan.
     */
    GlobalQueryPlanHandle getGlobalQueryPlanHandle() override;

    /**
     * @brief Obtain a mutable source catalog handle. Will throw an exception if the resource has not been locked in the
     * acquireResources function
     * @return a handle to the source catalog.
     */
    SourceCatalogHandle getSourceCatalogHandle() override;

    /**
     * @brief Obtain a mutable udf catalog handle. Will throw an exception if the resource has not been locked in the
     * acquireResources function
     * @return a handle to the udf catalog.
     */
    UdfCatalogHandle getUdfCatalogHandle() override;

  private:
    /**
     * @brief Locks the mutex corresponding to a resource and maintains the lock until the storage handle object is destructed
     * @param resourceType: The type of resource to be locked
     */
    void lockResource(StorageHandlerResourceType resourceType);

    TwoPhaseLockManagerPtr lockManager;
    bool resourcesLocked;

    std::unique_lock<std::mutex> topologyLock;
    std::unique_lock<std::mutex> queryCatalogLock;
    std::unique_lock<std::mutex> sourceCatalogLock;
    std::unique_lock<std::mutex> globalExecutionPlanLock;
    std::unique_lock<std::mutex> globalQueryPlanLock;
    std::unique_lock<std::mutex> udfCatalogLock;
};
}// namespace NES

#endif//NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_CONSERVATIVETWOPHASELOCKINGSTORAGEHANDLE_HPP
