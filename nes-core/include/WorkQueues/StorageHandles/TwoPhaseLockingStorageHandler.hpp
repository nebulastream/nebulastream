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

#include <WorkQueues/StorageHandles/ResourceType.hpp>
#include <WorkQueues/StorageHandles/StorageHandler.hpp>
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
    explicit TwoPhaseLockingStorageHandler(TwoPhaseLockManagerPtr lockManager);

    /**
     * @brief factory to create a two phase locking storage manager object
     * @return shared pointer to the two phase locking storage manager
     */
    static std::shared_ptr<TwoPhaseLockingStorageHandler> create(const TwoPhaseLockManagerPtr& lockManager);

    /**
     * @brief Locks the specified resources ordered after the corresponding enum variants in ResourceType beginning
     * with the first variant. Locking the resources in a specified order will prevent deadlocks.
     * This function can only be executed once during the lifetime of the storage handle. Will throw an
     * exception on second execution attempt. Resources which are not locked using this function can not be locked later on.
     * @param requiredResources the types of the resources to be locked
     */
    void acquireResources(std::vector<ResourceType> requiredResources) override;

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
    QueryCatalogServiceHandle getQueryCatalogServiceHandle() override;

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
    UDFCatalogHandle getUDFCatalogHandle() override;

  private:
    /**
     * @brief Locks the mutex corresponding to a resource and maintains the lock until the storage handle object is destructed
     * @param resourceType: The type of resource to be locked
     */
    void lockResource(ResourceType resourceType);

    TwoPhaseLockManagerPtr lockManager;
    bool resourcesLocked;

    //resource pointers
    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    QueryCatalogServicePtr queryCatalogService;
    GlobalQueryPlanPtr globalQueryPlan;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
};
}// namespace NES

#endif//NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_CONSERVATIVETWOPHASELOCKINGSTORAGEHANDLE_HPP
