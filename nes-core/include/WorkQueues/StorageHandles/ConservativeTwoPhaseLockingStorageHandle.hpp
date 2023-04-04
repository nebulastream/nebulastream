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

    void preExecution(std::vector<StorageHandleResourceType> requiredResources) override;

    /**
     * @brief Obtain a mutable global execution plan handle. Throws an exception if the lock could not be acquired
     * @return a handle to the global execution plan.
     */
    GlobalExecutionPlanHandle getGlobalExecutionPlanHandle() override;

    /**
     * @brief Obtain a mutable topology handle. Throws an exception if the lock could not be acquired
     * @return a handle to the topology
     */
    TopologyHandle getTopologyHandle() override;

    /**
     * @brief Obtain a mutable query catalog handle. Throws an exception if the lock could not be acquired
     * @return a handle to the query catalog.
     */
    QueryCatalogServiceHandle getQueryCatalogHandle() override;

    /**
     * @brief Obtain a mutable global query plan handle. Throws an exception if the lock could not be acquired
     * @return a handle to the global query plan.
     */
    GlobalQueryPlanHandle getGlobalQueryPlanHandle() override;

    /**
     * @brief Obtain a mutable source catalog handle. Throws an exception if the lock could not be acquired
     * @return a handle to the source catalog.
     */
    SourceCatalogHandle getSourceCatalogHandle() override;

    /**
     * @brief Obtain a mutable udf catalog handle. Throws an exception if the lock could not be acquired
     * @return a handle to the udf catalog.
     */
    UdfCatalogHandle getUdfCatalogHandle() override;
  private:
    void lockResource(StorageHandleResourceType);

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
