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
#ifndef NES_TWOPHASELOCKINGSTORAGEACCESSHANDLE_HPP
#define NES_TWOPHASELOCKINGSTORAGEACCESSHANDLE_HPP

#include <WorkQueues/StorageHandles/StorageHandle.hpp>

namespace NES {

/**
 * Resource handles created by this class ensure that the resource is locked until the handle goes out of scope.
 */
class TwoPhaseLockingStorageHandle : public StorageAccessHandle {
  public:
    TwoPhaseLockingStorageHandle(GlobalExecutionPlanPtr  globalExecutionPlan,
                         TopologyPtr  topology,
                         QueryCatalogServicePtr  queryCatalogService,
                         GlobalQueryPlanPtr  globalQueryPlan,
                         Catalogs::Source::SourceCatalogPtr  sourceCatalog,
                         Catalogs::UDF::UdfCatalogPtr  udfCatalog);

    static std::shared_ptr<TwoPhaseLockingStorageHandle> create(const GlobalExecutionPlanPtr&  globalExecutionPlan,
                         const TopologyPtr&  topology,
                         const QueryCatalogServicePtr&  queryCatalogService,
                         const GlobalQueryPlanPtr&  globalQueryPlan,
                         const Catalogs::Source::SourceCatalogPtr&  sourceCatalog,
                         const Catalogs::UDF::UdfCatalogPtr&  udfCatalog);

    /**
     * Obtain a mutable global execution plan handle. Throws an exception if the lock could not be acquired
     * @return a handle to the global execution plan.
     */
    GlobalExecutionPlanHandle getGlobalExecutionPlanHandle() override;

    /**
     * Obtain a mutable topology handle. Throws an exception if the lock could not be acquired
     * @return a handle to the topology
     */
    TopologyHandle getTopologyHandle() override;

    /**
     * Obtain a mutable query catalog handle. Throws an exception if the lock could not be acquired
     * @return a handle to the query catalog.
     */
    QueryCatalogServiceHandle getQueryCatalogHandle() override;

    /**
     * Obtain a mutable global query plan handle. Throws an exception if the lock could not be acquired
     * @return a handle to the global query plan.
     */
    GlobalQueryPlanHandle getGlobalQueryPlanHandle() override;

    /**
     * Obtain a mutable source catalog handle. Throws an exception if the lock could not be acquired
     * @return a handle to the source catalog.
     */
    SourceCatalogHandle getSourceCatalogHandle() override;

    /**
     * Obtain a mutable udf catalog handle. Throws an exception if the lock could not be acquired
     * @return a handle to the udf catalog.
     */
    UdfCatalogHandle getUdfCatalogHandle() override;

  private:
    //todo #3588: keep the mutexes here or in the class of the resource itself?
    std::mutex topologyMutex;
    std::mutex queryCatalogMutex;
    std::mutex sourceCatalogMutex;
    std::mutex globalExecutionPlanMutex;
    std::mutex globalQueryPlanMutex;
    std::mutex udfCatalogMutex;
};
}
#endif//NES_TWOPHASELOCKINGSTORAGEACCESSHANDLE_HPP
