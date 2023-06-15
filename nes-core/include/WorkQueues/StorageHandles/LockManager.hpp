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
#ifndef NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_LOCKMANAGER_HPP_
#define NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_LOCKMANAGER_HPP_
#include <WorkQueues/StorageHandles/StorageHandler.hpp>

//todo #3588: This class is not not needed if we move the mutexes into the data structures itself
/**
 * @brief This class locks resources and hands out resource handles to different instances of the two phase locking
 * storage handle. This way each request can have its own storage handle but still have thread safe access.
 */
namespace NES {
class LockManager {
  public:
    LockManager(GlobalExecutionPlanPtr globalExecutionPlan,
                TopologyPtr topology,
                QueryCatalogServicePtr queryCatalogService,
                GlobalQueryPlanPtr globalQueryPlan,
                Catalogs::Source::SourceCatalogPtr sourceCatalog,
                Catalogs::UDF::UDFCatalogPtr udfCatalog);

    /**
     * @brief Obtain a mutable global execution plan handle.
     * @return a handle to the global execution plan.
     */
    GlobalExecutionPlanHandle getGlobalExecutionPlanHandle();

    /**
     * @brief Obtain a mutable topology handle.
     * @return a handle to the topology
     */
    TopologyHandle getTopologyHandle();

    /**
     * @brief Obtain a mutable query catalog handle.
     * @return a handle to the query catalog.
     */
    QueryCatalogServiceHandle getQueryCatalogHandle();

    /**
     * @brief Obtain a mutable global query plan handle.
     * @return a handle to the global query plan.
     */
    GlobalQueryPlanHandle getGlobalQueryPlanHandle();

    /**
     * @brief Obtain a mutable source catalog handle.
     * @return a handle to the source catalog.
     */
    SourceCatalogHandle getSourceCatalogHandle();

    /**
     * @brief Obtain a mutable udf catalog handle.
     * @return a handle to the udf catalog.
     */
    UDFCatalogHandle getUDFCatalogHandle();

  private:
    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    QueryCatalogServicePtr queryCatalogService;
    GlobalQueryPlanPtr globalQueryPlan;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    std::mutex topologyMutex;
    std::mutex queryCatalogMutex;
    std::mutex sourceCatalogMutex;
    std::mutex globalExecutionPlanMutex;
    std::mutex globalQueryPlanMutex;
    std::mutex udfCatalogMutex;
};
}// namespace NES
#endif// NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_LOCKMANAGER_HPP_
