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
#ifndef NES_SERIALSTORAGEHANDLE_HPP
#define NES_SERIALSTORAGEHANDLE_HPP

#include <WorkQueues/StorageHandles/StorageHandle.hpp>

namespace NES{

/**
 * @brief This class is intended for serial access and does not perform any locking before creating a resource handle.
 * Not thread safe!
 */
class SerialStorageHandle : public StorageHandle {
  public:
    SerialStorageHandle(GlobalExecutionPlanPtr  globalExecutionPlan,
                         TopologyPtr  topology,
                         QueryCatalogServicePtr  queryCatalogService,
                         GlobalQueryPlanPtr  globalQueryPlan,
                         Catalogs::Source::SourceCatalogPtr  sourceCatalog,
                         Catalogs::UDF::UdfCatalogPtr  udfCatalog);

    static std::shared_ptr<SerialStorageHandle> create(const GlobalExecutionPlanPtr&  globalExecutionPlan,
                         const TopologyPtr&  topology,
                         const QueryCatalogServicePtr&  queryCatalogService,
                         const GlobalQueryPlanPtr&  globalQueryPlan,
                         const Catalogs::Source::SourceCatalogPtr&  sourceCatalog,
                         const Catalogs::UDF::UdfCatalogPtr&  udfCatalog);

    /**
     * @brief Obtain a mutable topology handle.
     * @return a handle to the topology
     */
    TopologyHandle getTopologyHandle() override;

    /**
     * @brief Obtain a mutable query catalog handle.
     * @return a handle to the query catalog.
     */
    QueryCatalogServiceHandle getQueryCatalogHandle() override;

    /**
     * @brief Obtain a mutable source catalog handle.
     * @return a handle to the source catalog.
     */
    SourceCatalogHandle getSourceCatalogHandle() override;

    /**
     * @brief Obtain a mutable global execution plan handle.
     * @return a handle to the global execution plan.
     */
    GlobalExecutionPlanHandle getGlobalExecutionPlanHandle() override;

    /**
     * @brief Obtain a mutable global query plan handle.
     * @return a handle to the global query plan.
     */
    GlobalQueryPlanHandle getGlobalQueryPlanHandle() override;

    /**
     * @brief Obtain a mutable udf catalog handle.
     * @return a handle to the udf catalog.
     */
    UdfCatalogHandle getUdfCatalogHandle() override;
};
}
#endif//NES_SERIALSTORAGEHANDLE_HPP
