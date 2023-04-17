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
#ifndef NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_SERIALSTORAGEHANDLE_HPP_
#define NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_SERIALSTORAGEHANDLE_HPP_

#include <WorkQueues/StorageHandles/StorageHandler.hpp>

namespace NES {

/**
 * @brief This class is intended for serial access and does not perform any locking before creating a resource handle.
 * Not thread safe!
 */
class SerialStorageHandler : public StorageHandler {
  public:
    SerialStorageHandler(GlobalExecutionPlanPtr globalExecutionPlan,
                        TopologyPtr topology,
                        QueryCatalogServicePtr queryCatalogService,
                        GlobalQueryPlanPtr globalQueryPlan,
                        Catalogs::Source::SourceCatalogPtr sourceCatalog,
                        Catalogs::UDF::UdfCatalogPtr udfCatalog);

    /**
     * @brief factory to create a serial storage manager object
     * @param globalExecutionPlan a pointer to the global execution plan
     * @param topology a pointer to the topology
     * @param queryCatalogService a pointer to the query catalog service
     * @param globalQueryPlan a pointer to the global query plan
     * @param sourceCatalog a pointer to the source catalog
     * @param udfCatalog a pointer to the udf catalog
     * @param lockManager a pointer to the lock manager which maintains the mutexes for all of the above data structures
     * @return shared pointer to the serial storage manager
     */
    static std::shared_ptr<SerialStorageHandler> create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                       const TopologyPtr& topology,
                                                       const QueryCatalogServicePtr& queryCatalogService,
                                                       const GlobalQueryPlanPtr& globalQueryPlan,
                                                       const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                                       const Catalogs::UDF::UdfCatalogPtr& udfCatalog);

    /**
     * @brief This function does nothing because no special actions are needed to acquire resources for serial execution
     * @param requiredResources The resources required for request execution
     */
    void acquireResources(std::vector<StorageHandlerResourceType> requiredResources) override;

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
}// namespace NES
#endif//NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_SERIALSTORAGEHANDLE_HPP_
