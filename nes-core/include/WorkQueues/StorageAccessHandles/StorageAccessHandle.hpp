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
#ifndef NES_STORAGEACCESSHANDLE_HPP
#define NES_STORAGEACCESSHANDLE_HPP

#include <WorkQueues/StorageAccessHandles/UnlockDeleter.hpp>
#include <memory>

namespace NES {

//todo #3610: currently we only have handle that allow reading and writing. but we should also define also handles that allow only const operations

class UnlockDeleter;
template<typename T>
//on deletion, of the resource handle, the unlock deleter will only unlock the resource instead of freeing it
using ResourceHandle = std::unique_ptr<T, UnlockDeleter>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;
using TopologyHandle = ResourceHandle<Topology>;

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;
using QueryCatalogServiceHandle = ResourceHandle<QueryCatalogService>;

namespace Catalogs::Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}
using SourceCatalogHandle = ResourceHandle<Catalogs::Source::SourceCatalog>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;
using GlobalExecutionPlanHandle = ResourceHandle<GlobalExecutionPlan>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;
using GlobalQueryPlanHandle = ResourceHandle<GlobalQueryPlan>;

namespace Catalogs::UDF {
class UdfCatalog;
using UdfCatalogPtr = std::shared_ptr<UdfCatalog>;
}
using UdfCatalogHandle = ResourceHandle<Catalogs::UDF::UdfCatalog>;


class StorageAccessHandle;
using StorageAccessHandlePtr = std::shared_ptr<StorageAccessHandle>;

/*
 * This class provides handles to access the coordinator storage layer.
 * This is an abstract class and its subclasses will have to provide the implementation to warrant safe concurrent
 * access the storage layer.
 */
class StorageAccessHandle {
  public:
    /**
     * Constructs a new storage access handle.
     * @param topology
     */
    StorageAccessHandle(GlobalExecutionPlanPtr  globalExecutionPlan,
                         TopologyPtr  topology,
                         QueryCatalogServicePtr  queryCatalogService,
                         GlobalQueryPlanPtr  globalQueryPlan,
                         Catalogs::Source::SourceCatalogPtr  sourceCatalog,
                         Catalogs::UDF::UdfCatalogPtr  udfCatalog
        );

    virtual ~StorageAccessHandle() = default;

    /**
     * Obtain a mutable global execution plan handle. Throws an exception if the lock could not be acquired
     * @return a handle to the global execution plan.
     */
    virtual GlobalExecutionPlanHandle getGlobalExecutionPlanHandle() = 0;

    /**
     * Obtain a mutable topology handle. Throws an exception if the lock could not be acquired
     * @return a handle to the topology
     */
    virtual TopologyHandle getTopologyHandle() = 0;

    /**
     * Obtain a mutable query catalog handle. Throws an exception if the lock could not be acquired
     * @return a handle to the query catalog.
     */
    virtual QueryCatalogServiceHandle getQueryCatalogHandle() = 0;

    /**
     * Obtain a mutable global query plan handle. Throws an exception if the lock could not be acquired
     * @return a handle to the global query plan.
     */
    virtual GlobalQueryPlanHandle getGlobalQueryPlanHandle() = 0;

    /**
     * Obtain a mutable source catalog handle. Throws an exception if the lock could not be acquired
     * @return a handle to the source catalog.
     */
    virtual SourceCatalogHandle getSourceCatalogHandle() = 0;

    /**
     * Obtain a mutable udf catalog handle. Throws an exception if the lock could not be acquired
     * @return a handle to the udf catalog.
     */
    virtual UdfCatalogHandle getUdfCatalogHandle() = 0;

  protected:
    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    QueryCatalogServicePtr queryCatalogService;
    GlobalQueryPlanPtr globalQueryPlan;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::UDF::UdfCatalogPtr udfCatalog;
};
}
#endif//NES_STORAGEACCESSHANDLE_HPP
