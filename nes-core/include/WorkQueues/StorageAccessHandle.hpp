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

#include <memory>
#include <WorkQueues/UnlockDeleter.hpp>

namespace NES {

//todo: currently we only have handle that allow reading and writing. but define also handles that allow only const operations

class UnlockDeleter;

/*
 * Resource handle idea taken from
 * https://stackoverflow.com/questions/23610561/return-locked-resource-from-class-with-automatic-unlocking#comment36245473_23610561
 */
template<typename T>
using ResourceHandle = std::unique_ptr<T, UnlockDeleter>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;
using TopologyHandle = ResourceHandle<Topology>;

class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;
using QueryCatalogHandle = ResourceHandle<QueryCatalog>;

class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
using SourceCatalogHandle = ResourceHandle<SourceCatalog>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;
using GlobalExecutionPlanHandle = ResourceHandle<GlobalExecutionPlan>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;
using GlobalQueryPlanHandle = ResourceHandle<GlobalQueryPlan>;

class StorageAccessHandle;
using StorageAccessHandlePtr = std::shared_ptr<StorageAccessHandle>;

/*
 * This class provides handles to access the coordinator storage layer.
 * This is an abstract class and its subclasses will have to provide the implementation to access the storage layer using
 * a certain concurrency control technique.
 */
class StorageAccessHandle {
  public:
    /**
     * Constructs a new storage access handle.
     * @param topology
     */
    StorageAccessHandle(TopologyPtr topology);

    virtual ~StorageAccessHandle() = default;

    /**
     * Indicates if the storage handle requires a rollback in case of an aborted operation.
     * @return
     */
    virtual bool requiresRollback() = 0;

    /**
     * Obtain a mutable topology handle. Throws an exception if the lock could not be acquired
     * @return a handle to the topology
     */
    virtual TopologyHandle getTopologyHandle() = 0;

    /**
     * Obtain a mutable query catalog handle. Throws an exception if the lock could not be acquired
     * @return a handle to the query catalog.
     */
    virtual QueryCatalogHandle getQueryCatalogHandle() = 0;

    /**
     * Obtain a mutable source catalog handle. Throws an exception if the lock could not be acquired
     * @return a handle to the source catalog.
     */
    virtual SourceCatalogHandle getSourceCatalogHandle() = 0;

    /**
     * Obtain a mutable global execution plan handle. Throws an exception if the lock could not be acquired
     * @return a handle to the global execution plan.
     */
    virtual GlobalExecutionPlanHandle getGlobalExecutionPlanHandle() = 0;

    /**
     * Obtain a mutable global query plan handle. Throws an exception if the lock could not be acquired
     * @return a handle to the global query plan.
     */
    virtual GlobalQueryPlanHandle getGlobalQueryPlanHandle() = 0;

  protected:
    TopologyPtr topology;
    QueryCatalogPtr queryCatalog;
    SourceCatalogPtr sourceCatalog;
    GlobalExecutionPlanPtr globalExecutionPlan;
    GlobalQueryPlanPtr globalQueryPlan;
};
}

#endif//NES_STORAGEACCESSHANDLE_HPP
