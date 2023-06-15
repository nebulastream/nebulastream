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
#ifndef NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_STORAGEHANDLER_HPP_
#define NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_STORAGEHANDLER_HPP_

#include <WorkQueues/StorageHandles/ResourceType.hpp>
#include <WorkQueues/StorageHandles/UnlockDeleter.hpp>
#include <memory>
#include <vector>
#include <Common/Identifiers.hpp>

namespace NES {

//todo #3610: currently we only have handle that allow reading and writing. but we should also define also handles that allow only const operations

class UnlockDeleter;
template<typename T>
//on deletion, of the resource handle, the unlock deleter will only unlock the resource instead of freeing it
using ResourceHandle = std::shared_ptr<T>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;
using TopologyHandle = ResourceHandle<Topology>;

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;
using QueryCatalogServiceHandle = ResourceHandle<QueryCatalogService>;

namespace Catalogs::Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Catalogs::Source
using SourceCatalogHandle = ResourceHandle<Catalogs::Source::SourceCatalog>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;
using GlobalExecutionPlanHandle = ResourceHandle<GlobalExecutionPlan>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;
using GlobalQueryPlanHandle = ResourceHandle<GlobalQueryPlan>;

namespace Catalogs::UDF {
class UDFCatalog;
using UDFCatalogPtr = std::shared_ptr<UDFCatalog>;
}// namespace Catalogs::UDF
using UDFCatalogHandle = ResourceHandle<Catalogs::UDF::UDFCatalog>;

class StorageHandler;
using StorageHandlerPtr = std::shared_ptr<StorageHandler>;

/*
 * @brief This class provides handles to access the coordinator storage layer.
 * This is an abstract class and its subclasses will have to provide the implementation to warrant safe concurrent
 * access the storage layer.
 */
class StorageHandler {
  public:
    virtual ~StorageHandler() = default;

    /**
     * Performs tasks necessary before request execution and locks resources if necessary
     * @param requestId The id of the request which calls this function
     * @param requiredResources The resources required for executing the request.
     */
    virtual void acquireResources(RequestId requestId, std::vector<ResourceType> requiredResources) = 0;

    /**
     * Performs tasks necessary after request execution, releases resources if necessary
     * @param requestId The id of the request which calls this function
     */
    virtual void releaseResources(RequestId requestId) = 0;

    /**
     * @brief Obtain a mutable global execution plan handle.
     * @param requestId The id of the request which calls this function
     * @return a handle to the global execution plan.
     */
    virtual GlobalExecutionPlanHandle getGlobalExecutionPlanHandle(RequestId requestId) = 0;

    /**
     * @brief Obtain a mutable topology handle.
     * @param requestId The id of the request which calls this function
     * @return a handle to the topology
     */
    virtual TopologyHandle getTopologyHandle(RequestId requestId) = 0;

    /**
     * @brief Obtain a mutable query catalog handle.
     * @param requestId The id of the request which calls this function
     * @return a handle to the query catalog.
     */
    virtual QueryCatalogServiceHandle getQueryCatalogServiceHandle(RequestId requestId) = 0;

    /**
     * @brief Obtain a mutable global query plan handle.
     * @param requestId The id of the request which calls this function
     * @return a handle to the global query plan.
     */
    virtual GlobalQueryPlanHandle getGlobalQueryPlanHandle(RequestId requestId) = 0;

    /**
     * @brief Obtain a mutable source catalog handle.
     * @param requestId The id of the request which calls this function
     * @return a handle to the source catalog.
     */
    virtual SourceCatalogHandle getSourceCatalogHandle(RequestId requestId) = 0;

    /**
     * @brief Obtain a mutable udf catalog handle.
     * @param requestId The id of the request which calls this function
     * @return a handle to the udf catalog.
     */
    virtual UDFCatalogHandle getUDFCatalogHandle(RequestId requestId) = 0;
};
}// namespace NES
#endif// NES_CORE_INCLUDE_WORKQUEUES_STORAGEHANDLES_STORAGEHANDLER_HPP_
