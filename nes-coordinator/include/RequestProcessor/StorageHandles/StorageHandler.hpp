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
#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_STORAGEHANDLES_STORAGEHANDLER_HPP_
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_STORAGEHANDLES_STORAGEHANDLER_HPP_

#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/UnlockDeleter.hpp>
#include <memory>
#include <vector>

namespace NES {

//todo #3610: currently we only have handle that allow reading and writing. but we should also define also handles that allow only const operations

using RequestId = uint64_t;
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

namespace Configurations {
class CoordinatorConfiguration;
using CoordinatorConfigurationPtr = std::shared_ptr<CoordinatorConfiguration>;
}// namespace Configurations

namespace Catalogs::Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Catalogs::Source

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

using SourceCatalogHandle = ResourceHandle<Catalogs::Source::SourceCatalog>;
using CoordinatorConfigurationHandle = ResourceHandle<Configurations::CoordinatorConfiguration>;
using GlobalExecutionPlanHandle = ResourceHandle<GlobalExecutionPlan>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;
using GlobalQueryPlanHandle = ResourceHandle<GlobalQueryPlan>;

namespace Catalogs::UDF {
class UDFCatalog;
using UDFCatalogPtr = std::shared_ptr<UDFCatalog>;
}// namespace Catalogs::UDF
using UDFCatalogHandle = ResourceHandle<Catalogs::UDF::UDFCatalog>;

namespace RequestProcessor::Experimental {

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
     * This function is to be executed before the request logic. It's base class implementation is empty. Derived classes need
     * to override this function in case they need to lock resources or perform other actions before a request is executed.
     * @param requestId The id of the request which calls this function
     * @param requiredResources The resources required for executing the request.
     */
    virtual void acquireResources(RequestId requestId, const std::vector<ResourceType>& requiredResources);

    /**
     * This function is called after the request finished executing. The base class implementation is empty. Derived classes need
     * to override this function in case they need to release resources or perform other actions after a request finished executing
     * @param requestId The id of the request which calls this function
     */
    virtual void releaseResources(RequestId requestId);

    /**
     * @brief Obtain a mutable global execution plan handle.
     * @param requestId The id of the request which calls this function
     * @return a handle to the global execution plan.
     */
    virtual GlobalExecutionPlanHandle getGlobalExecutionPlanHandle(RequestId requestId);

    /**
     * @brief Obtain a mutable topology handle.
     * @param requestId The id of the request which calls this function
     * @return a handle to the topology
     */
    virtual TopologyHandle getTopologyHandle(RequestId requestId);

    /**
     * @brief Obtain a mutable query catalog handle.
     * @param requestId The id of the request which calls this function
     * @return a handle to the query catalog.
     */
    virtual QueryCatalogServiceHandle getQueryCatalogServiceHandle(RequestId requestId);

    /**
     * @brief Obtain a mutable global query plan handle.
     * @param requestId The id of the request which calls this function
     * @return a handle to the global query plan.
     */
    virtual GlobalQueryPlanHandle getGlobalQueryPlanHandle(RequestId requestId);

    /**
     * @brief Obtain a mutable source catalog handle.
     * @param requestId The id of the request which calls this function
     * @return a handle to the source catalog.
     */
    virtual SourceCatalogHandle getSourceCatalogHandle(RequestId requestId);

    /**
     * @brief Obtain a mutable udf catalog handle.
     * @param requestId The id of the request which calls this function
     * @return a handle to the udf catalog.
     */
    virtual UDFCatalogHandle getUDFCatalogHandle(RequestId requestId);

    /**
     * @brief Get coordinator configuration
     * @param requestId the id of the request which calls this function
     * @return  a handle to the coordinator configuration
     */
    virtual CoordinatorConfigurationHandle getCoordinatorConfiguration(RequestId requestId);
};
}// namespace RequestProcessor::Experimental
}// namespace NES
#endif  // NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_STORAGEHANDLES_STORAGEHANDLER_HPP_
