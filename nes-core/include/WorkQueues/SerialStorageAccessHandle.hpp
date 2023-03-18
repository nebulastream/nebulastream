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
#ifndef NES_SERIALSTORAGEACCESSHANDLE_HPP
#define NES_SERIALSTORAGEACCESSHANDLE_HPP
#include <WorkQueues/StorageAccessHandle.hpp>
namespace NES{

/**
 * This class does not perform any locking before handing out a resource handle. Not thread safe!
 */
class SerialStorageAccessHandle : public StorageAccessHandle {
  public:
    SerialStorageAccessHandle(TopologyPtr topology);

    static std::shared_ptr<SerialStorageAccessHandle> create(const TopologyPtr& topology);

    /**
     * Indicates that the storage handle requires a rollback because we perform serial operations on the actual data.
     * @return always true
     */
    bool requiresRollback() override;

    /**
     * Obtain a mutable topology handle. Throws an exception if the lock could not be acquired
     * @return a handle to the topology
     */
    TopologyHandle getTopologyHandle() override;

    /**
     * Obtain a mutable query catalog handle. Throws an exception if the lock could not be acquired
     * @return a handle to the query catalog.
     */
    QueryCatalogHandle getQueryCatalogHandle() override;

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
};

}

#endif//NES_SERIALSTORAGEACCESSHANDLE_HPP
