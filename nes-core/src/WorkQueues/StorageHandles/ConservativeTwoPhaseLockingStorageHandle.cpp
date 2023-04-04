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
#include <WorkQueues/StorageHandles/ConservativeTwoPhaseLockingStorageHandle.hpp>
#include <WorkQueues/StorageHandles/ConservativeTwoPhaseLockManager.hpp>
namespace NES {

ConservativeTwoPhaseLockingStorageHandle::ConservativeTwoPhaseLockingStorageHandle(
    GlobalExecutionPlanPtr globalExecutionPlan,
    TopologyPtr topology,
    QueryCatalogServicePtr queryCatalogService,
    GlobalQueryPlanPtr globalQueryPlan,
    Catalogs::Source::SourceCatalogPtr sourceCatalog,
    Catalogs::UDF::UdfCatalogPtr udfCatalog,
    ConservativeTwoPhaseLockManagerPtr lockManager)
    : StorageHandle(globalExecutionPlan,topology,queryCatalogService,globalQueryPlan,sourceCatalog,udfCatalog), lockManager(lockManager), resourcesLocked(false) {}

void ConservativeTwoPhaseLockingStorageHandle::preExecution(std::vector<StorageHandleResourceType> requiredResources) {
    //do not allow performing preexecution twice
    if (resourcesLocked) {
        //todo: more verbose exception handling
        throw std::exception();
    }
    resourcesLocked = true;

    //sort the resource list to ensure that resources are acquired in a deterministic order for deadlock prevention
    std::sort(requiredResources.begin(), requiredResources.end());

    //lockthe resources
    for (const auto& type : requiredResources) {
        lockResource(type);
    }
}

void ConservativeTwoPhaseLockingStorageHandle::lockResource(StorageHandleResourceType resourceType) {
    switch (resourceType) {
        case StorageHandleResourceType::Topology:
            topologyLock = lockManager->getLock(resourceType);
            break;
        case StorageHandleResourceType::QueryCatalogService:
            queryCatalogLock = lockManager->getLock(resourceType);
            break;
        case StorageHandleResourceType::SourceCatalog:
            sourceCatalogLock = lockManager->getLock(resourceType);
            break;
        case StorageHandleResourceType::GlobalExecutionPlan:
            globalExecutionPlanLock = lockManager->getLock(resourceType);
            break;
        case StorageHandleResourceType::GlobalQueryPlan:
            globalQueryPlanLock = lockManager->getLock(resourceType);
            break;
        case StorageHandleResourceType::UdfCatalog:
            udfCatalogLock = lockManager->getLock(resourceType);
            break;
    }
}

GlobalExecutionPlanHandle ConservativeTwoPhaseLockingStorageHandle::getGlobalExecutionPlanHandle() {
    if (!globalExecutionPlanLock) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return globalExecutionPlan;
}

TopologyHandle ConservativeTwoPhaseLockingStorageHandle::getTopologyHandle() {
    if (!topologyLock) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return topology;
}

QueryCatalogServiceHandle ConservativeTwoPhaseLockingStorageHandle::getQueryCatalogHandle() {
    if (!queryCatalogLock) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return queryCatalogService;
}

GlobalQueryPlanHandle ConservativeTwoPhaseLockingStorageHandle::getGlobalQueryPlanHandle() {
    if (!globalQueryPlanLock) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return globalQueryPlan;
}

SourceCatalogHandle ConservativeTwoPhaseLockingStorageHandle::getSourceCatalogHandle() {
    if (!sourceCatalogLock) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return sourceCatalog;
}
UdfCatalogHandle ConservativeTwoPhaseLockingStorageHandle::getUdfCatalogHandle() {
    if (!udfCatalogLock) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return udfCatalog;
}
std::shared_ptr<ConservativeTwoPhaseLockingStorageHandle>
ConservativeTwoPhaseLockingStorageHandle::create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                 const TopologyPtr& topology,
                                                 const QueryCatalogServicePtr& queryCatalogService,
                                                 const GlobalQueryPlanPtr& globalQueryPlan,
                                                 const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                                 const Catalogs::UDF::UdfCatalogPtr& udfCatalog,
                                                 ConservativeTwoPhaseLockManagerPtr lockManager) {
    return std::make_shared<ConservativeTwoPhaseLockingStorageHandle>(globalExecutionPlan, topology, queryCatalogService, globalQueryPlan, sourceCatalog, udfCatalog, lockManager);
}

}
