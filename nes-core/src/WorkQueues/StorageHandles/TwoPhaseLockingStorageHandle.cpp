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
#include <WorkQueues/StorageHandles/LockStore.hpp>
#include <WorkQueues/StorageHandles/TwoPhaseLockingStorageHandle.hpp>
#include <utility>
namespace NES {

TwoPhaseLockingStorageHandle::TwoPhaseLockingStorageHandle(
    GlobalExecutionPlanPtr globalExecutionPlan,
    TopologyPtr topology,
    QueryCatalogServicePtr queryCatalogService,
    GlobalQueryPlanPtr globalQueryPlan,
    Catalogs::Source::SourceCatalogPtr sourceCatalog,
    Catalogs::UDF::UdfCatalogPtr udfCatalog,
    ConservativeTwoPhaseLockManagerPtr lockManager)
    : StorageHandle(std::move(globalExecutionPlan),std::move(topology),std::move(queryCatalogService),std::move(globalQueryPlan),std::move(sourceCatalog),std::move(udfCatalog)), lockManager(std::move(lockManager)), resourcesLocked(false) {}

void TwoPhaseLockingStorageHandle::preExecution(std::vector<StorageHandleResourceType> requiredResources) {
    //do not allow performing preexecution twice
    if (resourcesLocked) {
        //todo #3611: more verbose exception handling
        throw std::exception();
    }
    resourcesLocked = true;

    //sort the resource list to ensure that resources are acquired in a deterministic order for deadlock prevention
    std::sort(requiredResources.begin(), requiredResources.end());

    //lock the resources
    for (const auto& type : requiredResources) {
        lockResource(type);
    }
}

void TwoPhaseLockingStorageHandle::lockResource(StorageHandleResourceType resourceType) {
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

GlobalExecutionPlanHandle TwoPhaseLockingStorageHandle::getGlobalExecutionPlanHandle() {
    if (!globalExecutionPlanLock) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return globalExecutionPlan;
}

TopologyHandle TwoPhaseLockingStorageHandle::getTopologyHandle() {
    if (!topologyLock) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return topology;
}

QueryCatalogServiceHandle TwoPhaseLockingStorageHandle::getQueryCatalogHandle() {
    if (!queryCatalogLock) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return queryCatalogService;
}

GlobalQueryPlanHandle TwoPhaseLockingStorageHandle::getGlobalQueryPlanHandle() {
    if (!globalQueryPlanLock) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return globalQueryPlan;
}

SourceCatalogHandle TwoPhaseLockingStorageHandle::getSourceCatalogHandle() {
    if (!sourceCatalogLock) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return sourceCatalog;
}

UdfCatalogHandle TwoPhaseLockingStorageHandle::getUdfCatalogHandle() {
    if (!udfCatalogLock) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return udfCatalog;
}

std::shared_ptr<TwoPhaseLockingStorageHandle>
TwoPhaseLockingStorageHandle::create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                 const TopologyPtr& topology,
                                                 const QueryCatalogServicePtr& queryCatalogService,
                                                 const GlobalQueryPlanPtr& globalQueryPlan,
                                                 const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                                 const Catalogs::UDF::UdfCatalogPtr& udfCatalog,
                                                 const ConservativeTwoPhaseLockManagerPtr& lockManager) {
    return std::make_shared<TwoPhaseLockingStorageHandle>(globalExecutionPlan, topology, queryCatalogService, globalQueryPlan, sourceCatalog, udfCatalog, lockManager);
}
}
