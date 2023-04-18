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
#include <WorkQueues/StorageHandles/LockManager.hpp>
#include <WorkQueues/StorageHandles/TwoPhaseLockingStorageHandler.hpp>
#include <utility>
#include <algorithm>

namespace NES {

TwoPhaseLockingStorageHandler::TwoPhaseLockingStorageHandler(
    GlobalExecutionPlanPtr globalExecutionPlan,
    TopologyPtr topology,
    QueryCatalogServicePtr queryCatalogService,
    GlobalQueryPlanPtr globalQueryPlan,
    Catalogs::Source::SourceCatalogPtr sourceCatalog,
    Catalogs::UDF::UdfCatalogPtr udfCatalog,
                                                           TwoPhaseLockManagerPtr lockManager)
    : StorageHandler(std::move(globalExecutionPlan),std::move(topology),std::move(queryCatalogService),std::move(globalQueryPlan),std::move(sourceCatalog),std::move(udfCatalog)), lockManager(std::move(lockManager)), resourcesLocked(false) {}

void TwoPhaseLockingStorageHandler::acquireResources(std::vector<StorageHandlerResourceType> requiredResources) {
    //do not allow performing resource acquisition more than once
    if (resourcesLocked) {
        //todo #3611: more verbose exception handling
        throw std::exception();
    } else if (!requiredResources.empty()) {
        resourcesLocked = true;
    }

    //sort the resource list to ensure that resources are acquired in a deterministic order for deadlock prevention
    std::sort(requiredResources.begin(), requiredResources.end());

    //lock the resources
    for (const auto& type : requiredResources) {
        lockResource(type);
    }
}

void TwoPhaseLockingStorageHandler::lockResource(StorageHandlerResourceType resourceType) {
    switch (resourceType) {
        case StorageHandlerResourceType::Topology:
            topologyLock = lockManager->getLock(resourceType);
            break;
        case StorageHandlerResourceType::QueryCatalogService:
            queryCatalogLock = lockManager->getLock(resourceType);
            break;
        case StorageHandlerResourceType::SourceCatalog:
            sourceCatalogLock = lockManager->getLock(resourceType);
            break;
        case StorageHandlerResourceType::GlobalExecutionPlan:
            globalExecutionPlanLock = lockManager->getLock(resourceType);
            break;
        case StorageHandlerResourceType::GlobalQueryPlan:
            globalQueryPlanLock = lockManager->getLock(resourceType);
            break;
        case StorageHandlerResourceType::UdfCatalog:
            udfCatalogLock = lockManager->getLock(resourceType);
            break;
    }
}

GlobalExecutionPlanHandle TwoPhaseLockingStorageHandler::getGlobalExecutionPlanHandle() {
    if (!globalExecutionPlanLock.owns_lock()) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return globalExecutionPlan;
}

TopologyHandle TwoPhaseLockingStorageHandler::getTopologyHandle() {
    if (!topologyLock.owns_lock()) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return topology;
}

QueryCatalogServiceHandle TwoPhaseLockingStorageHandler::getQueryCatalogHandle() {
    if (!queryCatalogLock.owns_lock()) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return queryCatalogService;
}

GlobalQueryPlanHandle TwoPhaseLockingStorageHandler::getGlobalQueryPlanHandle() {
    if (!globalQueryPlanLock.owns_lock()) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return globalQueryPlan;
}

SourceCatalogHandle TwoPhaseLockingStorageHandler::getSourceCatalogHandle() {
    if (!sourceCatalogLock.owns_lock()) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return sourceCatalog;
}

UdfCatalogHandle TwoPhaseLockingStorageHandler::getUdfCatalogHandle() {
    if (!udfCatalogLock.owns_lock()) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return udfCatalog;
}

std::shared_ptr<TwoPhaseLockingStorageHandler>
TwoPhaseLockingStorageHandler::create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                 const TopologyPtr& topology,
                                                 const QueryCatalogServicePtr& queryCatalogService,
                                                 const GlobalQueryPlanPtr& globalQueryPlan,
                                                 const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                                 const Catalogs::UDF::UdfCatalogPtr& udfCatalog,
                                                 const TwoPhaseLockManagerPtr& lockManager) {
    return std::make_shared<TwoPhaseLockingStorageHandler>(globalExecutionPlan, topology, queryCatalogService, globalQueryPlan, sourceCatalog, udfCatalog, lockManager);
}
}
