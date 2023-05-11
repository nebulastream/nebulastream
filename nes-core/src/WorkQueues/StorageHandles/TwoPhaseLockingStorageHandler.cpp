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
#include <algorithm>
#include <utility>

namespace NES {

TwoPhaseLockingStorageHandler::TwoPhaseLockingStorageHandler(TwoPhaseLockManagerPtr lockManager)
    : lockManager(std::move(lockManager)), resourcesLocked(false) {}

void TwoPhaseLockingStorageHandler::acquireResources(std::vector<ResourceType> requiredResources) {
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

void TwoPhaseLockingStorageHandler::lockResource(ResourceType resourceType) {
    switch (resourceType) {
        case ResourceType::Topology: topology = lockManager->getTopologyHandle(); break;
        case ResourceType::QueryCatalogService: queryCatalogService = lockManager->getQueryCatalogHandle(); break;
        case ResourceType::SourceCatalog: sourceCatalog = lockManager->getSourceCatalogHandle(); break;
        case ResourceType::GlobalExecutionPlan: globalExecutionPlan = lockManager->getGlobalExecutionPlanHandle(); break;
        case ResourceType::GlobalQueryPlan: globalQueryPlan = lockManager->getGlobalQueryPlanHandle(); break;
        case ResourceType::UdfCatalog: udfCatalog = lockManager->getUDFCatalogHandle(); break;
    }
}

GlobalExecutionPlanHandle TwoPhaseLockingStorageHandler::getGlobalExecutionPlanHandle() {
    if (!globalExecutionPlan) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return globalExecutionPlan;
}

TopologyHandle TwoPhaseLockingStorageHandler::getTopologyHandle() {
    if (!topology) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return topology;
}

QueryCatalogServiceHandle TwoPhaseLockingStorageHandler::getQueryCatalogServiceHandle() {
    if (!queryCatalogService) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return queryCatalogService;
}

GlobalQueryPlanHandle TwoPhaseLockingStorageHandler::getGlobalQueryPlanHandle() {
    if (!globalQueryPlan) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return globalQueryPlan;
}

SourceCatalogHandle TwoPhaseLockingStorageHandler::getSourceCatalogHandle() {
    if (!sourceCatalog) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return sourceCatalog;
}

UDFCatalogHandle TwoPhaseLockingStorageHandler::getUDFCatalogHandle() {
    if (!udfCatalog) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return udfCatalog;
}

std::shared_ptr<TwoPhaseLockingStorageHandler> TwoPhaseLockingStorageHandler::create(const TwoPhaseLockManagerPtr& lockManager) {
    return std::make_shared<TwoPhaseLockingStorageHandler>(lockManager);
}
}// namespace NES
