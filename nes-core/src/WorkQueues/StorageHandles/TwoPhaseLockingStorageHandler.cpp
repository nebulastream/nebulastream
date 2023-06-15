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
#include <Util/Logger/Logger.hpp>
#include <WorkQueues/StorageHandles/LockManager.hpp>
#include <WorkQueues/StorageHandles/TwoPhaseLockingStorageHandler.hpp>
#include <algorithm>
#include <utility>

namespace NES {

TwoPhaseLockingStorageHandler::TwoPhaseLockingStorageHandler(GlobalExecutionPlanPtr globalExecutionPlan,
                                                             TopologyPtr topology,
                                                             QueryCatalogServicePtr queryCatalogService,
                                                             GlobalQueryPlanPtr globalQueryPlan,
                                                             Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                                             Catalogs::UDF::UDFCatalogPtr udfCatalog) : globalExecutionPlan(std::move(globalExecutionPlan)), topology(std::move(topology)), queryCatalogService(std::move(queryCatalogService)), globalQueryPlan(std::move(globalQueryPlan)), sourceCatalog(std::move(sourceCatalog)), udfCatalog(std::move(udfCatalog)) {}

void TwoPhaseLockingStorageHandler::acquireResources(RequestId requestId, std::vector<ResourceType> requiredResources) {
    //do not allow performing resource acquisition more than once
    if (isLockHolder(requestId)) {
        //todo #3611: more verbose exception handling
        throw std::exception();
    }

    //sort the resource list to ensure that resources are acquired in a deterministic order for deadlock prevention
    std::sort(requiredResources.begin(), requiredResources.end());

    //lock the resources
    for (const auto& type : requiredResources) {
        NES_DEBUG2("Request {} trying to acquire resource {}", requestId, (uint8_t) type);
        lockResource(type, requestId);
    }
}

void TwoPhaseLockingStorageHandler::releaseResources(RequestId requestId) {
    for (uint8_t i = 0; i <= (uint8_t) ResourceType::Last; ++i) {
        auto& holder = getHolder((ResourceType) i);
        /*
        if (holder == requestId) {
            holder = INVALID_REQUEST_ID;
            NES_DEBUG2("Request {}: release resource {}", requestId, i);
        }
         */
        auto expected = requestId;
        if (holder.compare_exchange_strong(expected, INVALID_REQUEST_ID)) {
            NES_DEBUG2("Request {}: release resource {}", requestId, i);
        }
    }
}

void TwoPhaseLockingStorageHandler::lockResource(std::atomic<uint64_t>& holder, RequestId requestId) {
    auto expected = INVALID_REQUEST_ID;
    while (!holder.compare_exchange_weak(expected, requestId)) {
        NES_DEBUG2("Request {} could not acquire lock because resource is held by {}", requestId, expected);
        if (expected == INVALID_REQUEST_ID) {
            continue;
        }
        holder.wait(expected);
        expected = INVALID_REQUEST_ID;
    }
    NES_DEBUG2("Request {} acquired resource", requestId);
}

//todo: define a function starting at a certain type only
std::vector<ResourceType> TwoPhaseLockingStorageHandler::getResourcesHeldByRequest(RequestId requestId) {
    std::vector<ResourceType> resourcesTypes;
    for (uint8_t i = 0; i <= (uint8_t) ResourceType::Last; ++i) {
        if (getHolder((ResourceType) i) == requestId) {
            resourcesTypes.push_back((ResourceType) i);
        }
    }
    return resourcesTypes;
}

bool TwoPhaseLockingStorageHandler::isLockHolder(RequestId requestId) {
    for (uint8_t i = 0; i <= (uint8_t) ResourceType::Last; ++i) {
        if (getHolder((ResourceType) i) == requestId) {
            return true;
        }
    }
    return false;
}

std::atomic<RequestId>& TwoPhaseLockingStorageHandler::getHolder(ResourceType resourceType) {
    switch (resourceType) {
        case ResourceType::Topology:
            return topologyHolder;
        case ResourceType::QueryCatalogService:
            return queryCatalogServiceHolder;
        case ResourceType::SourceCatalog:
            return sourceCatalogHolder;
        case ResourceType::GlobalExecutionPlan:
            return globalExecutionPlanHolder;
        case ResourceType::GlobalQueryPlan:
            return globalQueryPlanHolder;
        case ResourceType::UdfCatalog:
            return udfCatalogHolder;
    }
}


void TwoPhaseLockingStorageHandler::lockResource(ResourceType resourceType, RequestId requestId) {
    lockResource(getHolder(resourceType), requestId);
    /*
    switch (resourceType) {
        case ResourceType::Topology:
            lockResource(topologyHolder, requestId);
            break;
        case ResourceType::QueryCatalogService:
            lockResource(queryCatalogServiceHolder, requestId);
            break;
        case ResourceType::SourceCatalog:
            lockResource(sourceCatalogHolder, requestId);
            break;
        case ResourceType::GlobalExecutionPlan:
            lockResource(globalExecutionPlanHolder, requestId);
            break;
        case ResourceType::GlobalQueryPlan:
            lockResource(globalQueryPlanHolder, requestId);
            break;
        case ResourceType::UdfCatalog:
            lockResource(udfCatalogHolder, requestId);
            break;
    }
     */
}

GlobalExecutionPlanHandle TwoPhaseLockingStorageHandler::getGlobalExecutionPlanHandle(RequestId requestId) {
    if (globalExecutionPlanHolder != requestId) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return globalExecutionPlan;
}

TopologyHandle TwoPhaseLockingStorageHandler::getTopologyHandle(RequestId requestId) {
    if (topologyHolder != requestId) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return topology;
}

QueryCatalogServiceHandle TwoPhaseLockingStorageHandler::getQueryCatalogServiceHandle(RequestId requestId) {
    if (queryCatalogServiceHolder != requestId) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return queryCatalogService;
}

GlobalQueryPlanHandle TwoPhaseLockingStorageHandler::getGlobalQueryPlanHandle(RequestId requestId) {
    if (globalQueryPlanHolder != requestId) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return globalQueryPlan;
}

SourceCatalogHandle TwoPhaseLockingStorageHandler::getSourceCatalogHandle(RequestId requestId) {
    if (sourceCatalogHolder != requestId) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return sourceCatalog;
}

UDFCatalogHandle TwoPhaseLockingStorageHandler::getUDFCatalogHandle(RequestId requestId) {
    if (udfCatalogHolder != requestId) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return udfCatalog;
}
std::shared_ptr<TwoPhaseLockingStorageHandler>
TwoPhaseLockingStorageHandler::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                      TopologyPtr topology,
                                      QueryCatalogServicePtr queryCatalogService,
                                      GlobalQueryPlanPtr globalQueryPlan,
                                      Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                      Catalogs::UDF::UDFCatalogPtr udfCatalog) {
    return std::make_shared<TwoPhaseLockingStorageHandler>(globalExecutionPlan, topology, queryCatalogService, globalQueryPlan, sourceCatalog, udfCatalog);
}


}// namespace NES
