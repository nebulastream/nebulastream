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
        if (holder.id == requestId) {
            holder.id = INVALID_REQUEST_ID;
            holder.mutex.unlock();
            NES_DEBUG2("Request {}: release resource {}", requestId, i);
        }
    }
}

void TwoPhaseLockingStorageHandler::lockResource(ResourceLockInfo& holder, RequestId requestId) {
    holder.mutex.lock();
    holder.id = requestId;
    NES_DEBUG2("Request {} acquired resource", requestId);
}

bool TwoPhaseLockingStorageHandler::isLockHolder(RequestId requestId) {
    for (uint8_t i = 0; i <= (uint8_t) ResourceType::Last; ++i) {
        if (getHolder((ResourceType) i).id == requestId) {
            return true;
        }
    }
    return false;
}

ResourceLockInfo& TwoPhaseLockingStorageHandler::getHolder(ResourceType resourceType) {
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
}

GlobalExecutionPlanHandle TwoPhaseLockingStorageHandler::getGlobalExecutionPlanHandle(RequestId requestId) {
    if (globalExecutionPlanHolder.id != requestId) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return globalExecutionPlan;
}

TopologyHandle TwoPhaseLockingStorageHandler::getTopologyHandle(RequestId requestId) {
    if (topologyHolder.id != requestId) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return topology;
}

QueryCatalogServiceHandle TwoPhaseLockingStorageHandler::getQueryCatalogServiceHandle(RequestId requestId) {
    if (queryCatalogServiceHolder.id != requestId) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return queryCatalogService;
}

GlobalQueryPlanHandle TwoPhaseLockingStorageHandler::getGlobalQueryPlanHandle(RequestId requestId) {
    if (globalQueryPlanHolder.id != requestId) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return globalQueryPlan;
}

SourceCatalogHandle TwoPhaseLockingStorageHandler::getSourceCatalogHandle(RequestId requestId) {
    if (sourceCatalogHolder.id != requestId) {
        //todo #3611: write custom exception for this case
        throw std::exception();
    }
    return sourceCatalog;
}

UDFCatalogHandle TwoPhaseLockingStorageHandler::getUDFCatalogHandle(RequestId requestId) {
    if (udfCatalogHolder.id != requestId) {
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
