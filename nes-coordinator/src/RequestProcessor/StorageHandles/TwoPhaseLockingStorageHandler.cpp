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
#include <Exceptions/AccessNonLockedResourceException.hpp>
#include <Exceptions/ResourceLockingException.hpp>
#include <Exceptions/StorageHandlerAcquireResourcesException.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <RequestProcessor/StorageHandles/TwoPhaseLockingStorageHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::RequestProcessor {

TwoPhaseLockingStorageHandler::TwoPhaseLockingStorageHandler(StorageDataStructures storageDataStructures)
    : coordinatorConfiguration(std::move(storageDataStructures.coordinatorConfiguration)),
      topology(std::move(storageDataStructures.topology)),
      globalExecutionPlan(std::move(storageDataStructures.globalExecutionPlan)),
      queryCatalogService(std::move(storageDataStructures.queryCatalogService)),
      globalQueryPlan(std::move(storageDataStructures.globalQueryPlan)),
      sourceCatalog(std::move(storageDataStructures.sourceCatalog)), udfCatalog(std::move(storageDataStructures.udfCatalog)),
      coordinatorConfigurationHolder(INVALID_REQUEST_ID), topologyHolder(INVALID_REQUEST_ID),
      globalExecutionPlanHolder(INVALID_REQUEST_ID), queryCatalogServiceHolder(INVALID_REQUEST_ID),
      globalQueryPlanHolder(INVALID_REQUEST_ID), sourceCatalogHolder(INVALID_REQUEST_ID), udfCatalogHolder(INVALID_REQUEST_ID) {}

std::shared_ptr<TwoPhaseLockingStorageHandler>
TwoPhaseLockingStorageHandler::create(StorageDataStructures storageDataStructures) {
    return std::make_shared<TwoPhaseLockingStorageHandler>(storageDataStructures);
}

void TwoPhaseLockingStorageHandler::acquireResources(const RequestId requestId, std::vector<ResourceType> requiredResources) {
    //do not allow performing resource acquisition more than once
    if (isLockHolder(requestId)) {
        throw Exceptions::StorageHandlerAcquireResourcesException(
            "Attempting to call acquireResources on a 2 phase locking storage handler but some resources are already locked by "
            "this request");
    }

    //sort the resource list to ensure that resources are acquired in a deterministic order for deadlock prevention
    std::sort(requiredResources.begin(), requiredResources.end());

    //lock the resources
    for (const auto& type : requiredResources) {
        NES_TRACE("Request {} trying to acquire resource {}", requestId, (uint8_t) type);
        lockResource(type, requestId);
    }
}

void TwoPhaseLockingStorageHandler::releaseResources(const RequestId requestId) {
    for (const auto resourceType : resourceTypeList) {
        auto& holder = getHolder(resourceType);
        if (holder.holderId != requestId) {
            continue;
        }
        //if the waiting queue is not empty, signal to the next request in line, that it can overwrite this holders id
        std::unique_lock lock(holder.queueMutex);
        auto& queue = holder.waitingQueue;
        holder.holderId = INVALID_REQUEST_ID;

        //if another request is waiting on this resource, we pass the lock held by this request to the waiting one
        if (!queue.empty()) {
            auto promise = std::move(queue.front());
            queue.pop_front();
            promise.set_value(std::move(lock));
        }
    }
}

bool TwoPhaseLockingStorageHandler::isLockHolder(const RequestId requestId) {
    if (std::any_of(resourceTypeList.cbegin(), resourceTypeList.cend(), [this, requestId](const ResourceType resourceType) {
            return this->getHolder(resourceType).holderId == requestId;
        })) {
        return true;
    }
    return false;
}

TwoPhaseLockingStorageHandler::ResourceHolderData& TwoPhaseLockingStorageHandler::getHolder(const ResourceType resourceType) {
    switch (resourceType) {
        case ResourceType::CoordinatorConfiguration: return coordinatorConfigurationHolder;
        case ResourceType::Topology: return topologyHolder;
        case ResourceType::QueryCatalogService: return queryCatalogServiceHolder;
        case ResourceType::SourceCatalog: return sourceCatalogHolder;
        case ResourceType::GlobalExecutionPlan: return globalExecutionPlanHolder;
        case ResourceType::GlobalQueryPlan: return globalQueryPlanHolder;
        case ResourceType::UdfCatalog: return udfCatalogHolder;
    }
}

void TwoPhaseLockingStorageHandler::lockResource(const ResourceType resourceType, const RequestId requestId) {
    auto& holder = getHolder(resourceType);
    auto& holderId = holder.holderId;
    std::unique_lock lock(holder.queueMutex);

    //check if the resource is free
    if (holderId == INVALID_REQUEST_ID) {
        holderId = requestId;
        return;
    }

    //if another thread holds the resource, insert a promise into the queue and weit for previous requests to fullfill that promise
    std::promise<std::unique_lock<std::mutex>> promise;
    auto future = promise.get_future();
    holder.waitingQueue.push_back(std::move(promise));
    lock.unlock();

    //wait for previous request to pass the lock and then set this requests id as the holder id
    auto newLock = future.get();
    NES_ASSERT(holderId == INVALID_REQUEST_ID, "Request id does not match holder id");
    holderId = requestId;
    newLock.unlock();
}

GlobalExecutionPlanHandle TwoPhaseLockingStorageHandler::getGlobalExecutionPlanHandle(const RequestId requestId) {
    if (globalExecutionPlanHolder.holderId != requestId) {
        throw Exceptions::AccessNonLockedResourceException("Attempting to access resource which has not been locked",
                                                           ResourceType::GlobalExecutionPlan);
    }
    return globalExecutionPlan;
}

TopologyHandle TwoPhaseLockingStorageHandler::getTopologyHandle(const RequestId requestId) {
    if (topologyHolder.holderId != requestId) {
        throw Exceptions::AccessNonLockedResourceException("Attempting to access resource which has not been locked",
                                                           ResourceType::Topology);
    }
    return topology;
}

QueryCatalogServiceHandle TwoPhaseLockingStorageHandler::getQueryCatalogServiceHandle(const RequestId requestId) {
    if (queryCatalogServiceHolder.holderId != requestId) {
        throw Exceptions::AccessNonLockedResourceException("Attempting to access resource which has not been locked",
                                                           ResourceType::QueryCatalogService);
    }
    return queryCatalogService;
}

GlobalQueryPlanHandle TwoPhaseLockingStorageHandler::getGlobalQueryPlanHandle(const RequestId requestId) {
    if (globalQueryPlanHolder.holderId != requestId) {
        throw Exceptions::AccessNonLockedResourceException("Attempting to access resource which has not been locked",
                                                           ResourceType::GlobalQueryPlan);
    }
    return globalQueryPlan;
}

SourceCatalogHandle TwoPhaseLockingStorageHandler::getSourceCatalogHandle(const RequestId requestId) {
    if (sourceCatalogHolder.holderId != requestId) {
        throw Exceptions::AccessNonLockedResourceException("Attempting to access resource which has not been locked",
                                                           ResourceType::SourceCatalog);
    }
    return sourceCatalog;
}

UDFCatalogHandle TwoPhaseLockingStorageHandler::getUDFCatalogHandle(const RequestId requestId) {
    if (udfCatalogHolder.holderId != requestId) {
        throw Exceptions::AccessNonLockedResourceException("Attempting to access resource which has not been locked",
                                                           ResourceType::UdfCatalog);
    }
    return udfCatalog;
}

CoordinatorConfigurationHandle TwoPhaseLockingStorageHandler::getCoordinatorConfiguration(RequestId requestId) {
    if (coordinatorConfigurationHolder.holderId != requestId) {
        throw Exceptions::AccessNonLockedResourceException("Attempting to access resource which has not been locked",
                                                           ResourceType::CoordinatorConfiguration);
    }
    return coordinatorConfiguration;
}

uint32_t TwoPhaseLockingStorageHandler::getWaitingCount(ResourceType resource) {
    auto& holder = getHolder(resource);
    std::unique_lock lock(holder.queueMutex);
    return holder.waitingQueue.size();
}
}// namespace NES::RequestProcessor::Experimental
