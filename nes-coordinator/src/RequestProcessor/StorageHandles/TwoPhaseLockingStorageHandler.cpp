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
#include <Exceptions/StorageHandlerAcquireResourcesException.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <RequestProcessor/StorageHandles/TwoPhaseLockingStorageHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>
#include <atomic>

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
    //iterate over all resources
    for (const auto resourceType : resourceTypeList) {
        auto& holder = getHolder(resourceType);

        //if the resouce is not held by the request, proceed to the next resource
        if (holder.holderId != requestId) {
            continue;
        }

        //lock the holder data, increase the ticket number for the ticket to be served and notify all waiting threads
        std::unique_lock lock(holder.mutex);
        holder.holderId = INVALID_REQUEST_ID;
        TicketId nextTicket = (holder.currentTicket + 1) % MAX_TICKET;
        holder.currentTicket = nextTicket;

        //unlocking the mutex before calling notify_all to prevent unnecessary wake-ups of waiting threads.
        //see: https://en.cppreference.com/w/cpp/thread/condition_variable/notify_all
        lock.unlock();
        holder.cv.notify_all();
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
    std::unique_lock lock(holder.mutex);
    auto myTicket = holder.nextAvailableTicket;
    holder.nextAvailableTicket = (holder.nextAvailableTicket + 1) % MAX_TICKET;

    //wait until the resource is free
    while (holder.currentTicket != myTicket) {
        holder.cv.wait(lock);
    }

    //set holder id
    NES_ASSERT(holderId == INVALID_REQUEST_ID, "Ticket acquired by new request but the holder id was not reset");
    holderId = requestId;
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

TicketId TwoPhaseLockingStorageHandler::getCurrentTicket(ResourceType resource) {
    return getHolder(resource).currentTicket;
}

TicketId TwoPhaseLockingStorageHandler::getNextAvailableTicket(ResourceType resource) {
    return getHolder(resource).nextAvailableTicket;
}
}// namespace NES::RequestProcessor::Experimental
