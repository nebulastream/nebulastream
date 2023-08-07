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
#include <Util/Logger/Logger.hpp>
#include <WorkQueues/StorageHandles/ResourceType.hpp>
#include <WorkQueues/StorageHandles/StorageDataStructures.hpp>
#include <WorkQueues/StorageHandles/TwoPhaseLockingStorageHandler.hpp>
#include <utility>

namespace NES {

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

void TwoPhaseLockingStorageHandler::acquireResources(const RequestId requestId,
                                                     const std::vector<ResourceType>& requiredResources) {
    //do not allow performing resource acquisition more than once
    if (isLockHolder(requestId)) {
        throw Exceptions::StorageHandlerAcquireResourcesException(
            "Attempting to call acquireResources on a 2 phase locking storage handler but some resources are already locked by "
            "this request");
    }

    //lock the resources
    for (const auto& type : requiredResources) {
        NES_TRACE("Request {} trying to acquire resource {}", requestId, (uint8_t) type);
        lockResource(type, requestId);
    }
}

void TwoPhaseLockingStorageHandler::releaseResources(const RequestId requestId) {
    for (const auto resourceType : resourceTypeList) {
        auto& holder = getHolder(resourceType);
        auto expected = requestId;
        holder.compare_exchange_strong(expected, INVALID_REQUEST_ID);
    }
}

bool TwoPhaseLockingStorageHandler::isLockHolder(const RequestId requestId) {
    if (std::any_of(resourceTypeList.cbegin(), resourceTypeList.cend(), [this, requestId](const ResourceType resourceType) {
            return this->getHolder(resourceType) == requestId;
        })) {
        return true;
    }
    return false;
}

std::atomic<RequestId>& TwoPhaseLockingStorageHandler::getHolder(const ResourceType resourceType) {
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
    auto expected = INVALID_REQUEST_ID;
    if (!getHolder(resourceType).compare_exchange_strong(expected, requestId)) {
        throw Exceptions::ResourceLockingException("Failed to lock resource", resourceType);
    }
}

GlobalExecutionPlanHandle TwoPhaseLockingStorageHandler::getGlobalExecutionPlanHandle(const RequestId requestId) {
    if (globalExecutionPlanHolder != requestId) {
        throw Exceptions::AccessNonLockedResourceException("Attempting to access resource which has not been locked",
                                                           ResourceType::GlobalExecutionPlan);
    }
    return globalExecutionPlan;
}

TopologyHandle TwoPhaseLockingStorageHandler::getTopologyHandle(const RequestId requestId) {
    if (topologyHolder != requestId) {
        throw Exceptions::AccessNonLockedResourceException("Attempting to access resource which has not been locked",
                                                           ResourceType::Topology);
    }
    return topology;
}

QueryCatalogServiceHandle TwoPhaseLockingStorageHandler::getQueryCatalogServiceHandle(const RequestId requestId) {
    if (queryCatalogServiceHolder != requestId) {
        throw Exceptions::AccessNonLockedResourceException("Attempting to access resource which has not been locked",
                                                           ResourceType::QueryCatalogService);
    }
    return queryCatalogService;
}

GlobalQueryPlanHandle TwoPhaseLockingStorageHandler::getGlobalQueryPlanHandle(const RequestId requestId) {
    if (globalQueryPlanHolder != requestId) {
        throw Exceptions::AccessNonLockedResourceException("Attempting to access resource which has not been locked",
                                                           ResourceType::GlobalQueryPlan);
    }
    return globalQueryPlan;
}

SourceCatalogHandle TwoPhaseLockingStorageHandler::getSourceCatalogHandle(const RequestId requestId) {
    if (sourceCatalogHolder != requestId) {
        throw Exceptions::AccessNonLockedResourceException("Attempting to access resource which has not been locked",
                                                           ResourceType::SourceCatalog);
    }
    return sourceCatalog;
}

UDFCatalogHandle TwoPhaseLockingStorageHandler::getUDFCatalogHandle(const RequestId requestId) {
    if (udfCatalogHolder != requestId) {
        throw Exceptions::AccessNonLockedResourceException("Attempting to access resource which has not been locked",
                                                           ResourceType::UdfCatalog);
    }
    return udfCatalog;
}

CoordinatorConfigurationHandle TwoPhaseLockingStorageHandler::getCoordinatorConfiguration(RequestId requestId) {
    if (coordinatorConfigurationHolder != requestId) {
        throw Exceptions::AccessNonLockedResourceException("Attempting to access resource which has not been locked",
                                                           ResourceType::CoordinatorConfiguration);
    }
    return coordinatorConfiguration;
}
}// namespace NES
