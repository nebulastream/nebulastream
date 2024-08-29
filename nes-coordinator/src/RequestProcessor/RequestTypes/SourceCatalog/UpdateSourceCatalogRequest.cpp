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
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/AddKeyDistributionEntryEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/AddLogicalSourceEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/AddPhysicalSourcesEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/RemoveAllPhysicalSourcesByWorkerEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/RemoveLogicalSourceEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/RemovePhysicalSourceEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/UpdateLogicalSourceEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/UpdateSourceCatalogRequest.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <memory>

namespace NES::RequestProcessor {
UpdateSourceCatalogRequest::UpdateSourceCatalogRequest(UpdateSourceCatalogEventPtr event, uint8_t maxRetries)
    : AbstractUniRequest({ResourceType::SourceCatalog}, maxRetries), event(event) {}

std::vector<AbstractRequestPtr> UpdateSourceCatalogRequest::executeRequestLogic(const StorageHandlerPtr& storageHandle) {
    auto sourceCatalog = storageHandle->getSourceCatalogHandle(requestId);
    //check if source definitions are logical or physical
    try {
        if (event->instanceOf<AddPhysicalSourcesEvent>()) {
            auto addPhysicalSourceEvent = event->as<AddPhysicalSourcesEvent>();
            auto physicalSourceDefinitions = addPhysicalSourceEvent->getPhysicalSources();
            std::vector<std::string> successfulAdditions;
            for (const auto& physicalSourceDefinition : physicalSourceDefinitions) {
                //if there is no parameter for physicalSourceTypeName
                if (physicalSourceDefinition.physicalSourceTypeName == "") {
                    //register physical source
                    if (!sourceCatalog
                             ->addPhysicalSource(physicalSourceDefinition.physicalSourceName,
                                                 physicalSourceDefinition.logicalSourceName,
                                                 addPhysicalSourceEvent->getWorkerId())
                             .first) {
                        NES_ERROR("Failed to register physical source: {} for logical source: {}",
                                  physicalSourceDefinition.physicalSourceName,
                                  physicalSourceDefinition.logicalSourceName);
                        responsePromise.set_value(
                            std::make_shared<AddPhysicalSourcesResponse>(false,
                                                                         successfulAdditions,
                                                                         physicalSourceDefinition.physicalSourceName));
                        return {};
                    }
                    successfulAdditions.push_back(physicalSourceDefinition.physicalSourceName);
                } else {
                    if (!sourceCatalog
                             ->addPhysicalSource(physicalSourceDefinition.physicalSourceName,
                                                 physicalSourceDefinition.logicalSourceName,
                                                 physicalSourceDefinition.physicalSourceTypeName,
                                                 addPhysicalSourceEvent->getWorkerId())
                             .first) {
                        NES_ERROR("Failed to register physical source: {} for logical source: {}",
                                  physicalSourceDefinition.physicalSourceName,
                                  physicalSourceDefinition.logicalSourceName);
                        responsePromise.set_value(
                            std::make_shared<AddPhysicalSourcesResponse>(false,
                                                                         successfulAdditions,
                                                                         physicalSourceDefinition.physicalSourceName));
                        return {};
                    }
                    successfulAdditions.push_back(physicalSourceDefinition.physicalSourceName);
                }
            }
            responsePromise.set_value(std::make_shared<AddPhysicalSourcesResponse>(true, successfulAdditions));
            return {};
        } else if (event->instanceOf<RemovePhysicalSourceEvent>()) {
            auto removePhysicalSourceEvent = event->as<RemovePhysicalSourceEvent>();
            //unregister physical source
            if (!sourceCatalog->removePhysicalSource(removePhysicalSourceEvent->getLogicalSourceName(),
                                                     removePhysicalSourceEvent->getPhysicalSourceName(),
                                                     removePhysicalSourceEvent->getWorkerId())) {
                NES_ERROR("Failed to unregister physical source: {} for logical source: {}",
                          removePhysicalSourceEvent->getPhysicalSourceName(),
                          removePhysicalSourceEvent->getLogicalSourceName());
                responsePromise.set_value(std::make_shared<BaseUpdateSourceCatalogResponse>(false));
                return {};
            }
        } else if (event->instanceOf<RemoveAllPhysicalSourcesByWorkerEvent>()) {
            auto removeAllPhysicalSourcesByWorkerEvent = event->as<RemoveAllPhysicalSourcesByWorkerEvent>();
            NES_DEBUG("RemoveAllPhysicalSourcesByWorkerEvent: remove all physical sources for worker: {}",
                      removeAllPhysicalSourcesByWorkerEvent->getWorkerId());
            sourceCatalog->removeAllPhysicalSourcesByWorker(removeAllPhysicalSourcesByWorkerEvent->getWorkerId());
        } else if (event->instanceOf<AddLogicalSourceEvent>()) {
            auto addLogicalSourceEvent = event->as<AddLogicalSourceEvent>();
            //register logical source
            if (!sourceCatalog->addLogicalSource(addLogicalSourceEvent->getLogicalSourceName(),
                                                 addLogicalSourceEvent->getSchema())) {
                NES_ERROR("Failed to register logical source: {}", addLogicalSourceEvent->getLogicalSourceName());
                responsePromise.set_value(std::make_shared<BaseUpdateSourceCatalogResponse>(false));
                return {};
            }
        } else if (event->instanceOf<RemoveLogicalSourceEvent>()) {
            auto removeLogicalSourceEvent = event->as<RemoveLogicalSourceEvent>();
            //unregister logical source
            if (!sourceCatalog->removeLogicalSource(removeLogicalSourceEvent->getLogicalSourceName())) {
                NES_ERROR("Failed to unregister logical source: {}", removeLogicalSourceEvent->getLogicalSourceName());
                responsePromise.set_value(std::make_shared<BaseUpdateSourceCatalogResponse>(false));
                return {};
            }
        } else if (event->instanceOf<UpdateLogicalSourceEvent>()) {
            auto updateLogicalSourceEvent = event->as<UpdateLogicalSourceEvent>();
            //unregister logical source
            if (!sourceCatalog->updateLogicalSource(updateLogicalSourceEvent->getLogicalSourceName(),
                                                    updateLogicalSourceEvent->getSchema())) {
                NES_ERROR("Failed to update logical source: {}", updateLogicalSourceEvent->getLogicalSourceName());
                responsePromise.set_value(std::make_shared<BaseUpdateSourceCatalogResponse>(false));
                return {};
            }
        } else if (event->instanceOf<AddKeyDistributionEntryEvent>()) {
            auto addKeyDistributionEntryEvent = event->as<AddKeyDistributionEntryEvent>();
            Catalogs::Source::SourceCatalogEntryPtr catalogEntry;
            std::vector<Catalogs::Source::SourceCatalogEntryPtr> physicalSources =
                sourceCatalog->getPhysicalSources(addKeyDistributionEntryEvent->getLogicalSourceName());
            for (const auto& physicalSource : physicalSources) {
                if (physicalSource->getLogicalSource()->getLogicalSourceName()
                        == addKeyDistributionEntryEvent->getLogicalSourceName()
                    && physicalSource->getPhysicalSource()->getPhysicalSourceName()
                        == addKeyDistributionEntryEvent->getPhysicalSourceName()
                    && physicalSource->getTopologyNodeId() == addKeyDistributionEntryEvent->getWorkerId()) {
                    catalogEntry = physicalSource;
                    std::set<uint64_t> vals = {std::stoull(addKeyDistributionEntryEvent->getValue())};
                    if (sourceCatalog->getKeyDistributionMap().contains(catalogEntry)) {
                        NES_WARNING("AddKeyDistributionEntryEvent: entry {} already exists, overwriting values",
                                    catalogEntry->toString());
                    }
                    sourceCatalog->getKeyDistributionMap()[catalogEntry] = std::move(vals);
                    responsePromise.set_value(std::make_shared<AddKeyDistributionEntryResponse>(true));
                    return {};
                }
            }
            responsePromise.set_value(std::make_shared<AddKeyDistributionEntryResponse>(false));
            return {};
        }
        responsePromise.set_value(std::make_shared<BaseUpdateSourceCatalogResponse>(true));
    } catch (std::exception& e) {
        NES_ERROR("Failed to get source information: {}", e.what());
        responsePromise.set_exception(std::make_exception_ptr(e));
    }
    return {};
}

std::vector<AbstractRequestPtr> UpdateSourceCatalogRequest::rollBack(std::exception_ptr, const StorageHandlerPtr&) { return {}; }

void UpdateSourceCatalogRequest::preRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {}

void UpdateSourceCatalogRequest::postRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {}

UpdateSourceCatalogRequestPtr UpdateSourceCatalogRequest::create(UpdateSourceCatalogEventPtr event, uint8_t maxRetries) {
    return std::make_shared<UpdateSourceCatalogRequest>(event, maxRetries);
}
}// namespace NES::RequestProcessor
