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

#include <Catalogs/Source/SourceCatalog.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/AddLogicalSourceEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/AddPhysicalSourcesEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/RemoveLogicalSourceEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/RemovePhysicalSourceEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/UpdateLogicalSourceEvent.hpp>
#include <RequestProcessor/RequestTypes/UpdateSourceCatalogRequest.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <memory>

namespace NES::RequestProcessor {

UpdateSourceCatalogRequest::UpdateSourceCatalogRequest(SourceCatalogEventPtr event, uint8_t maxRetries)
    : AbstractUniRequest({ResourceType::SourceCatalog}, maxRetries), event(event) {}

std::vector<AbstractRequestPtr> UpdateSourceCatalogRequest::executeRequestLogic(const StorageHandlerPtr& storageHandle) {
    auto catalogHandle = storageHandle->getSourceCatalogHandle(requestId);
    //check if source definitions are logical or physical
    try {
        if (event->insteanceOf<AddPhysicalSourcesEvent>()) {
            auto addPhysicalSourceEvent = event->as<AddPhysicalSourcesEvent>();
            auto physicalSourceDefinitions = addPhysicalSourceEvent->getPhysicalSources();
            std::vector<std::string> succesful;
            for (const auto& physicalSourceDefinition : physicalSourceDefinitions) {
                //register physical source
                if (!catalogHandle->registerPhysicalSource(physicalSourceDefinition.physicalSourceName,
                                                           physicalSourceDefinition.logicalSourceName,
                                                           addPhysicalSourceEvent->getWorkerId())) {
                    NES_ERROR("Failed to register physical source: {} for logical source: {}",
                              physicalSourceDefinition.physicalSourceName,
                              physicalSourceDefinition.logicalSourceName);
                    responsePromise.set_value(
                        std::make_shared<AddPhysicalSourcesResponse>(false,
                                                                     succesful,
                                                                     physicalSourceDefinition.physicalSourceName));
                    return {};
                }
                succesful.push_back(physicalSourceDefinition.physicalSourceName);
            }
        } else if (event->insteanceOf<RemovePhysicalSourceEvent>()) {
            auto removePhysicalSourceEvent = event->as<RemovePhysicalSourceEvent>();
            //unregister physical source
            if (!catalogHandle->removePhysicalSource(removePhysicalSourceEvent->getLogicalSourceName(),
                                                     removePhysicalSourceEvent->getPhysicalSourceName(),
                                                     removePhysicalSourceEvent->getWorkerId())) {
                NES_ERROR("Failed to unregister physical source: {} for logical source: {}",
                          removePhysicalSourceEvent->getPhysicalSourceName(),
                          removePhysicalSourceEvent->getLogicalSourceName());
                responsePromise.set_value(std::make_shared<SourceCatalogResponse>(false));
                return {};
            }
        } else if (event->insteanceOf<AddLogicalSourceEvent>()) {
            auto addLogicalSourceEvent = event->as<AddLogicalSourceEvent>();
            //register logical source
            if (!catalogHandle->addLogicalSource(addLogicalSourceEvent->getLogicalSourceName(),
                                                 addLogicalSourceEvent->getSchema())) {
                NES_ERROR("Failed to register logical source: {}", addLogicalSourceEvent->getLogicalSourceName());
                responsePromise.set_value(std::make_shared<SourceCatalogResponse>(false));
                return {};
            }
        } else if (event->insteanceOf<RemoveLogicalSourceEvent>()) {
            auto removeLogicalSourceEvent = event->as<RemoveLogicalSourceEvent>();
            //unregister logical source
            if (!catalogHandle->removeLogicalSource(removeLogicalSourceEvent->getLogicalSourceName())) {
                NES_ERROR("Failed to unregister logical source: {}", removeLogicalSourceEvent->getLogicalSourceName());
                responsePromise.set_value(std::make_shared<SourceCatalogResponse>(false));
                return {};
            }
        } else if (event->insteanceOf<UpdateLogicalSourceEvent>()) {
            auto updateLogicalSourceEvent = event->as<UpdateLogicalSourceEvent>();
            //unregister logical source
            if (!catalogHandle->updateLogicalSource(updateLogicalSourceEvent->getLogicalSourceName(), updateLogicalSourceEvent->getSchema())) {
                NES_ERROR("Failed to update logical source: {}", updateLogicalSourceEvent->getLogicalSourceName());
                responsePromise.set_value(std::make_shared<SourceCatalogResponse>(false));
                return {};
            }
        }
        responsePromise.set_value(std::make_shared<SourceCatalogResponse>(true));
    } catch (std::exception& e) {
        NES_ERROR("Failed to get source information: {}", e.what());
        responsePromise.set_exception(std::make_exception_ptr(e));
    }
    return {};
}

std::vector<AbstractRequestPtr> UpdateSourceCatalogRequest::rollBack(std::exception_ptr, const StorageHandlerPtr&) { return {}; }

void UpdateSourceCatalogRequest::preRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {}

void UpdateSourceCatalogRequest::postRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {}

UpdateSourceCatalogRequestPtr UpdateSourceCatalogRequest::create(SourceCatalogEventPtr event, uint8_t maxRetries) {
    return std::make_shared<UpdateSourceCatalogRequest>(event, maxRetries);
}
}// namespace NES::RequestProcessor