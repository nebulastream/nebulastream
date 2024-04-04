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
#include <RequestProcessor/RequestTypes/UpdateSourceCatalogRequest.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <memory>

namespace NES::RequestProcessor {

UpdateSourceCatalogRequest::UpdateSourceCatalogRequest(SourceActionVector physicalSourceDefinitions, uint8_t maxRetries)
    : AbstractUniRequest({ResourceType::SourceCatalog}, maxRetries), sourceDefinitions(physicalSourceDefinitions) {}

UpdateSourceCatalogRequest::UpdateSourceCatalogRequest(std::vector<PhysicalSourceAddition> physicalSourceDefinitions,
                                                       uint8_t maxRetries)
    : AbstractUniRequest({ResourceType::SourceCatalog}, maxRetries), sourceDefinitions(physicalSourceDefinitions) {}

UpdateSourceCatalogRequest::UpdateSourceCatalogRequest(std::vector<PhysicalSourceRemoval> physicalSourceDefinitions,
                                                       uint8_t maxRetries)
    : AbstractUniRequest({ResourceType::SourceCatalog}, maxRetries), sourceDefinitions(physicalSourceDefinitions) {}

UpdateSourceCatalogRequest::UpdateSourceCatalogRequest(std::vector<LogicalSourceAddition> logicalSourceDefinitions,
                                                       uint8_t maxRetries)
    : AbstractUniRequest({ResourceType::SourceCatalog}, maxRetries), sourceDefinitions(logicalSourceDefinitions) {}

UpdateSourceCatalogRequest::UpdateSourceCatalogRequest(std::vector<LogicalSourceRemoval> logicalSourceDefinitions,
                                                       uint8_t maxRetries)
    : AbstractUniRequest({ResourceType::SourceCatalog}, maxRetries), sourceDefinitions(logicalSourceDefinitions) {}

std::vector<AbstractRequestPtr> UpdateSourceCatalogRequest::executeRequestLogic(const StorageHandlerPtr& storageHandle) {
    auto catalogHandle = storageHandle->getSourceCatalogHandle(requestId);
    //check if source definitions are logical or physical
    //todo: error handling
    if (std::holds_alternative<std::vector<PhysicalSourceAddition>>(sourceDefinitions)) {
        auto physicalSourceDefinitions = std::get<std::vector<PhysicalSourceAddition>>(sourceDefinitions);
        for (const auto& physicalSourceDefinition : physicalSourceDefinitions) {
            //register physical source
            if (!catalogHandle->registerPhysicalSource(physicalSourceDefinition.physicalSourceName,
                                                       physicalSourceDefinition.logicalSourceName,
                                                       physicalSourceDefinition.workerId)) {
                NES_ERROR("Failed to register physical source: {} for logical source: {}",
                          physicalSourceDefinition.physicalSourceName,
                          physicalSourceDefinition.logicalSourceName);
            }
            break;
        }
    } else if (std::holds_alternative<std::vector<PhysicalSourceRemoval>>(sourceDefinitions)) {
        auto physicalSourceDefinitions = std::get<std::vector<PhysicalSourceRemoval>>(sourceDefinitions);
        for (const auto& physicalSourceDefinition : physicalSourceDefinitions) {
            //unregister physical source
            if (!catalogHandle->removePhysicalSource(physicalSourceDefinition.physicalSourceName,
                                                     physicalSourceDefinition.logicalSourceName,
                                                     physicalSourceDefinition.workeId)) {
                NES_ERROR("Failed to unregister physical source: {} for logical source: {}",
                          physicalSourceDefinition.physicalSourceName,
                          physicalSourceDefinition.logicalSourceName);
            }
        }
    } else if (std::holds_alternative<std::vector<LogicalSourceAddition>>(sourceDefinitions)) {
        auto logicalSourceDefinitions = std::get<std::vector<LogicalSourceAddition>>(sourceDefinitions);
        for (const auto& logicalSourceDefinition : logicalSourceDefinitions) {
            //register logical source
            if (!catalogHandle->addLogicalSource(logicalSourceDefinition.logicalSourceName, logicalSourceDefinition.schema)) {
                NES_ERROR("Failed to register logical source: {}", logicalSourceDefinition.logicalSourceName);
            }
        }
    } else if (std::holds_alternative<std::vector<LogicalSourceRemoval>>(sourceDefinitions)) {
        auto logicalSourceDefinitions = std::get<std::vector<LogicalSourceRemoval>>(sourceDefinitions);
        for (const auto& logicalSourceDefinition : logicalSourceDefinitions) {
            //unregister logical source
            if (!catalogHandle->removeLogicalSource(logicalSourceDefinition.logicalSourceName)) {
                NES_ERROR("Failed to unregister logical source: {}", logicalSourceDefinition.logicalSourceName);
            }
        }
    } else if (std::holds_alternative<std::vector<LogicalSourceUpdate>>(sourceDefinitions)) {
        auto logicalSourceDefinitions = std::get<std::vector<LogicalSourceUpdate>>(sourceDefinitions);
        for (const auto& logicalSourceDefinition : logicalSourceDefinitions) {
            //unregister logical source
            if (!catalogHandle->updateLogicalSource(logicalSourceDefinition.logicalSourceName, logicalSourceDefinition.schema)) {
                NES_ERROR("Failed to unregister logical source: {}", logicalSourceDefinition.logicalSourceName);
            }
        }
    }
    responsePromise.set_value(std::make_shared<UpdateSourceCatalogResponse>(true));
    return {};
}
std::vector<AbstractRequestPtr> UpdateSourceCatalogRequest::rollBack(std::exception_ptr, const StorageHandlerPtr&) { return {}; }
void UpdateSourceCatalogRequest::preRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {}
void UpdateSourceCatalogRequest::postRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {}

UpdateSourceCatalogRequestPtr UpdateSourceCatalogRequest::create(SourceActionVector sourceActions, uint8_t maxRetries) {
    return std::make_shared<UpdateSourceCatalogRequest>(sourceActions, maxRetries);
}
}// namespace NES::RequestProcessor