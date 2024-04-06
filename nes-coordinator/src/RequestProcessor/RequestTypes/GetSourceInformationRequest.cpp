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
#include <RequestProcessor/RequestTypes/SourceCatalog/GetSourceInformationRequest.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES::RequestProcessor {
GetSourceInformationRequestPtr GetSourceInformationRequest::create(uint8_t maxRetries) {
    return std::make_shared<GetSourceInformationRequest>(maxRetries);
}
GetSourceInformationRequestPtr GetSourceInformationRequest::create(SourceType sourceType, std::string sourceName, uint8_t maxRetries) {
    return std::make_shared<GetSourceInformationRequest>(sourceType, sourceName, maxRetries);
}
GetSourceInformationRequest::GetSourceInformationRequest(uint8_t maxRetries)
    : AbstractUniRequest({ResourceType::SourceCatalog}, maxRetries),
      sourceInformationType(SourceType::LOGICAL_SOURCE) {}

GetSourceInformationRequest::GetSourceInformationRequest(SourceType sourceInformationType,
                                                         std::string sourceName,
                                                         uint8_t maxRetries)
    : AbstractUniRequest({ResourceType::SourceCatalog}, maxRetries), sourceInformationType(sourceInformationType),
      sourceName(sourceName) {}

std::vector<AbstractRequestPtr> GetSourceInformationRequest::executeRequestLogic(const StorageHandlerPtr& storageHandle) {
    auto catalogHandle = storageHandle->getSourceCatalogHandle(requestId);
    try {
        switch (sourceInformationType) {
            case SourceType::LOGICAL_SOURCE: {
                if (sourceName.has_value()) {
                    auto logicalSource = catalogHandle->getLogicalSourceAsJson(sourceName.value());
                    if (logicalSource.empty()) {
                        NES_ERROR("Failed to get logical source: {}", sourceName.value());
                        throw Exceptions::RuntimeException("Required source does not exist " + sourceName.value());
                        break;
                    }
                    NES_DEBUG("Got logical source: {}", logicalSource);
                    responsePromise.set_value(std::make_shared<GetSourceInformationResponse>(logicalSource));
                    break;
                } else {
                    //return all logical sources as json via promise
                    auto logicalSources = catalogHandle->getAllLogicalSourcesAsJson();
                    NES_DEBUG("Got all logical sources: {}", logicalSources);
                    responsePromise.set_value(std::make_shared<GetSourceInformationResponse>(logicalSources));
                    break;
                }
            }
            case SourceType::PHYSICAL_SOURCE: {
                //get physical sources for logical source
                auto physicalSources = catalogHandle->getPhysicalSourcesAsJson(sourceName.value());
                NES_DEBUG("Got physical sources for logical source {}: {}", sourceName.value(), physicalSources);
                responsePromise.set_value(std::make_shared<GetSourceInformationResponse>(physicalSources));
            }
        }
    } catch (std::exception& e) {
        NES_ERROR("Failed to get source information: {}", e.what());
        responsePromise.set_exception(std::make_exception_ptr(e));
    }

    return {};
}
std::vector<AbstractRequestPtr> GetSourceInformationRequest::rollBack(std::exception_ptr,
                                                                      const StorageHandlerPtr&) {
    return {};
}
void GetSourceInformationRequest::preRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {}
void GetSourceInformationRequest::postRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {}
}// namespace NES::RequestProcessor