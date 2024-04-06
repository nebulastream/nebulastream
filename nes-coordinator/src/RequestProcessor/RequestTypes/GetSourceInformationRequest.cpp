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
#include <Catalogs/Source/SourceCatalog.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/GetSourceInformationRequest.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/GetAllLogicalSourcesEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/GetPhysicalSourcesEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/GetSchemaEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/GetSourceInformationEvent.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES::RequestProcessor {

GetSourceInformationRequestPtr GetSourceInformationRequest::create(GetSourceInformationEventPtr event, uint8_t maxRetries) {
    return std::make_shared<GetSourceInformationRequest>(event, maxRetries);
}
GetSourceInformationRequest::GetSourceInformationRequest(GetSourceInformationEventPtr event, uint8_t maxRetries)
    : AbstractUniRequest({ResourceType::SourceCatalog}, maxRetries), event(event) {}

std::vector<AbstractRequestPtr> GetSourceInformationRequest::executeRequestLogic(const StorageHandlerPtr& storageHandle) {
    auto catalogHandle = storageHandle->getSourceCatalogHandle(requestId);
    try {
        if (event->insteanceOf<GetSchemaEvent>()) {
            auto getSchemaEvent = event->as<GetSchemaEvent>();
            auto schema = catalogHandle->getLogicalSourceOrThrowException(getSchemaEvent->getLogicalSourceName())->getSchema();
            responsePromise.set_value(std::make_shared<GetSchemaResponse>(true, schema));
            return {};
        } else if (event->insteanceOf<GetAllLogicalSourcesEvent>()) {
            //return all logical sources as json via promise
            auto logicalSources = catalogHandle->getAllLogicalSourcesAsJson();
            NES_DEBUG("Got logical sources: {}", logicalSources.dump());
            responsePromise.set_value(std::make_shared<GetSourceJsonResponse>(true, logicalSources));
            return {};
        }
        else if (event->insteanceOf<GetPhysicalSourcesEvent>()) {
            auto getPhysicalSourcesEvent = event->as<GetPhysicalSourcesEvent>();
            //get physical sources for logical source
            auto physicalSources = catalogHandle->getPhysicalSourcesAsJson(getPhysicalSourcesEvent->getLogicalSourceName());
            NES_DEBUG("Got physical sources for logical source {}: {}", getPhysicalSourcesEvent->getLogicalSourceName(), physicalSources.dump());
            responsePromise.set_value(std::make_shared<GetSourceJsonResponse>(true, physicalSources));
        }
    } catch (std::exception& e) {
        NES_ERROR("Failed to get source information: {}", e.what());
        responsePromise.set_exception(std::make_exception_ptr(e));
    }

    return {};
}
std::vector<AbstractRequestPtr> GetSourceInformationRequest::rollBack(std::exception_ptr, const StorageHandlerPtr&) { return {}; }
void GetSourceInformationRequest::preRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {}
void GetSourceInformationRequest::postRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {}
}// namespace NES::RequestProcessor