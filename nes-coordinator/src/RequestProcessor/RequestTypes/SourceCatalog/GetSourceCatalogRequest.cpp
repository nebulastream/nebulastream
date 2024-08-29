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
#include <RequestProcessor/RequestTypes/SourceCatalog/GetSourceCatalogRequest.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/BaseGetSourceCatalogEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/GetAllLogicalSourcesEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/GetPhysicalSourcesEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/GetSchemaEvent.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::RequestProcessor {
GetSourceCatalogRequestPtr GetSourceCatalogRequest::create(GetSourceCatalogEventPtr event, uint8_t maxRetries) {
    return std::make_shared<GetSourceCatalogRequest>(event, maxRetries);
}

GetSourceCatalogRequest::GetSourceCatalogRequest(GetSourceCatalogEventPtr event, uint8_t maxRetries)
    : AbstractUniRequest({ResourceType::SourceCatalog}, maxRetries), event(event) {}

std::vector<AbstractRequestPtr> GetSourceCatalogRequest::executeRequestLogic(const StorageHandlerPtr& storageHandle) {
    auto sourceCatalog = storageHandle->getSourceCatalogHandle(requestId);
    try {
        if (event->instanceOf<GetSchemaEvent>()) {
            auto getSchemaEvent = event->as<GetSchemaEvent>();
            auto schema =
                sourceCatalog->getLogicalSourceOrThrowException(getSchemaEvent->getLogicalSourceName())->getSchema()->copy();
            responsePromise.set_value(std::make_shared<GetSchemaResponse>(true, schema));
            return {};
        } else if (event->instanceOf<GetAllLogicalSourcesEvent>()) {
            //return all logical sources as json via promise
            auto logicalSources = sourceCatalog->getAllLogicalSourcesAsJson();
            NES_DEBUG("Got logical sources: {}", logicalSources.dump());
            responsePromise.set_value(std::make_shared<BaseGetSourceJsonResponse>(true, logicalSources));
            return {};
        } else if (event->instanceOf<GetPhysicalSourcesEvent>()) {
            auto getPhysicalSourcesEvent = event->as<GetPhysicalSourcesEvent>();
            //get physical sources for logical source
            auto physicalSources = sourceCatalog->getPhysicalSourcesAsJson(getPhysicalSourcesEvent->getLogicalSourceName());
            NES_DEBUG("Got physical sources for logical source {}: {}",
                      getPhysicalSourcesEvent->getLogicalSourceName(),
                      physicalSources.dump());
            responsePromise.set_value(std::make_shared<BaseGetSourceJsonResponse>(true, physicalSources));
        }
    } catch (std::exception& e) {
        NES_ERROR("Failed to get source information: {}", e.what());
        responsePromise.set_exception(std::make_exception_ptr(e));
    }

    return {};
}

std::vector<AbstractRequestPtr> GetSourceCatalogRequest::rollBack(std::exception_ptr, const StorageHandlerPtr&) { return {}; }

void GetSourceCatalogRequest::preRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {}

void GetSourceCatalogRequest::postRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) {}
}// namespace NES::RequestProcessor
