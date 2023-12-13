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

#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::RequestProcessor {

void StorageHandler::acquireResources(const RequestId, std::vector<ResourceType>) {}

void StorageHandler::releaseResources(const RequestId) {}

GlobalExecutionPlanHandle StorageHandler::getGlobalExecutionPlanHandle(RequestId) { NES_NOT_IMPLEMENTED(); }

TopologyHandle StorageHandler::getTopologyHandle(RequestId) { NES_NOT_IMPLEMENTED(); }

QueryCatalogServiceHandle StorageHandler::getQueryCatalogServiceHandle(RequestId) { NES_NOT_IMPLEMENTED(); }

GlobalQueryPlanHandle StorageHandler::getGlobalQueryPlanHandle(RequestId) { NES_NOT_IMPLEMENTED(); }

SourceCatalogHandle StorageHandler::getSourceCatalogHandle(RequestId) { NES_NOT_IMPLEMENTED(); }

UDFCatalogHandle StorageHandler::getUDFCatalogHandle(RequestId) { NES_NOT_IMPLEMENTED(); }

CoordinatorConfigurationHandle StorageHandler::getCoordinatorConfiguration(RequestId) { NES_NOT_IMPLEMENTED(); }

RequestId StorageHandler::generateRequestId() {
    std::unique_lock lock(idMutex);
    RequestId requestId = nextFreeRequestId;
    nextFreeRequestId = (nextFreeRequestId % MAX_REQUEST_ID) + 1;
    return requestId;
}

}// namespace NES::RequestProcessor::Experimental