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
#include <Common/Identifiers.hpp>
#include <Util/Logger/Logger.hpp>
#include <RequestProcessor/RequestTypes/AbstractRequest.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>

namespace NES::RequestProcessor::Experimental {
AbstractRequest::AbstractRequest(const std::vector<ResourceType>& requiredResources, const uint8_t maxRetries)
    : requestId(INVALID_REQUEST_ID), responsePromise(), maxRetries(maxRetries), actualRetries(0),
      requiredResources(requiredResources) {}

std::vector<AbstractRequestPtr> AbstractRequest::handleError(RequestExecutionException& ex,
                                                             const StorageHandlerPtr& storageHandle) {

    //error handling to be performed before rolling back
    preRollbackHandle(ex, storageHandle);

    //roll back the changes made by the failed request
    auto followUpRequests = rollBack(ex, storageHandle);

    //error handling to be performed after rolling back
    postRollbackHandle(ex, storageHandle);
    return followUpRequests;
}

bool AbstractRequest::retry() { return actualRetries++ < maxRetries; }

std::vector<AbstractRequestPtr> AbstractRequest::execute(const StorageHandlerPtr& storageHandle) {
    if (requestId == INVALID_REQUEST_ID) {
        NES_THROW_RUNTIME_ERROR("Trying to execute a request before its id has been set");
    }
    //acquire locks and perform other tasks to prepare for execution
    preExecution(storageHandle);

    //execute the request logic
    auto followUpRequests = executeRequestLogic(storageHandle);

    //release locks
    postExecution(storageHandle);
    return followUpRequests;
}

//template<ConceptResponse ResponseType>
void AbstractRequest::preExecution(const StorageHandlerPtr& storageHandle) {
    storageHandle->acquireResources(requestId, requiredResources);
}

std::future<AbstractRequestResponsePtr> AbstractRequest::getFuture() { return responsePromise.get_future(); }

void AbstractRequest::setId(RequestId requestId) { this->requestId = requestId; }
}// namespace NES
