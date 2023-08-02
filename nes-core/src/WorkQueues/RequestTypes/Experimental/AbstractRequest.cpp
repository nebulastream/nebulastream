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
#include <WorkQueues/RequestTypes/AbstractRequest.hpp>
#include <Exceptions/RequestExecutionException.hpp>
#include <WorkQueues/StorageHandles/ResourceType.hpp>
#include <WorkQueues/StorageHandles/StorageHandler.hpp>
namespace NES {

//template<ConceptResponse ResponseType>
AbstractRequest::AbstractRequest(RequestId requestId,
                                 const std::vector<ResourceType>& requiredResources,
                                 const uint8_t maxRetries)
    : requestId(requestId), responsePromise({}), maxRetries(maxRetries), actualRetries(0),
      requiredResources(requiredResources) {}

//template<ConceptResponse ResponseType>
std::vector<AbstractRequestPtr> AbstractRequest::handleError(const RequestExecutionException& ex, StorageHandler& storageHandle) {
    //error handling to be performed before rolling back
    preRollbackHandle(ex, storageHandle);

    //roll back the changes made by the failed request
    auto followUpRequests = rollBack(ex, storageHandle);

    //error handling to be performed after rolling back
    postRollbackHandle(ex, storageHandle);
    return followUpRequests;
}

//template<ConceptResponse ResponseType>
bool AbstractRequest::retry() { return actualRetries++ < maxRetries; }

//template<ConceptResponse ResponseType>
std::vector<AbstractRequestPtr> AbstractRequest::execute(StorageHandler& storageHandle) {
    //acquire locks and perform other tasks to prepare for execution
    preExecution(storageHandle);

    //execute the request logic
    auto followUpRequests = executeRequestLogic(storageHandle);

    //release locks
    postExecution(storageHandle);
    return followUpRequests;
}

//template<ConceptResponse ResponseType>
void AbstractRequest::preExecution(StorageHandler& storageHandle) {
    storageHandle.acquireResources(requestId, requiredResources);
}

std::future<AbstractRequestResponsePtr> AbstractRequest::makeFuture() { return responsePromise.get_future(); }
}// namespace NES
