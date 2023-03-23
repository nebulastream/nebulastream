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
#include <WorkQueues/StorageHandles/StorageHandle.hpp>
#include <WorkQueues/RequestTypes/AbstractRequest.hpp>
#include <utility>

namespace NES {

AbstractRequest::AbstractRequest(size_t maxRetries, std::vector<ResourceType> requiredResources) : maxRetries(maxRetries),
                                                                                                   actualRetries(0),
                                                                                                   requiredResources(std::move(requiredResources)) {}

void AbstractRequest::handleError(std::exception ex, const StorageAccessHandlePtr& storageHandle) {
    //error handling to be performed before rolling back
    preRollbackHandle(ex, storageHandle);

    rollBack(ex, storageHandle);

    postRollbackHandle(ex, storageHandle);
}

bool AbstractRequest::retry() {
    return actualRetries++ < maxRetries;
}
void AbstractRequest::execute(const StorageAccessHandlePtr& storageHandle) {
    preExecution(storageHandle, requiredResources);

    executeRequestLogic(storageHandle);

    postExecution(storageHandle, requiredResources);
}
}