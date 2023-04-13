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
#include <WorkQueues/StorageHandles/StorageHandle.hpp>
#include <utility>

namespace NES {

AbstractRequest::AbstractRequest(size_t maxRetries, std::vector<StorageHandleResourceType> requiredResources)
    : maxRetries(maxRetries), actualRetries(0), requiredResources(std::move(requiredResources)) {}

void AbstractRequest::handleError(std::exception ex, const StorageHandlePtr& storageHandle) {
    //error handling to be performed before rolling back
    preRollbackHandle(ex, storageHandle);

    //roll back the changes made by the failed request
    rollBack(ex, storageHandle);

    //error handling to be performed adter rolling back
    postRollbackHandle(ex, storageHandle);
}

bool AbstractRequest::retry() { return actualRetries++ < maxRetries; }

void AbstractRequest::execute(const StorageHandlePtr& storageHandle) {
    //acquire locks and perform other tasks to prepare for execution
    preExecution(storageHandle, requiredResources);

    //execute the request logic
    executeRequestLogic(storageHandle);

    //release locks
    postExecution(storageHandle, requiredResources);
}
}// namespace NES