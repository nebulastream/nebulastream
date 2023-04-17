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
#include <WorkQueues/StorageHandles/StorageHandler.hpp>
#include <utility>

namespace NES {

AbstractRequest::AbstractRequest(size_t maxRetries)
    : maxRetries(maxRetries), actualRetries(0) {}

void AbstractRequest::handleError(std::exception ex, const StorageHandlerPtr& storageHandle) {
    //error handling to be performed before rolling back
    preRollbackHandle(ex, storageHandle);

    //roll back the changes made by the failed request
    rollBack(ex, storageHandle);

    //error handling to be performed after rolling back
    postRollbackHandle(ex, storageHandle);
}

bool AbstractRequest::retry() { return actualRetries++ < maxRetries; }

void AbstractRequest::execute(const StorageHandlerPtr& storageHandle) {
    //acquire locks and perform other tasks to prepare for execution
    preExecution(storageHandle);

    //execute the request logic
    executeRequestLogic(storageHandle);

    //release locks
    postExecution(storageHandle);
}

void AbstractRequest::preExecution(const StorageHandlerPtr& storageHandle) { storageHandle->acquireResources(requiredResources);
}
}// namespace NES