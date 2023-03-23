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
#include <WorkQueues/StorageAccessHandles/StorageAccessHandle.hpp>
#include <WorkQueues/RequestTypes/AbstractRequest.hpp>

namespace NES {

AbstractRequest::AbstractRequest(size_t maxRetries) : maxRetries(maxRetries), actualRetries(0) {}

void AbstractRequest::handleError(std::exception ex, const StorageAccessHandlePtr& storageAccessHandle) {
    preRollbackErrorHandling(ex, storageAccessHandle);

    rollBack(ex, storageAccessHandle);

    afterRollbackErrorHandling(ex, storageAccessHandle);
}

bool AbstractRequest::retry() {
    return actualRetries++ < maxRetries;
}
}