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

namespace NES {

AbstractRequest::AbstractRequest(size_t maxRetries) : maxRetries(maxRetries), actualRetries(0) {}

void AbstractRequest::rollBack(std::exception& ex, StorageAccessHandlePtr storageAccessHandle) {
    (void) ex;

    /* check if the storage handle requires a rollback
     * this allows us in the case of an mvcc storage handler to skip a rollback on a transaction specific access handler
     * if the versions it points to will not be used any further
     */
    if (storageAccessHandle->requireseRollback()) {
        //roll back changes
        //use info in exception to see where execution left of, so we know how much we have to roll back
    }
}

void AbstractRequest::handleError(std::exception ex, StorageAccessHandlePtr storageAccessHandle) {
    preRollbackErrorHandling(ex, storageAccessHandle);

    rollBack(ex, storageAccessHandle);

    afterRollbackErrorHandling(ex, storageAccessHandle);
}

bool AbstractRequest::retry() {
    return actualRetries++ < maxRetries;
}

void AbstractRequest::preRollbackErrorHandling(std::exception ex, StorageAccessHandlePtr storageAccessHandle) {
    (void) ex;
    (void) storageAccessHandle;
}

void AbstractRequest::afterRollbackErrorHandling(std::exception ex, StorageAccessHandlePtr storageAccessHandle) {
    (void) ex;
    (void) storageAccessHandle;
}
}