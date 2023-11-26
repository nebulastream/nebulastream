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
#include <Exceptions/RequestExecutionException.hpp>
#include <Identifiers.hpp>
#include <RequestProcessor/RequestTypes/AbstractRequest.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::RequestProcessor {
AbstractRequest::AbstractRequest(const std::vector<ResourceType>& requiredResources, const uint8_t maxRetries)
    : StorageResourceLocker(requiredResources), responsePromise(), maxRetries(maxRetries) {}

std::vector<AbstractRequestPtr> AbstractRequest::handleError(const std::exception_ptr& ex,
                                                             const StorageHandlerPtr& storageHandle) {
    std::vector<AbstractRequestPtr> followUpRequests;
    try {
        //error handling to be performed before rolling back
        preRollbackHandle(ex, storageHandle);

        //roll back the changes made by the failed request
        followUpRequests = rollBack(ex, storageHandle);

        //error handling to be performed after rolling back
        postRollbackHandle(ex, storageHandle);
    } catch (RequestExecutionException& exception) {
        if (retry()) {
            handleError(std::current_exception(), storageHandle);
        } else {
            NES_ERROR("Final failure to rollback. No retries left. Error: {}", exception.what());
        }
    }
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

std::future<AbstractRequestResponsePtr> AbstractRequest::getFuture() { return responsePromise.get_future(); }


void AbstractRequest::trySetExceptionInPromise(std::exception_ptr exception) {
    try {
        responsePromise.set_exception(std::move(exception));
    } catch (std::future_error& e) {
        if (e.code() != std::future_errc::promise_already_satisfied) {
            throw;
        }
    }
}

void AbstractRequest::setExceptionInPromiseOrRethrow(std::exception_ptr exception) {
    try {
        responsePromise.set_exception(exception);
    } catch (std::future_error& e) {
        std::rethrow_exception(exception);
    }
}

}// namespace NES::RequestProcessor::Experimental
