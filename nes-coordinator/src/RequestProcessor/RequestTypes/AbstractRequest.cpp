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
#include <Identifiers/Identifiers.hpp>
#include <RequestProcessor/RequestTypes/AbstractRequest.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::RequestProcessor {
AbstractRequest::AbstractRequest(const uint8_t maxRetries) : responsePromise(), maxRetries(maxRetries) {}

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

void AbstractRequest::setId(RequestId requestId) { this->requestId = requestId; }

RequestId AbstractRequest::getId() { return requestId; }
}// namespace NES::RequestProcessor
