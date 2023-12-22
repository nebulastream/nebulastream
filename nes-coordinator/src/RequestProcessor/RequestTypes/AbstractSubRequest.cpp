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
#include <RequestProcessor/RequestTypes/AbstractSubRequest.hpp>
#include <RequestProcessor/RequestTypes/AbstractMultiRequest.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>
namespace NES::RequestProcessor {

AbstractSubRequest::AbstractSubRequest(std::vector<ResourceType> requiredResources)
    : StorageResourceLocker(std::move(requiredResources)) {}

void AbstractSubRequest::setPromise(std::promise<std::any> promise) { responsePromise = std::move(promise); }
void AbstractSubRequest::setStorageHandler(StorageHandlerPtr storageHandler) { this->storageHandler = std::move(storageHandler); }

bool AbstractSubRequest::execute() {
    auto expected = false;
    if (!executionStarted.compare_exchange_strong(expected, true)) {
        return false;
    }
    requestId = storageHandler->generateRequestId();
    if (requestId == INVALID_REQUEST_ID) {
        NES_THROW_RUNTIME_ERROR("Trying to execute a subrequest before its id has been set");
    }
    //acquire locks and perform other tasks to prepare for execution
    preExecution(storageHandler);

    //execute the request logic
    responsePromise.set_value(executeSubRequestLogic(storageHandler));

    //release locks
    postExecution(storageHandler);
    return true;
}

bool AbstractSubRequest::executionHasStarted() {
    return executionStarted.load();
}
SubRequestFuture::SubRequestFuture(AbstractSubRequestPtr parentRequest, std::future<std::any> future) : request(std::move(parentRequest)), future(std::move(future)) {}

std::any SubRequestFuture::get() {
    request->execute();
    return future.get();
}

}// namespace NES::RequestProcessor
