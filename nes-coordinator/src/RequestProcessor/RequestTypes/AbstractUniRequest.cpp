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
#include <Util/Logger/Logger.hpp>
#include <RequestProcessor/RequestTypes/AbstractUniRequest.hpp>
namespace NES::RequestProcessor {
AbstractUniRequest::AbstractUniRequest(const std::vector<ResourceType>& requiredResources,
                                       uint8_t maxRetries)
    : AbstractRequest(requiredResources, maxRetries) {}


std::vector<AbstractRequestPtr> AbstractUniRequest::execute(const StorageHandlerPtr& storageHandle) {
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
}
