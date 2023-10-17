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

#include <Optimizer/RequestTypes/QueryRequests/FailQueryRequest.hpp>
#include <utility>

namespace NES {

FailQueryRequest::FailQueryRequest(const SharedQueryId sharedQueryId, std::string failureReason)
    : sharedQueryId(sharedQueryId), failureReason(std::move(failureReason)) {}

FailQueryRequestPtr FailQueryRequest::create(const SharedQueryId sharedQueryId, std::string failureReason) {
    return std::make_shared<FailQueryRequest>(FailQueryRequest(sharedQueryId, std::move(failureReason)));
}

std::string FailQueryRequest::getFailureReason() { return failureReason; }

std::string FailQueryRequest::toString() {
    return "FailQueryRequest { Shared Query Plan Id: " + std::to_string(sharedQueryId) + ", Failure Reason: " + failureReason
        + "}";
}

uint64_t FailQueryRequest::getSharedQueryId() const { return sharedQueryId; }

RequestType FailQueryRequest::getRequestType() { return RequestType::FailQuery; }

}// namespace NES