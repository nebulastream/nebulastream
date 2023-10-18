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

#include <Optimizer/RequestTypes/QueryRequests/AddQueryRequest.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <utility>

namespace NES {

AddQueryRequest::AddQueryRequest(const QueryPlanPtr& queryPlan, Optimizer::PlacementStrategy queryPlacementStrategy)
    : queryPlan(queryPlan), queryPlacementStrategy(queryPlacementStrategy) {}

AddQueryRequestPtr AddQueryRequest::create(QueryPlanPtr queryPlan, Optimizer::PlacementStrategy queryPlacementStrategy) {
    return std::make_shared<AddQueryRequest>(AddQueryRequest(std::move(queryPlan), queryPlacementStrategy));
}

QueryPlanPtr AddQueryRequest::getQueryPlan() { return queryPlan; }

Optimizer::PlacementStrategy AddQueryRequest::getQueryPlacementStrategy() { return queryPlacementStrategy; }

std::string AddQueryRequest::toString() {
    return "RunQueryRequest { QueryId: " + std::to_string(queryPlan->getQueryId()) + ", QueryPlan: " + queryPlan->toString()
        + ", QueryPlacementStrategy: " + std::string(magic_enum::enum_name(queryPlacementStrategy)) + "}";
}

uint64_t AddQueryRequest::getQueryId() { return queryPlan->getQueryId(); }

RequestType AddQueryRequest::getRequestType() { return RequestType::AddQuery; }
}// namespace NES
