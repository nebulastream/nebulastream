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

#include <Plans/Query/QueryPlan.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <WorkQueues/RequestTypes/RunQueryRequest.hpp>
#include <utility>

namespace NES {

RunQueryRequest::RunQueryRequest(const QueryPlanPtr& queryPlan, Optimizer::PlacementStrategy queryPlacementStrategy)
    : queryPlan(queryPlan), queryPlacementStrategy(queryPlacementStrategy) {
    queryPlan->setPlacementStrategy(queryPlacementStrategy);
}

RunQueryRequestPtr RunQueryRequest::create(QueryPlanPtr queryPlan, Optimizer::PlacementStrategy queryPlacementStrategy) {
    return std::make_shared<RunQueryRequest>(RunQueryRequest(std::move(queryPlan), queryPlacementStrategy));
}

QueryPlanPtr RunQueryRequest::getQueryPlan() { return queryPlan; }

Optimizer::PlacementStrategy RunQueryRequest::getQueryPlacementStrategy() { return queryPlacementStrategy; }

std::string RunQueryRequest::toString() {
    return "RunQueryRequest { QueryId: " + std::to_string(queryPlan->getQueryId()) + ", QueryPlan: " + queryPlan->toString()
        + ", QueryPlacementStrategy: " + std::string(magic_enum::enum_name(queryPlacementStrategy)) + "}";
}
uint64_t RunQueryRequest::getQueryId() { return queryPlan->getQueryId(); }
}// namespace NES
