/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <WorkQueues/RequestTypes/RunQueryRequest.hpp>
#include <utility>

namespace NES {

RunQueryRequest::RunQueryRequest(const QueryPlanPtr& queryPlan, std::string queryPlacementStrategy)
    : NESRequest(queryPlan->getQueryId()), queryPlan(queryPlan), queryPlacementStrategy(std::move(queryPlacementStrategy)) {}

RunQueryRequestPtr RunQueryRequest::create(QueryPlanPtr queryPlan, std::string queryPlacementStrategy) {
    return std::make_shared<RunQueryRequest>(RunQueryRequest(std::move(queryPlan), std::move(queryPlacementStrategy)));
}

QueryPlanPtr RunQueryRequest::getQueryPlan() { return queryPlan; }

std::string RunQueryRequest::getQueryPlacementStrategy() { return queryPlacementStrategy; }

std::string RunQueryRequest::toString() {
    return "RunQueryRequest { QueryId: " + std::to_string(getQueryId()) + ", QueryPlan: " + queryPlan->toString()
        + ", QueryPlacementStrategy: " + queryPlacementStrategy + "}";
}
}// namespace NES
