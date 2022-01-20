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

#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <utility>

namespace NES {

QueryCatalogEntry::QueryCatalogEntry(QueryId queryId,
                                     std::string queryString,
                                     std::string queryPlacementStrategy,
                                     QueryPlanPtr inputQueryPlan,
                                     QueryStatus queryStatus)
    : queryId(queryId), queryString(std::move(queryString)), queryPlacementStrategy(std::move(queryPlacementStrategy)),
      inputQueryPlan(std::move(inputQueryPlan)), queryStatus(queryStatus) {}

QueryId QueryCatalogEntry::getQueryId() const noexcept { return queryId; }

std::string QueryCatalogEntry::getQueryString() const { return queryString; }

QueryPlanPtr QueryCatalogEntry::getInputQueryPlan() const { return inputQueryPlan; }

QueryPlanPtr QueryCatalogEntry::getExecutedQueryPlan() const { return executedQueryPlan; }

void QueryCatalogEntry::setExecutedQueryPlan(QueryPlanPtr executedQueryPlan) { this->executedQueryPlan = executedQueryPlan; }

QueryStatus QueryCatalogEntry::getQueryStatus() const { return queryStatus; }

std::string QueryCatalogEntry::getQueryStatusAsString() const { return queryStatusToStringMap[queryStatus]; }

void QueryCatalogEntry::setQueryStatus(QueryStatus queryStatus) { this->queryStatus = queryStatus; }

void QueryCatalogEntry::setFailureReason(std::string failureReason) { this->failureReason = std::move(failureReason); }

std::string QueryCatalogEntry::getFailureReason() { return failureReason; }

const std::string& QueryCatalogEntry::getQueryPlacementStrategy() const { return queryPlacementStrategy; }

QueryCatalogEntry QueryCatalogEntry::copy() {
    auto queryCatalogEntry = QueryCatalogEntry(queryId, queryString, queryPlacementStrategy, inputQueryPlan, queryStatus);
    queryCatalogEntry.setExecutedQueryPlan(executedQueryPlan);
    return queryCatalogEntry;
}

}// namespace NES
