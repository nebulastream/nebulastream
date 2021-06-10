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

#include <Catalogs/QueryCatalogEntry.hpp>
#include <utility>

namespace NES {

QueryCatalogEntry::QueryCatalogEntry(QueryId queryId,
                                     std::string queryString,
                                     std::string queryPlacementStrategy,
                                     QueryPlanPtr queryPlanPtr,
                                     QueryStatus queryStatus)
    : queryId(queryId), queryString(std::move(queryString)), queryPlacementStrategy(std::move(queryPlacementStrategy)), queryPlanPtr(std::move(queryPlanPtr)),
      queryStatus(queryStatus) {}

QueryId QueryCatalogEntry::getQueryId() const noexcept { return queryId; }

std::string QueryCatalogEntry::getQueryString() const { return queryString; }

const QueryPlanPtr QueryCatalogEntry::getQueryPlan() const { return queryPlanPtr; }

QueryStatus QueryCatalogEntry::getQueryStatus() const { return queryStatus; }

std::string QueryCatalogEntry::getQueryStatusAsString() const { return queryStatusToStringMap[queryStatus]; }

void QueryCatalogEntry::setQueryStatus(QueryStatus queryStatus) { this->queryStatus = queryStatus; }

void QueryCatalogEntry::setFailureReason(std::string failureReason) { this->failureReason = failureReason; }

std::string QueryCatalogEntry::getFailureReason() { return failureReason; }

const std::string& QueryCatalogEntry::getQueryPlacementStrategy() const { return queryPlacementStrategy; }

QueryCatalogEntry QueryCatalogEntry::copy() {
    return QueryCatalogEntry(queryId, queryString, queryPlacementStrategy, queryPlanPtr, queryStatus);
}

}// namespace NES
