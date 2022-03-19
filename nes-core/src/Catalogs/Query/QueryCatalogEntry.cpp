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

#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Catalogs/Query/QuerySubPlanMetaData.hpp>
#include <Exceptions/InvalidQueryException.hpp>
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <utility>

namespace NES {

QueryCatalogEntry::QueryCatalogEntry(QueryId queryId,
                                     std::string queryString,
                                     std::string queryPlacementStrategy,
                                     QueryPlanPtr inputQueryPlan,
                                     QueryStatus::Value queryStatus)
    : queryId(queryId), queryString(std::move(queryString)), queryPlacementStrategy(std::move(queryPlacementStrategy)),
      inputQueryPlan(std::move(inputQueryPlan)), queryStatus(queryStatus) {}

QueryId QueryCatalogEntry::getQueryId() const noexcept { return queryId; }

std::string QueryCatalogEntry::getQueryString() const { return queryString; }

QueryPlanPtr QueryCatalogEntry::getInputQueryPlan() const { return inputQueryPlan; }

QueryPlanPtr QueryCatalogEntry::getExecutedQueryPlan() const { return executedQueryPlan; }

void QueryCatalogEntry::setExecutedQueryPlan(QueryPlanPtr executedQueryPlan) { this->executedQueryPlan = executedQueryPlan; }

QueryStatus::Value QueryCatalogEntry::getQueryStatus() const { return queryStatus; }

std::string QueryCatalogEntry::getQueryStatusAsString() const { return QueryStatus::toString(queryStatus); }

void QueryCatalogEntry::setQueryStatus(QueryStatus::Value queryStatus) { this->queryStatus = queryStatus; }

void QueryCatalogEntry::setFailureReason(std::string failureReason) { this->failureReason = std::move(failureReason); }

std::string QueryCatalogEntry::getFailureReason() { return failureReason; }

const std::string& QueryCatalogEntry::getQueryPlacementStrategyAsString() const { return queryPlacementStrategy; }

QueryCatalogEntry QueryCatalogEntry::copy() {
    auto queryCatalogEntry = QueryCatalogEntry(queryId, queryString, queryPlacementStrategy, inputQueryPlan, queryStatus);
    queryCatalogEntry.setExecutedQueryPlan(executedQueryPlan);
    return queryCatalogEntry;
}

PlacementStrategy::Value QueryCatalogEntry::getQueryPlacementStrategy() {
    return PlacementStrategy::getFromString(queryPlacementStrategy);
}

void QueryCatalogEntry::addOptimizationPhase(std::string phaseName, QueryPlanPtr queryPlan) {
    optimizationPhases.insert(std::pair<std::string, QueryPlanPtr>(phaseName, queryPlan));
}

std::map<std::string, QueryPlanPtr> QueryCatalogEntry::getOptimizationPhases() { return optimizationPhases; }

void QueryCatalogEntry::addQuerySubPlanMetaData(QuerySubPlanId querySubPlanId, uint64_t workerId) {

    if (querySubPlanMetaDataMap.find(querySubPlanId) != querySubPlanMetaDataMap.end()) {
        throw InvalidQueryException("Query catalog entry already contain the query sub plan id "
                                    + std::to_string(querySubPlanId));
    }

    auto subQueryMetaData = QuerySubPlanMetaData::create(querySubPlanId, QueryStatus::Migrating, workerId);
    querySubPlanMetaDataMap[querySubPlanId] = subQueryMetaData;
}

QuerySubPlanMetaDataPtr QueryCatalogEntry::getQuerySubPlanMetaData(QuerySubPlanId querySubPlanId) {

    if (querySubPlanMetaDataMap.find(querySubPlanId) == querySubPlanMetaDataMap.end()) {
        throw InvalidQueryException("Query catalog entry does not contains the input query sub pln Id "
                                    + std::to_string(querySubPlanId));
    }
    return querySubPlanMetaDataMap[querySubPlanId];
}

std::vector<QuerySubPlanMetaDataPtr> QueryCatalogEntry::getAllSubQueryPlanMetaData() {

    std::vector<QuerySubPlanMetaDataPtr> allQuerySubPlanMetaData;
    for (const auto& pair : querySubPlanMetaDataMap) {
        allQuerySubPlanMetaData.emplace_back(pair.second);
    }
    return allQuerySubPlanMetaData;
}

}// namespace NES
