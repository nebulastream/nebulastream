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
#include <Catalogs/Exceptions/InvalidQueryException.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <utility>

namespace NES::Catalogs::Query {

QueryCatalogEntry::QueryCatalogEntry(QueryId queryId,
                                     std::string queryString,
                                     Optimizer::PlacementStrategy queryPlacementStrategy,
                                     QueryPlanPtr inputQueryPlan,
                                     QueryState queryStatus)
    : queryId(queryId), queryString(std::move(queryString)), queryPlacementStrategy(queryPlacementStrategy),
      inputQueryPlan(std::move(inputQueryPlan)), queryState(queryStatus) {}

QueryId QueryCatalogEntry::getQueryId() const noexcept { return queryId; }

std::string QueryCatalogEntry::getQueryString() const { return queryString; }

QueryPlanPtr QueryCatalogEntry::getInputQueryPlan() const { return inputQueryPlan; }

QueryPlanPtr QueryCatalogEntry::getExecutedQueryPlan() const { return executedQueryPlan; }

void QueryCatalogEntry::setExecutedQueryPlan(QueryPlanPtr executedQueryPlan) {
    std::unique_lock lock(mutex);
    this->executedQueryPlan = executedQueryPlan;
}

QueryState QueryCatalogEntry::getQueryState() const {
    std::unique_lock lock(mutex);
    return queryState;
}

std::string QueryCatalogEntry::getQueryStatusAsString() const {
    std::unique_lock lock(mutex);
    return std::string(magic_enum::enum_name(queryState));
}

void QueryCatalogEntry::setQueryStatus(QueryState queryStatus) {
    std::unique_lock lock(mutex);
    this->queryState = queryStatus;
}

void QueryCatalogEntry::setMetaInformation(std::string metaInformation) {
    std::unique_lock lock(mutex);
    this->metaInformation = std::move(metaInformation);
}

std::string QueryCatalogEntry::getMetaInformation() { return metaInformation; }

const std::string QueryCatalogEntry::getQueryPlacementStrategyAsString() const {
    return std::string(magic_enum::enum_name(queryPlacementStrategy));
}

Optimizer::PlacementStrategy QueryCatalogEntry::getQueryPlacementStrategy() { return queryPlacementStrategy; }

void QueryCatalogEntry::addOptimizationPhase(std::string phaseName, QueryPlanPtr queryPlan) {
    std::unique_lock lock(mutex);
    optimizationPhases.insert(std::pair<std::string, QueryPlanPtr>(phaseName, queryPlan));
}

std::map<std::string, QueryPlanPtr> QueryCatalogEntry::getOptimizationPhases() {
    std::unique_lock lock(mutex);
    return optimizationPhases;
}

void QueryCatalogEntry::addQuerySubPlanMetaData(QuerySubPlanId querySubPlanId, uint64_t workerId) {
    std::unique_lock lock(mutex);
    if (querySubPlanMetaDataMap.find(querySubPlanId) != querySubPlanMetaDataMap.end()) {
        throw InvalidQueryException("Query catalog entry already contains the query sub plan id "
                                    + std::to_string(querySubPlanId));
    }

    auto subQueryMetaData = QuerySubPlanMetaData::create(querySubPlanId, QueryState::RUNNING, workerId);
    querySubPlanMetaDataMap[querySubPlanId] = subQueryMetaData;
}

QuerySubPlanMetaDataPtr QueryCatalogEntry::getQuerySubPlanMetaData(QuerySubPlanId querySubPlanId) {
    std::unique_lock lock(mutex);
    if (querySubPlanMetaDataMap.find(querySubPlanId) == querySubPlanMetaDataMap.end()) {
        throw InvalidQueryException("Query catalog entry does not contains the input query sub pln Id "
                                    + std::to_string(querySubPlanId));
    }
    return querySubPlanMetaDataMap[querySubPlanId];
}

std::vector<QuerySubPlanMetaDataPtr> QueryCatalogEntry::getAllSubQueryPlanMetaData() {
    std::unique_lock lock(mutex);
    //Fetch all query sub plan metadata information
    std::vector<QuerySubPlanMetaDataPtr> allQuerySubPlanMetaData;
    for (const auto& pair : querySubPlanMetaDataMap) {
        allQuerySubPlanMetaData.emplace_back(pair.second);
    }
    return allQuerySubPlanMetaData;
}

void QueryCatalogEntry::removeAllQuerySubPlanMetaData() {
    std::unique_lock lock(mutex);
    querySubPlanMetaDataMap.clear();
}

}// namespace NES::Catalogs::Query
