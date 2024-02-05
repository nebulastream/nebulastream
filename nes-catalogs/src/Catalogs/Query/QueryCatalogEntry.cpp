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

#include <Catalogs/Exceptions/InvalidQueryException.hpp>
#include <Catalogs/Query/DecomposedQueryPlanMetaData.hpp>
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <utility>

namespace NES::Catalogs::Query {

QueryCatalogEntry::QueryCatalogEntry(QueryId queryId,
                                     std::string queryString,
                                     Optimizer::PlacementStrategy queryPlacementStrategy,
                                     QueryPlanPtr inputQueryPlan,
                                     QueryState queryStatus)
    : queryId(queryId), queryString(std::move(queryString)), queryPlacementStrategy(queryPlacementStrategy),
      inputQueryPlan(std::move(inputQueryPlan)) {
    // Make sure that initial status is timestamped.
    setQueryState(queryStatus);
}

QueryId QueryCatalogEntry::getQueryId() const noexcept { return queryId; }

SharedQueryId QueryCatalogEntry::getSharedQueryId() const noexcept { return sharedQueryId; }

std::string QueryCatalogEntry::getQueryString() const { return queryString; }

QueryPlanPtr QueryCatalogEntry::getInputQueryPlan() const { return inputQueryPlan; }

QueryPlanPtr QueryCatalogEntry::getExecutedQueryPlan() const { return executedQueryPlan; }

void QueryCatalogEntry::setExecutedQueryPlan(QueryPlanPtr executedQueryPlan) { this->executedQueryPlan = executedQueryPlan; }

QueryState QueryCatalogEntry::getQueryState() const { return queryState; }

void QueryCatalogEntry::setQueryState(QueryState queryStatus) {
    this->queryState = queryStatus;
    uint64_t usSinceEpoch =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    history.emplace_back(usSinceEpoch, queryStatus);
}

void QueryCatalogEntry::setTerminationReason(std::string metaInformation) { this->terminationReason = metaInformation; }

std::string QueryCatalogEntry::getMetaInformation() { return terminationReason; }

const std::string QueryCatalogEntry::getQueryPlacementStrategyAsString() const {
    return std::string(magic_enum::enum_name(queryPlacementStrategy));
}

Optimizer::PlacementStrategy QueryCatalogEntry::getQueryPlacementStrategy() { return queryPlacementStrategy; }

void QueryCatalogEntry::addOptimizationPhase(std::string phaseName, QueryPlanPtr queryPlan) {
    optimizationPhases.insert(std::pair<std::string, QueryPlanPtr>(phaseName, queryPlan));
}

std::map<std::string, QueryPlanPtr> QueryCatalogEntry::getOptimizationPhases() { return optimizationPhases; }

const QueryStateHistory& QueryCatalogEntry::getHistory() const { return history; }
}// namespace NES::Catalogs::Query
