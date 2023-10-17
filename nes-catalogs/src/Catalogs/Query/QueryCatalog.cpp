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

#include <Catalogs/Exceptions/InvalidQueryStateException.hpp>
#include <Catalogs/Exceptions/QueryNotFoundException.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <string>

namespace NES::Catalogs::Query {

std::map<uint64_t, std::string> QueryCatalog::getQueriesWithStatus(QueryState status) {
    NES_INFO("QueryCatalog : fetching all queryIdAndCatalogEntryMapping with status {}", magic_enum::enum_name(status));
    std::map<uint64_t, QueryCatalogEntryPtr> queries = getQueryCatalogEntries(status);
    std::map<uint64_t, std::string> result;
    for (auto const& [key, value] : queries) {
        result[key] = value->getQueryString();
    }
    NES_INFO("QueryCatalog : found {} all queryIdAndCatalogEntryMapping with status {}",
             result.size(),
             magic_enum::enum_name(status));
    return result;
}

std::map<uint64_t, std::string> QueryCatalog::getAllQueries() {
    NES_INFO("QueryCatalog : get all queryIdAndCatalogEntryMapping");
    std::map<uint64_t, QueryCatalogEntryPtr> registeredQueries = getAllQueryCatalogEntries();
    std::map<uint64_t, std::string> result;
    for (auto [key, value] : registeredQueries) {
        result[key] = value->getQueryString();
    }
    NES_INFO("QueryCatalog : found {} queryIdAndCatalogEntryMapping in catalog.", result.size());
    return result;
}

QueryCatalogEntryPtr QueryCatalog::createNewEntry(const std::string& queryString,
                                                  const QueryPlanPtr& queryPlan,
                                                  const Optimizer::PlacementStrategy placementStrategyName) {
    QueryId queryId = queryPlan->getQueryId();
    NES_INFO("QueryCatalog: Creating query catalog entry for query with id {}", queryId);
    QueryCatalogEntryPtr queryCatalogEntry =
        std::make_shared<QueryCatalogEntry>(queryId, queryString, placementStrategyName, queryPlan, QueryState::REGISTERED);
    queryIdAndCatalogEntryMapping[queryId] = queryCatalogEntry;
    return queryCatalogEntry;
}

std::map<uint64_t, QueryCatalogEntryPtr> QueryCatalog::getAllQueryCatalogEntries() {
    NES_TRACE("QueryCatalog: return registered queryIdAndCatalogEntryMapping={}", printQueries());
    return queryIdAndCatalogEntryMapping;
}

QueryCatalogEntryPtr QueryCatalog::getQueryCatalogEntry(QueryId queryId) {
    NES_TRACE("QueryCatalog: getQueryCatalogEntry with id {}", queryId);
    return queryIdAndCatalogEntryMapping[queryId];
}

bool QueryCatalog::queryExists(QueryId queryId) {
    NES_TRACE("QueryCatalog: Check if query with id {} exists.", queryId);
    if (queryIdAndCatalogEntryMapping.count(queryId) > 0) {
        NES_TRACE("QueryCatalog: query with id {} exists.", queryId);
        return true;
    }
    NES_WARNING("QueryCatalog: query with id {} does not exist", queryId);
    return false;
}

std::map<uint64_t, QueryCatalogEntryPtr> QueryCatalog::getQueryCatalogEntries(QueryState requestedStatus) {
    NES_TRACE("QueryCatalog: getQueriesWithStatus() registered queryIdAndCatalogEntryMapping={}", printQueries());
    std::map<uint64_t, QueryCatalogEntryPtr> matchingQueries;
    for (auto const& q : queryIdAndCatalogEntryMapping) {
        if (q.second->getQueryState() == requestedStatus) {
            matchingQueries.insert(q);
        }
    }
    return matchingQueries;
}

std::string QueryCatalog::printQueries() {
    std::stringstream ss;
    for (const auto& q : queryIdAndCatalogEntryMapping) {
        ss << "queryID=" << q.first << " running=" << magic_enum::enum_name(q.second->getQueryState()) << std::endl;
    }
    return ss.str();
}

void QueryCatalog::mapSharedQueryPlanId(SharedQueryId sharedQueryId, QueryCatalogEntryPtr queryCatalogEntry) {
    // Find the shared query plan for mapping
    if (sharedQueryIdAndCatalogEntryMapping.find(sharedQueryId) == sharedQueryIdAndCatalogEntryMapping.end()) {
        sharedQueryIdAndCatalogEntryMapping[sharedQueryId] = {queryCatalogEntry};
        return;
    }

    // Add the shared query id
    auto queryCatalogEntries = sharedQueryIdAndCatalogEntryMapping[sharedQueryId];
    queryCatalogEntries.emplace_back(queryCatalogEntry);
    sharedQueryIdAndCatalogEntryMapping[sharedQueryId] = queryCatalogEntries;
}

std::vector<QueryCatalogEntryPtr> QueryCatalog::getQueryCatalogEntriesForSharedQueryId(SharedQueryId sharedQueryId) {
    if (sharedQueryIdAndCatalogEntryMapping.find(sharedQueryId) == sharedQueryIdAndCatalogEntryMapping.end()) {
        NES_ERROR("QueryCatalog: Unable to find shared query plan with id {}", std::to_string(sharedQueryId));
        throw Exceptions::QueryNotFoundException("QueryCatalog: Unable to find shared query plan with id "
                                                 + std::to_string(sharedQueryId));
    }
    return sharedQueryIdAndCatalogEntryMapping[sharedQueryId];
}

void QueryCatalog::removeSharedQueryPlanIdMappings(SharedQueryId sharedQueryId) {
    if (sharedQueryIdAndCatalogEntryMapping.find(sharedQueryId) == sharedQueryIdAndCatalogEntryMapping.end()) {
        NES_ERROR("QueryCatalog: Unable to find shared query plan with id {}", std::to_string(sharedQueryId));
        throw Exceptions::QueryNotFoundException("QueryCatalog: Unable to find shared query plan with id "
                                                 + std::to_string(sharedQueryId));
    }
    sharedQueryIdAndCatalogEntryMapping.erase(sharedQueryId);
}

void QueryCatalog::clearQueries() {
    NES_TRACE("QueryCatalog: clear query catalog");
    queryIdAndCatalogEntryMapping.clear();
}

}// namespace NES::Catalogs::Query
