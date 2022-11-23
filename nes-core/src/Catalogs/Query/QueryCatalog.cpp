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

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Common/Identifiers.hpp>
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <string>

namespace NES::Catalogs::Query {

std::map<uint64_t, std::string> QueryCatalog::getQueriesWithStatus(QueryStatus::Value status) {
    NES_INFO("QueryCatalog : fetching all queryIdAndCatalogEntryMapping with status " << status);
    std::map<uint64_t, QueryCatalogEntryPtr> queries = getQueryCatalogEntries(status);
    std::map<uint64_t, std::string> result;
    for (auto const& [key, value] : queries) {
        result[key] = value->getQueryString();
    }
    NES_INFO("QueryCatalog : found " << result.size() << " all queryIdAndCatalogEntryMapping with status " << status);
    return result;
}

std::map<uint64_t, std::string> QueryCatalog::getAllQueries() {
    NES_INFO("QueryCatalog : get all queryIdAndCatalogEntryMapping");
    std::map<uint64_t, QueryCatalogEntryPtr> registeredQueries = getAllQueryCatalogEntries();
    std::map<uint64_t, std::string> result;
    for (auto [key, value] : registeredQueries) {
        result[key] = value->getQueryString();
    }
    NES_INFO("QueryCatalog : found " << result.size() << " queryIdAndCatalogEntryMapping in catalog.");
    return result;
}

QueryCatalogEntryPtr QueryCatalog::createNewEntry(const std::string& queryString,
                                                  const QueryPlanPtr& queryPlan,
                                                  const std::string& placementStrategyName) {
    QueryId queryId = queryPlan->getQueryId();
    NES_INFO("QueryCatalog: Creating query catalog entry for query with id " << queryId);
    QueryCatalogEntryPtr queryCatalogEntry =
        std::make_shared<QueryCatalogEntry>(queryId, queryString, placementStrategyName, queryPlan, QueryStatus::Registered);
    queryIdAndCatalogEntryMapping[queryId] = queryCatalogEntry;
    return queryCatalogEntry;
}

std::map<uint64_t, QueryCatalogEntryPtr> QueryCatalog::getAllQueryCatalogEntries() {
    NES_TRACE("QueryCatalog: return registered queryIdAndCatalogEntryMapping=" << printQueries());
    return queryIdAndCatalogEntryMapping;
}

QueryCatalogEntryPtr QueryCatalog::getQueryCatalogEntry(QueryId queryId) {
    NES_TRACE("QueryCatalog: getQueryCatalogEntry with id " << queryId);
    return queryIdAndCatalogEntryMapping[queryId];
}

bool QueryCatalog::queryExists(QueryId queryId) {
    NES_TRACE("QueryCatalog: Check if query with id " << queryId << " exists.");
    if (queryIdAndCatalogEntryMapping.count(queryId) > 0) {
        NES_TRACE("QueryCatalog: query with id " << queryId << " exists");
        return true;
    }
    NES_WARNING("QueryCatalog: query with id " << queryId << " does not exist");
    return false;
}

std::map<uint64_t, QueryCatalogEntryPtr> QueryCatalog::getQueryCatalogEntries(QueryStatus::Value requestedStatus) {
    NES_TRACE("QueryCatalog: getQueriesWithStatus() registered queryIdAndCatalogEntryMapping=" << printQueries());
    std::map<uint64_t, QueryCatalogEntryPtr> matchingQueries;
    for (auto const& q : queryIdAndCatalogEntryMapping) {
        if (q.second->getQueryStatus() == requestedStatus) {
            matchingQueries.insert(q);
        }
    }
    return matchingQueries;
}

std::string QueryCatalog::printQueries() {
    std::stringstream ss;
    for (const auto& q : queryIdAndCatalogEntryMapping) {
        ss << "queryID=" << q.first << " running=" << q.second->getQueryStatus() << std::endl;
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
    return;
}

std::vector<QueryCatalogEntryPtr> QueryCatalog::getQueryCatalogEntriesForSharedQueryId(SharedQueryId sharedQueryId) {
    if (sharedQueryIdAndCatalogEntryMapping.find(sharedQueryId) == sharedQueryIdAndCatalogEntryMapping.end()) {
        NES_ERROR("QueryCatalog: Unable to find shared query plan with id " + std::to_string(sharedQueryId));
        throw QueryNotFoundException("QueryCatalog: Unable to find shared query plan with id " + std::to_string(sharedQueryId));
    }
    return sharedQueryIdAndCatalogEntryMapping[sharedQueryId];
}

void QueryCatalog::removeSharedQueryPlanIdMappings(SharedQueryId sharedQueryId) {
    if (sharedQueryIdAndCatalogEntryMapping.find(sharedQueryId) == sharedQueryIdAndCatalogEntryMapping.end()) {
        NES_ERROR("QueryCatalog: Unable to find shared query plan with id " + std::to_string(sharedQueryId));
        throw QueryNotFoundException("QueryCatalog: Unable to find shared query plan with id " + std::to_string(sharedQueryId));
    }
    sharedQueryIdAndCatalogEntryMapping.erase(sharedQueryId);
}

void QueryCatalog::clearQueries() {
    NES_TRACE("QueryCatalog: clear query catalog");
    queryIdAndCatalogEntryMapping.clear();
}

}// namespace NES::Catalogs::Query
