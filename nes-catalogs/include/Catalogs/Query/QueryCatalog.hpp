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

#ifndef NES_CATALOGS_INCLUDE_CATALOGS_QUERY_QUERYCATALOG_HPP_
#define NES_CATALOGS_INCLUDE_CATALOGS_QUERY_QUERYCATALOG_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
#include <Util/QueryState.hpp>
#include <condition_variable>
#include <folly/Synchronized.h>
#include <map>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <queue>
#include <set>
#include <string>
#include <vector>

namespace NES {
class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

namespace Catalogs::Query {
class QueryCatalogEntry;
using QueryCatalogEntryPtr = std::shared_ptr<QueryCatalogEntry>;

class SharedQueryCatalogEntry;
using SharedQueryCatalogEntryPtr = std::shared_ptr<SharedQueryCatalogEntry>;

/**
         * @brief catalog class to handle the queryIdAndCatalogEntryMapping in the system
         * @note: This class is not thread safe. Please use QueryCatalogService to access this object.
         */
class QueryCatalog {
  public:
    QueryCatalog() = default;

    /**
             * @brief registers a new query into the NES Query catalog.
             * @param queryString: a user query in string form
             * @param queryPlan: a user query plan to be executed
             * @param placementStrategyName: the placement strategy (e.g. bottomUp, topDown, etc)
             * @param queryState: the state of the query
             */
    void createQueryCatalogEntry(const std::string& queryString,
                                 const QueryPlanPtr& queryPlan,
                                 const Optimizer::PlacementStrategy placementStrategyName,
                                 QueryState queryState);

    /**
             * @brief Get state of the input query id
             * @param queryId : the query id
             * @return state of the query
             */
    QueryState getQueryState(QueryId queryId);

    /**
             * @brief Update query entry with new status
             * @param queryId : query id
             * @param queryStatus : new status
             * @param terminationReason : additional meta information
             * @return true if updated successfully
             */
    void updateQueryStatus(QueryId queryId, QueryState queryStatus, const std::string& terminationReason);

    /**
             * @brief link query catalog entry to the shared query catalog entry
             * @param queryId the query id
             * @param sharedQueryId the shared query id
             */
    void linkSharedQuery(QueryId queryId, SharedQueryId sharedQueryId);

    /**
             * @brief Get linked shared query id
             * @param queryId the query id
             */
    SharedQueryId getLinkedSharedQueryId(QueryId queryId);

    /**
             * @brief registers a new shared query into the Query catalog
             * @param sharedQueryId: a user query in string form
             * @param queryIds: query ids contained in the shared query plan
             * @param queryStatus: the status of the shared query
             */
    void createSharedQueryCatalogEntry(SharedQueryId sharedQueryId, std::set<QueryId> queryIds, QueryState queryStatus);

    /**
             * @brief Update query entry with new status
             * @param sharedQueryId : shared query id
             * @param queryState : new status
             * @param terminationReason : additional meta information
             * @return true if updated successfully
             */
    void updateSharedQueryStatus(SharedQueryId sharedQueryId, QueryState queryState, const std::string& terminationReason);

    /**
             * @brief check and mark the shared query for soft stop
             * @param sharedQueryId: the query which is being soft stopped
             * @param decomposedQueryId: the decomposed query plan id
             * @return true if successful else false
             */
    bool handleDecomposedQueryPlanMarkedForSoftStop(SharedQueryId sharedQueryId,
                                                    DecomposedQueryId decomposedQueryId,
                                                    DecomposedQueryPlanVersion decomposedQueryVersion);

    /**
             * @brief Handle decomposed query plan soft stop completed
             * @param sharedQueryId : the shared query id
             * @param decomposedQueryId : the decomposed query plan id
             * @return true if handled or false
             */
    bool handleDecomposedQueryPlanSoftStopCompleted(SharedQueryId sharedQueryId,
                                                    DecomposedQueryId decomposedQueryId,
                                                    DecomposedQueryPlanVersion decomposedQueryVersion);

    /**
             * @brief Handle decomposed query plan soft stop triggered
             * @param sharedQueryId : the shared query id
             * @param decomposedQueryId : the decomposed query plan id
             * @return true if handled or false
             */
    bool handleDecomposedQueryPlanSoftStopTriggered(SharedQueryId sharedQueryId,
                                                    DecomposedQueryId decomposedQueryId,
                                                    DecomposedQueryPlanVersion decomposedQueryVersion);

    /**
             * @brief Check and mark shared query for failure
             * @param sharedQueryId
             * @param decomposedQueryId
             */
    void checkAndMarkSharedQueryForFailure(SharedQueryId sharedQueryId,
                                           DecomposedQueryId decomposedQueryId,
                                           DecomposedQueryPlanVersion decomposedQueryVersion);

    /**
             * Add update query plans to the query catalog
             * @param queryId : the query id
             * @param step : step that produced the updated plan
             * @param updatedQueryPlan : the updated query plan
             */
    void addUpdatedQueryPlan(QueryId queryId, std::string step, QueryPlanPtr updatedQueryPlan);

    /**
             * Update decomposed query plan status
             * @param sharedQueryId: the query id to which sub plan is added
             * @param decomposedQueryId: the query sub plan id
             * @param decomposedQueryPlanVersion: the decomposed query plan
             * @param decomposedQueryState : the new decomposed query plan status
             * @param workerId: the worker id
             */
    bool updateDecomposedQueryPlanStatus(SharedQueryId sharedQueryId,
                                         DecomposedQueryId decomposedQueryId,
                                         DecomposedQueryPlanVersion decomposedQueryPlanVersion,
                                         QueryState decomposedQueryState,
                                         WorkerId workerId);

    /**
             * Fetch decomposed query plan status
             * @param sharedQueryId : the shared query id
             * @param decomposedQueryId : the decomposed query plan id
             * @return decomposed query plan status
             */
    QueryState getDecomposedQueryPlanSatus(SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId);

    /**
             * @brief Get a copy of executed query plan
             * @param queryId: the query id
             * @return the copy of query plan ptr
             */
    QueryPlanPtr getCopyOfExecutedQueryPlan(QueryId queryId) const;

    /**
             * @brief Gets a copy of the logical input query plan without any rules applied
             * @param queryId
             * @return the copy of the query plan ptr
             */
    QueryPlanPtr getCopyOfLogicalInputQueryPlan(const QueryId& queryId) const;

    /**
             * @brief Get query with given id
             * @param queryId: the input query id
             * @return json representing query with given id
             */
    nlohmann::json getQueryEntry(QueryId queryId);

    /**
             * @brief Get all queries in the input state
             * @param queryState : the input query state
             * @return json representing queries in the given state
             */
    nlohmann::json getQueryEntriesWithStatus(const std::string& queryState);

    /**
             * @brief Get all queryIdAndCatalogEntryMapping registered in the system
             * @return map of query ids and query string with query status
             */
    nlohmann::json getAllQueryEntries();

    /**
             * @brief method to reset the catalog
             */
    void reset();

  private:
    /**
             * @brief check and mark the query for hard stop
             * @param queryId: the query which need to be stopped
             * @return true if successful else false
             */
    bool checkAndMarkQueryForHardStop(QueryId queryId);

  private:
    folly::Synchronized<std::map<QueryId, QueryCatalogEntryPtr>> queryCatalogEntryMapping;
    folly::Synchronized<std::map<SharedQueryId, SharedQueryCatalogEntryPtr>> sharedQueryCatalogEntryMapping;
};

using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;
}// namespace Catalogs::Query
}// namespace NES
#endif// NES_CATALOGS_INCLUDE_CATALOGS_QUERY_QUERYCATALOG_HPP_
