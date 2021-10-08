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

#ifndef NES_INCLUDE_CATALOGS_QUERYCATALOGENTRY_HPP_
#define NES_INCLUDE_CATALOGS_QUERYCATALOGENTRY_HPP_

#include <Plans/Query/QueryId.hpp>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class NESExecutionPlan;
using NESExecutionPlanPtr = std::shared_ptr<NESExecutionPlan>;

/**
 * @brief Represents various states the user query goes through.
 *
 * Registered: Query is registered to be scheduled to the worker nodes (added to the queue).
 * Scheduling: Coordinator node is processing the Query and will transmit the execution pipelines to worker nodes.
 * Running: Query is now running successfully.
 * MarkedForStop: A request arrived into the system for stopping a query and system marks the query for stopping (added to the queue).
 * Stopped: Query was explicitly stopped either by system or by user.
 * Failed: Query failed because of some reason.
 */
enum QueryStatus { Registered, Scheduling, Running, MarkedForStop, Stopped, Failed };

static std::map<std::string, QueryStatus> stringToQueryStatusMap{
    {"REGISTERED", Registered},
    {"SCHEDULING", Scheduling},
    {"RUNNING", Running},
    {"MARKED_FOR_STOP", MarkedForStop},
    {"STOPPED", Stopped},
    {"FAILED", Failed},
};

static std::map<QueryStatus, std::string> queryStatusToStringMap{
    {Registered, "REGISTERED"},
    {Scheduling, "SCHEDULING"},
    {Running, "RUNNING"},
    {MarkedForStop, "MARKED_FOR_STOP"},
    {Stopped, "STOPPED"},
    {Failed, "FAILED"},
};

/**
 * @brief class to handle the entry in the query catalog
 * @param queryId: id of the query (is also the key in the queries map)
 * @param queryString: string representation of the query
 * @param QueryPtr: a pointer to the query
 * @param schema: the schema of this query
 * @param running: bool indicating if the query is running (has been deployed)
 */
class QueryCatalogEntry {
  public:
    QueryCatalogEntry(QueryId queryId,
                      std::string queryString,
                      std::string queryPlacementStrategy,
                      QueryPlanPtr inputQueryPlan,
                      QueryStatus queryStatus);

    /**
     * @brief method to get the id of the query
     * @return query id
     */
    [[nodiscard]] QueryId getQueryId() const noexcept;

    /**
     * @brief method to get the string of the query
     * @return query string
     */
    [[nodiscard]] std::string getQueryString() const;

    /**
     * @brief method to get the input query plan
     * @return pointer to the query plan
     */
    [[nodiscard]] QueryPlanPtr getInputQueryPlan() const;

    /**
     * @brief method to get the executed query plan
     * @return pointer to the query plan
     */
    [[nodiscard]] QueryPlanPtr getExecutedQueryPlan() const;

    /**
     * @brief method to set the executed query plan
     * @param executedQueryPlan: the executed query plan for the query
     */
    void setExecutedQueryPlan(QueryPlanPtr executedQueryPlan);

    /**
     * @brief method to get the status of the query
     * @return query status
     */
    [[nodiscard]] QueryStatus getQueryStatus() const;

    /**
     * @brief method to get the status of the query as string
     * @return query status: as string
     */
    [[nodiscard]] std::string getQueryStatusAsString() const;

    /**
     * @brief method to set the status of the query
     * @param query status
     */
    void setQueryStatus(QueryStatus queryStatus);

    /**
      * @brief Get name of the query placement strategy
      * @return query placement strategy
      */
    [[nodiscard]] std::string const& getQueryPlacementStrategy() const;

    /**
     * @brief create a copy of query catalog entry.
     * @return copy of this query catalog entry
     */
    QueryCatalogEntry copy();

    void setFailureReason(std::string failureReason);

    std::string getFailureReason();

  private:
    QueryId queryId;
    std::string queryString;
    std::string queryPlacementStrategy;
    QueryPlanPtr inputQueryPlan;
    QueryPlanPtr executedQueryPlan;
    QueryStatus queryStatus;
    std::string failureReason;
};
using QueryCatalogEntryPtr = std::shared_ptr<QueryCatalogEntry>;
}// namespace NES

#endif//NES_INCLUDE_CATALOGS_QUERYCATALOGENTRY_HPP_
