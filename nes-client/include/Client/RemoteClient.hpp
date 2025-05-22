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

#pragma once

#include <chrono>
#include <memory>
#include <API/Query.hpp>
#include <API/Schema.hpp>
#include <Client/QueryConfig.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Util/QueryState.hpp>

namespace NES::Client
{
/**
 * @brief Remote client to deploy queryIdAndCatalogEntryMapping over the REST API
 */
class RemoteClient
{
public:
    struct QueryStopResult
    {
        QueryId queryId = INVALID_QUERY_ID;
        bool withError;
        std::string errorMessage;

        bool operator!() const { return withError; }

        operator bool() const { return !withError; }
    };

public:
    /**
     * @brief constructor of the client
     * @param string coordinator host e.g. 127.0.0.1
     * @param uint16_t coordinator REST port e.g. 8081
     * @param seconds request timeout
     */
    explicit RemoteClient(
        const std::string& coordinatorHost = "127.0.0.1",
        uint16_t coordinatorPort = 8081,
        std::chrono::seconds requestTimeout = std::chrono::seconds(20),
        bool disableLogging = false);

    /**
     * @brief test if a connection to the coordinator can be established
     * @return connection test successful
     */
    bool testConnection();

    /**
     * @brief deploy a new query
     * @param Query query to deploy
     * @param QueryConfig query config
     * @return query id
     */
    QueryId submitQuery(const Query& query, QueryConfig config = QueryConfig());

    /**
     * @brief stop the query with the given query id
     * @param uint64_t query id
     * @return stop was successfully, if not why
     */
    [[nodiscard]] QueryStopResult stopQuery(QueryId queryId);

    /**
     * @brief get a queryIdAndCatalogEntryMapping query plan
     * @param uint64_t query id
     * @return query plan
     */
    std::string getQueryPlan(QueryId queryId);

    /**
     * @brief get a queryIdAndCatalogEntryMapping execution plan
     * @param uint64_t query id
     * @return execution plan
     */
    std::string getQueryExecutionPlan(QueryId queryId);

    /**
     * @brief get query status of a query
     * @param uint64_t query id
     * @return query status
     */
    std::string getQueryStatus(QueryId queryId);

    /**
     * @brief get current topology
     * @return topology
     */
    std::string getTopology();

    /**
     * @brief get all registered queryIdAndCatalogEntryMapping
     * @return query information
     */
    std::string getQueries();

    /**
     * @brief get all registered queryIdAndCatalogEntryMapping in the given status
     * @param QueryStatus query status, i.e. registered
     * @return query information
     */
    std::string getQueries(QueryState status);

    /**
     * @brief add a logical source
     * @param std::shared_ptr<Schema> schema of the new logical source
     * @param string name of the new logical source
     * @return successfully added
     */
    bool addLogicalSource(std::shared_ptr<Schema> schema, const std::string& sourceName);

    /**
     * @brief get logical sources
     * @return logical sources
     */
    std::string getLogicalSources();

    /**
     * @brief get physical sources
     * @param logicalSourceName: name of the logical source for which physical source is required
     * @return physical sources
     */
    std::string getPhysicalSources(std::string logicalSourceName);

private:
    const std::string coordinatorHost;
    const uint16_t coordinatorRESTPort;
    const std::chrono::seconds requestTimeout;

    std::string getHostName();
};
}
