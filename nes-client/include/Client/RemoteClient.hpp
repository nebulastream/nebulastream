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

#ifndef NES_INCLUDE_CLIENT_CPPCLIENT_HPP_
#define NES_INCLUDE_CLIENT_CPPCLIENT_HPP_

#include <API/Query.hpp>
#include <Client/QueryConfig.hpp>
#include <chrono>
#include <cpprest/http_client.h>

namespace NES {
class Query;
}
namespace NES::Client {
class RemoteClient;
using RemoteClientPtr = std::shared_ptr<RemoteClient>;

/**
 * @brief Remote client to deploy queries over the REST API
 */
class RemoteClient {
  public:
    /* @brief constructor of the client
     * @param coordinator host e.g. 127.0.0.1
     * @param coordinator REST port e.g. 8081
     * @param request timeout
     */
    RemoteClient(const std::string& coordinatorHost = "127.0.0.1",
                 uint16_t coordinatorPort = 8081,
                 std::chrono::seconds requestTimeout = std::chrono::seconds(20));

    /// @brief test if a connection to the coordinator can be established
    bool testConnection();

    /*
     * @brief Deploy a query to the coordinator
     * @param query plan to deploy
     * @return query config
     */
    int64_t submitQuery(const Query& query, QueryConfig config = QueryConfig());

    /// @brief stop the query with the given queryId
    /// @return stop was successfully
    bool stopQuery(uint64_t queryId);

    /// @brief get a queries query plan
    std::string getQueryPlan(uint64_t queryId);

    /// @brief get a queries execution plan
    std::string getQueryExecutionPlan(uint64_t queryId);

    /// @brief get current topology
    std::string getTopology();

    /// @brief get all registered queries
    std::string getQueries();

    /// @brief get all registered queries in the given state
    std::string getQueries(const QueryStatus& status);

    /// @brief add a logical stream
    bool addLogicalStream(const SchemaPtr, const std::string& streamName);

    /// @brief get logical streams
    std::string getLogicalStreams();

    /// @brief get physical streams
    std::string getPhysicalStreams();

  private:
    const std::string coordinatorHost;
    const uint16_t coordinatorRESTPort;
    const std::chrono::seconds requestTimeout;

    std::string getHostName();
    web::json::value send(const web::http::method& method, const std::string& path, const std::string& message);
};
}// namespace NES

#endif//NES_INCLUDE_CLIENT_CPPCLIENT_HPP_