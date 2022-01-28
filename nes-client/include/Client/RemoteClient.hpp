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

#include <Client/QueryConfig.hpp>
#include <chrono>
namespace NES{
class Query;
}
namespace NES::Client {

/**
 * @brief CPP client to deploy queries over the REST API
 */
class RemoteClient {
  public:
    /* @brief constructor of the client
     * @param coordinator host e.g. 127.0.0.1
     * @param coordinator REST port e.g. 8081
     */
    RemoteClient(const std::string& coordinatorHost = "127.0.0.1",
              uint16_t coordinatorPort = 8081,
              std::chrono::seconds requestTimeout = std::chrono::seconds(20));

    /*
     * @brief Deploy a query to the coordinator
     * @param query plan to deploy
     * @return query id of deployed query.
     */
    int64_t submitQuery(const Query& query, QueryConfig config = QueryConfig());

  private:
    const std::string coordinatorHost;
    const uint16_t coordinatorRESTPort;
    const std::chrono::seconds requestTimeout;

    std::string getHostName();
};
}// namespace NES

#endif//NES_INCLUDE_CLIENT_CPPCLIENT_HPP_